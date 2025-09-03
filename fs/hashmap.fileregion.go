package fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	log "log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/encoding"
)

var zeroSector = bytes.Repeat([]byte{0}, sop.HandleSizeInBytes)

// deriveLockAlignedContext returns a child context with a deadline that does not exceed
// the lockFileRegionDuration minus a slack percentage (with a small minimum). If the
// parent already has a sooner deadline, the sooner deadline is kept. This provides a soft
// timeout for I/O so calls return before the lock expires, without attempting to cancel
// the underlying kernel I/O.
func deriveLockAlignedContext(parent context.Context) (context.Context, context.CancelFunc) {
	// Compute desired relative timeout using a percentage slack with a small minimum.
	slack := time.Duration(float64(LockFileRegionDuration) * LockDeadlineSlackPercent)
	if slack < 2*time.Second {
		slack = 2 * time.Second
	}
	lockBudget := LockFileRegionDuration - slack
	if lockBudget <= 0 {
		lockBudget = LockFileRegionDuration / 2
		if lockBudget <= 0 {
			lockBudget = 5 * time.Second
		}
	}
	// If parent has a deadline earlier than now+lockBudget, honor that.
	if dl, ok := parent.Deadline(); ok {
		parentBudget := time.Until(dl)
		// Pick the smaller of parentBudget and lockBudget.
		if parentBudget < lockBudget {
			lockBudget = parentBudget
		}
		if lockBudget <= 0 {
			// Parent already expired or insufficient budget; return an already-cancelled context
			// to ensure callers notice immediately.
			cctx, cancel := context.WithCancel(parent)
			cancel()
			return cctx, func() {}
		}
		return context.WithTimeout(parent, lockBudget)
	}
	return context.WithTimeout(parent, lockBudget)
}

// updateFileRegion marshals each handle and writes it into the correct position within its block.
func (hm *hashmap) updateFileRegion(ctx context.Context, fileRegionDetails []fileRegionDetails) error {
	m := encoding.NewHandleMarshaler()
	buffer := make([]byte, 0, sop.HandleSizeInBytes)
	for _, frd := range fileRegionDetails {
		ba2, _ := m.Marshal(frd.handle, buffer)
		if err := hm.updateFileBlockRegion(ctx, frd.dio, frd.blockOffset, int(frd.handleInBlockOffset), ba2); err != nil {
			return err
		}
	}
	return nil
}

// markDeleteFileRegion zeroes out the handle-sized region inside a block to mark deletion.
// This results in visually clean zeroed sectors and keeps logic simple.
func (hm *hashmap) markDeleteFileRegion(ctx context.Context, fileRegionDetails []fileRegionDetails) error {
	// Study whether we want to zero out only the "Logical ID" part. For now, zero out entire Handle block
	// which could aid in cleaner deleted blocks(as marked w/ all zeroes). Negligible difference in IO.
	for _, frd := range fileRegionDetails {

		log.Debug(fmt.Sprintf("marking deleted file %s, sector offset %v, offset in block %v", frd.dio.filename, frd.blockOffset, frd.handleInBlockOffset))
		if err := hm.updateFileBlockRegion(ctx, frd.dio, frd.blockOffset, int(frd.handleInBlockOffset), zeroSector); err != nil {
			return err
		}
	}
	return nil
}

// updateFileBlockRegion acquires a cache-backed lock for the target block region, reads the block,
// merges the handle data, writes back, and finally releases the lock. Retries acquiring the lock
// until timeout to avoid deadlocks across writers.
func (hm *hashmap) updateFileBlockRegion(ctx context.Context, dio *fileDirectIO, blockOffset int64, handleInBlockOffset int, handleData []byte) error {
	// Lock the block file region.
	var lk *sop.LockKey
	var err error
	var ok bool

	startTime := sop.Now()
	var tid sop.UUID
	for {
		ok, tid, lk, err = hm.lockFileBlockRegion(ctx, dio, blockOffset)
		if err != nil {
			return err
		}
		if ok {
			break
		}
		if err := sop.TimedOut(ctx, "lockFileBlockRegion", startTime, LockFileRegionDuration+(1*time.Minute)); err != nil {
			// If the context is canceled or the operation's context deadline was exceeded, return the raw error
			// so callers treat it as a normal timeout/cancellation and NOT a failover trigger.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			// Otherwise, convert to a lock acquisition failure to allow callers to attempt
			// stale-lock recovery (e.g., priority rollback) using the lock key in UserData.
			err = fmt.Errorf("updateFileBlockRegion failed: %w", err)
			log.Debug(err.Error())
			lk.LockID = tid
			return sop.Error{
				Code:     sop.LockAcquisitionFailure,
				Err:      err,
				UserData: lk,
			}
		}
		sop.RandomSleep(ctx)
	}

	alignedBuffer := dio.createAlignedBlock()

	// Derive a single deadline aligned to the lock TTL and use it for both read and write.
	opCtx, opCancel := deriveLockAlignedContext(ctx)
	defer opCancel()

	// Read the block file region data.
	if n, err := dio.readAt(opCtx, alignedBuffer, blockOffset); n != blockSize || err != nil {
		hm.unlockFileBlockRegion(ctx, lk)
		if err == nil {
			return fmt.Errorf("only partially (n=%d) read the block at offset %v", n, blockOffset)
		}
		return err
	}

	// Merge the updated Handle record w/ the read block file region data.
	copy(alignedBuffer[handleInBlockOffset:handleInBlockOffset+sop.HandleSizeInBytes], handleData)
	// Update the block file region with merged data.
	if n, err := dio.writeAt(opCtx, alignedBuffer, blockOffset); n != blockSize || err != nil {
		hm.unlockFileBlockRegion(ctx, lk)
		if err == nil {
			return fmt.Errorf("only partially (n=%d) wrote at block offset %v, data: %v", n, blockOffset, handleData)
		}
		return err
	}
	// Unlock the block file region.
	return hm.unlockFileBlockRegion(ctx, lk)

}

func (hm *hashmap) lockFileBlockRegion(ctx context.Context, dio *fileDirectIO, offset int64) (bool, sop.UUID, *sop.LockKey, error) {
	tid := hm.replicationTracker.tid
	if tid == sop.NilUUID {
		tid = sop.NewUUID()
	}
	s := hm.formatLockKey(dio.filename, offset)
	lk := hm.cache.CreateLockKeysForIDs([]sop.Tuple[string, sop.UUID]{
		{
			First:  s,
			Second: tid,
		},
	})
	ok, uuid, err := hm.cache.Lock(ctx, LockFileRegionDuration, lk)
	if err == nil && ok {
		// Confirm lock ownership, then write a per-sector claim marker to the file system.
		if isLocked, ierr := hm.cache.IsLocked(ctx, []*sop.LockKey{lk[0]}); ierr == nil && isLocked {
			modFileNumber := parseSegmentIndex(dio.filename)
			modFileSectorNumber := int(offset / int64(blockSize))
			pl := priorityLog{replicationTracker: hm.replicationTracker, tid: tid}
			if werr := pl.WriteRegistrySectorClaim(ctx, modFileNumber, modFileSectorNumber, tid); werr != nil {
				// If another process already created the marker, consider we lost the race: unlock and return not-acquired.
				if errors.Is(werr, os.ErrExist) {
					_ = hm.cache.Unlock(ctx, []*sop.LockKey{lk[0]})
					return false, sop.NilUUID, lk[0], nil
				}
				log.Debug(fmt.Sprintf("failed writing sector claim for %s idx=%d sector=%d: %v", dio.filename, modFileNumber, modFileSectorNumber, werr))
			}
		}
	}
	return ok, uuid, lk[0], err
}
func (hm *hashmap) unlockFileBlockRegion(ctx context.Context, lk *sop.LockKey) error {
	// Attempt to remove the sector claim based on the lock key's metadata before unlocking.
	// Try to parse the lock key into filename and offset to reconstruct the marker name.
	// Lock key format uses hm.formatLockKey("infs"+filename+offset), so split filename/offset heuristically.
	// Best-effort: only act when we can derive a valid segment index and sector number.
	// Note: We can't access dio here, so parse from the key string.
	fn, off, ok := parseFilenameAndOffsetFromLockKey(lk.Key)
	if ok {
		modFileNumber := parseSegmentIndex(fn)
		modFileSectorNumber := int(off / int64(blockSize))
		pl := priorityLog{replicationTracker: hm.replicationTracker}
		exists, _ := pl.RemoveRegistrySectorClaim(ctx, modFileNumber, modFileSectorNumber)
		if !exists {
			hm.cache.Unlock(ctx, []*sop.LockKey{lk})

			// Another transaction may have removed the claim due to timeout waiting for its turn.
			// This should cause a transaction rollback for the caller.
			return fmt.Errorf("sector claim is missing for %s idx=%d sector=%d", fn, modFileNumber, modFileSectorNumber)
		}
	}
	return hm.cache.Unlock(ctx, []*sop.LockKey{lk})
}

func (hm *hashmap) formatLockKey(filename string, offset int64) string {
	return hm.cache.FormatLockKey(fmt.Sprintf("%s%s%v", lockFileRegionKeyPrefix, filename, offset))
}

// parseSegmentIndex extracts the numeric segment index from a segment filename like "table-12.reg".
// Returns 0 when parsing fails.
func parseSegmentIndex(segmentFilename string) int {
	// Expect pattern: <name>-<index>.reg
	dash := strings.LastIndex(segmentFilename, "-")
	dot := strings.LastIndex(segmentFilename, ".")
	if dash == -1 || dot == -1 || dot <= dash+1 {
		return 0
	}
	n, err := strconv.Atoi(segmentFilename[dash+1 : dot])
	if err != nil {
		return 0
	}
	return n
}

// parseFilenameAndOffsetFromLockKey attempts to pull the original filename and numeric offset from a lock key string.
// The key is built as FormatLockKey("infs" + filename + offset). We strip any global format prefix, then split by the
// last dash/dot pattern to isolate the filename; the trailing digits are interpreted as the offset.
func parseFilenameAndOffsetFromLockKey(key string) (string, int64, bool) {
	// Remove possible cache formatting; ensure our prefix is present.
	idx := strings.Index(key, lockFileRegionKeyPrefix)
	if idx == -1 {
		return "", 0, false
	}
	raw := key[idx+len(lockFileRegionKeyPrefix):]
	// raw is filename + offset concatenated. We know filename has ".reg" and offset is digits at the end.
	// Find the last occurrence of ".reg" and parse digits after it.
	regIdx := strings.LastIndex(raw, registryFileExtension)
	if regIdx == -1 {
		return "", 0, false
	}
	filename := raw[:regIdx+len(registryFileExtension)]
	offStr := raw[regIdx+len(registryFileExtension):]
	if offStr == "" {
		return "", 0, false
	}
	off, err := strconv.ParseInt(offStr, 10, 64)
	if err != nil {
		return "", 0, false
	}
	return filename, off, true
}
