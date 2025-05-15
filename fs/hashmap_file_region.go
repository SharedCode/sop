package fs

import (
	"bytes"
	"context"
	"fmt"
	log "log/slog"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
)

const (
	lockSectorRetryMax           = 7
	lockSectorRetryTimeoutInSecs = 5
)

func (hm *hashmap) updateFileRegion(ctx context.Context, fileRegionDetails ...fileRegionDetails) error {
	dio := newDirectIO()
	ba := dio.createAlignedBlock()
	m := encoding.NewHandleMarshaler()
	for _, frd := range fileRegionDetails {
		ba2, _ := m.Marshal(frd.handle)
		if err := hm.updateFileBlockRegion(ctx, frd.dio, frd.blockOffset, int(frd.handleInBlockOffset), ba2, ba); err != nil {
			return err
		}
	}
	return nil
}

func (hm *hashmap) markDeleteFileRegion(ctx context.Context, fileRegionDetails ...fileRegionDetails) error {
	dio := newDirectIO()
	ba := dio.createAlignedBlock()
	// Study whether we want to zero out only the "Logical ID" part. For now, zero out entire Handle block
	// which could aid in cleaner deleted blocks(as marked w/ all zeroes). Negligible difference in IO.
	ba2 := bytes.Repeat([]byte{0}, sop.HandleSizeInBytes)
	for _, frd := range fileRegionDetails {
		if err := hm.updateFileBlockRegion(ctx, frd.dio, frd.blockOffset, int(frd.handleInBlockOffset), ba2, ba); err != nil {
			return err
		}
	}
	return nil
}

func (hm *hashmap) updateFileBlockRegion(ctx context.Context, dio *directIO, blockOffset int64, handleInBlockOffset int, handleData []byte, alignedBuffer []byte) error {
	// Lock the block file region.
	var lk *sop.LockKey
	var err error
	var ok bool

	startTime := sop.Now()
	ctr := 0
	for {
		ok, lk, err = hm.lockFileBlockRegion(ctx, dio, blockOffset)
		if err != nil {
			return err
		}
		if ok {
			// Double check to ensure we have no race condition and 100% acquired a lock on the sector.
			if ok, err := hm.cache.IsLocked(ctx, lk); ok {
				break
			} else if err != nil {
				// Unlock the sector just in case it can "get through", before return.
				hm.unlockFileBlockRegion(ctx, dio, blockOffset, lk)
				return err
			}
		}
		if err := sop.TimedOut(ctx, "lockFileBlockRegion", startTime, time.Duration(lockSectorRetryTimeoutInSecs*time.Second)); err != nil {
			log.Debug(fmt.Sprintf("updateFileBlockRegion retry loop: %v", err))
			return err
		}
		if ctr >= lockSectorRetryMax {
			return fmt.Errorf("can't lock file '%s' region at block offset %v, it's locked by another", dio.filename, blockOffset)
		}
		sop.RandomSleep(ctx)
		ctr++
	}

	// Read the block file region data.
	if n, err := dio.readAt(alignedBuffer, blockOffset); n != blockSize || err != nil {
		hm.unlockFileBlockRegion(ctx, dio, blockOffset, lk)
		if err == nil {
			return fmt.Errorf("only partially (n=%d) read the block at offset %v", n, blockOffset)
		}
		return err
	}

	// Merge the updated Handle record w/ the read block file region data.
	copy(alignedBuffer[handleInBlockOffset:handleInBlockOffset+sop.HandleSizeInBytes], handleData)
	// Update the block file region with merged data.
	if n, err := dio.writeAt(alignedBuffer, blockOffset); n != blockSize || err != nil {
		hm.unlockFileBlockRegion(ctx, dio, blockOffset, lk)
		if err == nil {
			return fmt.Errorf("only partially (n=%d) wrote at block offset %v, data: %v", n, blockOffset, handleData)
		}
		return err
	}
	// Unlock the block file region.
	return hm.unlockFileBlockRegion(ctx, dio, blockOffset, lk)
}

func (hm *hashmap) lockFileBlockRegion(ctx context.Context, dio *directIO, offset int64) (bool, *sop.LockKey, error) {
	lk := hm.cache.CreateLockKeys(hm.formatLockKey(dio.filename, offset))[0]
	ok, err := hm.cache.Lock(ctx, lockFileRegionDuration, lk)
	return ok, lk, err
}
func (hm *hashmap) unlockFileBlockRegion(ctx context.Context, dio *directIO, offset int64, lk *sop.LockKey) error {
	return hm.cache.Unlock(ctx, lk)
}

func (hm *hashmap) formatLockKey(filename string, offset int64) string {
	return hm.cache.FormatLockKey(fmt.Sprintf("%s%s%v", lockFileRegionKeyPrefix, filename, offset))
}
