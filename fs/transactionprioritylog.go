package fs

import (
	"context"
	"fmt"
	log "log/slog"
	"os"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/encoding"
)

const (
	priorityLogFileExtension = ".plg"
	priorityLogMinAgeInMin   = 5
	// regionSignalFolder is where per-sector claim markers are written.
	// Kept separate from priority log folder to avoid interfering with batching logic.
	regionSignalFolder = "regionsignals"
	// regionSignalBucketCount defines how many sub-folders we shard into per level
	// to avoid a single hot directory. We use a two-level directory structure:
	// regionsignals/XX/YY/<filename>
	// where XX and YY are zero-padded [00..N-1].
	regionSignalBucketCount = 20
)

// priorityLog persists per-transaction payloads that guide prioritized replication/work.
// Files are stored under the active folder with a .plg extension and are aged before batching.
type priorityLog struct {
	replicationTracker *replicationTracker
	tid                sop.UUID
}

// IsEnabled reports whether priority logging is enabled.
func (l priorityLog) IsEnabled() bool { return true }

// Add writes the priority log payload for a transaction.
func (l priorityLog) Add(ctx context.Context, tid sop.UUID, payload []byte) error {
	filename := l.replicationTracker.formatActiveFolderEntity(fmt.Sprintf("%s%c%s%s", logFolder, os.PathSeparator, tid.String(), priorityLogFileExtension))
	fio := NewFileIO()
	return fio.WriteFile(ctx, filename, payload, permission)
}

// LogCommitChanges persists commit-change metadata used when reinstating failed drives.
func (l priorityLog) LogCommitChanges(ctx context.Context, stores []sop.StoreInfo, newRootNodesHandles, addedNodesHandles, updatedNodesHandles, removedNodesHandles []sop.RegistryPayload[sop.Handle]) error {
	return l.replicationTracker.logCommitChanges(ctx, l.tid, stores, newRootNodesHandles, addedNodesHandles, updatedNodesHandles, removedNodesHandles)
}

// Get loads the priority log payload for a transaction, if present.
func (l priorityLog) Get(ctx context.Context, tid sop.UUID) ([]sop.RegistryPayload[sop.Handle], error) {
	filename := l.replicationTracker.formatActiveFolderEntity(fmt.Sprintf("%s%c%s%s", logFolder, os.PathSeparator, tid.String(), priorityLogFileExtension))
	fio := NewFileIO()
	if !fio.Exists(ctx, filename) {
		return nil, nil
	}
	ba, err := fio.ReadFile(ctx, filename)
	if err != nil {
		return nil, err
	}
	var data []sop.RegistryPayload[sop.Handle]
	if err := encoding.DefaultMarshaler.Unmarshal(ba, &data); err != nil {
		return nil, err
	}
	return data, nil
}

// GetBatch returns up to batchSize oldest priority log entries ready for processing.
// Entries are considered ready when their last-modified time is older than
// priorityLogMinAgeInMin from the current hour (capped to the hour).
func (l priorityLog) GetBatch(ctx context.Context, batchSize int) ([]sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]], error) {
	mh, _ := time.Parse(DateHourLayout, sop.Now().Format(DateHourLayout))
	cappedHour := mh.Add(-time.Duration(priorityLogMinAgeInMin * time.Minute))
	ignoreAge := false
	if v := ctx.Value(sop.ContextPriorityLogIgnoreAge); v != nil {
		if b, ok := v.(bool); ok && b {
			ignoreAge = true
		}
	}

	f := func(de os.DirEntry) bool {
		info, _ := de.Info()
		filename := info.Name()
		if _, err := sop.ParseUUID(filename[0 : len(filename)-len(priorityLogFileExtension)]); err != nil {
			return false
		}
		if ignoreAge {
			return true
		}
		fts := info.ModTime().Format(DateHourLayout)
		ft, _ := time.Parse(DateHourLayout, fts)
		return cappedHour.Compare(ft) >= 0
	}

	fn := l.replicationTracker.formatActiveFolderEntity(logFolder)
	fio := NewFileIO()
	if !fio.Exists(ctx, fn) {
		if err := fio.MkdirAll(ctx, fn, permission); err != nil {
			log.Warn(fmt.Sprintf("error creating %s, details: %v", fn, err))
		}
		return nil, nil
	}
	files, err := getFilesSortedDescByModifiedTime(ctx, fn, priorityLogFileExtension, f)
	if err != nil || len(files) == 0 {
		return nil, err
	}
	if batchSize <= 0 {
		batchSize = 25
	}

	res := make([]sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]], 0, batchSize)
	for i := 0; i < len(files) && i < batchSize; i++ {
		filename := files[i].Name()
		tid, te := sop.ParseUUID(filename[0 : len(filename)-len(priorityLogFileExtension)])
		if te != nil {
			log.Warn("file %s does not belong in this folder, details: %s", filename, te)
			continue
		}
		r, e := l.Get(ctx, tid)
		if e != nil {
			return res, e
		}
		res = append(res, sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]]{Key: tid, Value: r})
	}
	return res, nil
}

// Remove deletes the priority log for a transaction, if present.
func (l priorityLog) Remove(ctx context.Context, tid sop.UUID) error {
	fio := NewFileIO()
	filename := l.replicationTracker.formatActiveFolderEntity(fmt.Sprintf("%s%c%s%s", logFolder, os.PathSeparator, tid.String(), priorityLogFileExtension))
	if fio.Exists(ctx, filename) {
		return fio.Remove(ctx, filename)
	}
	return nil
}

// formatRegistrySectorClaimName builds the filename r[modFileNumber][modFileSectorNumber].plg
// Example: r12035.plg for modFileNumber=12 and modFileSectorNumber=035.
// No separators are used to keep the name compact; callers should choose widths to avoid ambiguity if needed.
func formatRegistrySectorClaimName(modFileNumber int, modFileSectorNumber int) string {
	// Zero-padded: 4 digits for file number, 6 digits for sector number (supports up to 9999 files, 999999 sectors).
	return fmt.Sprintf("r%04d%06d%s", modFileNumber, modFileSectorNumber, priorityLogFileExtension)
}

// formatRegistrySectorClaimRelPath builds the bucketed relative path where the
// per-sector claim marker should live under the active folder.
// Example: regionsignals/07/13/r0012000035.plg
func formatRegistrySectorClaimRelPath(modFileNumber int, modFileSectorNumber int) string {
	filename := formatRegistrySectorClaimName(modFileNumber, modFileSectorNumber)
	// Two-level bucketing for better directory spreading.
	// Simple, fast, and deterministic: mix indices with small primes to vary distribution.
	b1 := (modFileNumber + modFileSectorNumber) % regionSignalBucketCount
	b2 := (modFileNumber*13 + modFileSectorNumber*7) % regionSignalBucketCount
	return fmt.Sprintf("%s%c%02d%c%02d%c%s", regionSignalFolder, os.PathSeparator, b1, os.PathSeparator, b2, os.PathSeparator, filename)
}

// RegistrySectorClaimExists checks if the per-sector claim marker already exists.
func (l priorityLog) RegistrySectorClaimExists(ctx context.Context, modFileNumber int, modFileSectorNumber int) bool {
	fn := l.replicationTracker.formatActiveFolderEntity(formatRegistrySectorClaimRelPath(modFileNumber, modFileSectorNumber))
	return NewFileIO().Exists(ctx, fn)
}

// WriteRegistrySectorClaim writes a small per-sector claim marker file with empty content (fast/light).
// The file is named r[modFileNumber][modFileSectorNumber].plg and stored under regionSignalFolder.
// Implementation uses O_CREAT|O_EXCL to atomically create the marker; if it already exists, EEXIST is returned.
func (l priorityLog) WriteRegistrySectorClaim(ctx context.Context, modFileNumber int, modFileSectorNumber int, _ sop.UUID) error {
	// Ensure bucket directories exist then create the marker atomically.
	fio := NewFileIO()
	// Build the two-level bucket dir.
	b1 := (modFileNumber + modFileSectorNumber) % regionSignalBucketCount
	b2 := (modFileNumber*13 + modFileSectorNumber*7) % regionSignalBucketCount
	bucketDir := l.replicationTracker.formatActiveFolderEntity(fmt.Sprintf("%s%c%02d%c%02d", regionSignalFolder, os.PathSeparator, b1, os.PathSeparator, b2))
	_ = fio.MkdirAll(ctx, bucketDir, permission)

	target := l.replicationTracker.formatActiveFolderEntity(formatRegistrySectorClaimRelPath(modFileNumber, modFileSectorNumber))
	// Atomically create the marker file; fails with EEXIST if already present.
	f, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, permission)
	if err != nil {
		if os.IsExist(err) {
			return os.ErrExist
		}
		return err
	}
	_ = f.Close()
	return nil
}

// RemoveRegistrySectorClaim removes the per-sector claim marker file if present.
func (l priorityLog) RemoveRegistrySectorClaim(ctx context.Context, modFileNumber int, modFileSectorNumber int) (bool, error) {
	fn := l.replicationTracker.formatActiveFolderEntity(formatRegistrySectorClaimRelPath(modFileNumber, modFileSectorNumber))
	fio := NewFileIO()
	if fio.Exists(ctx, fn) {
		return true, fio.Remove(ctx, fn)
	}
	return false, nil
}

// ClearRegistrySectorClaims deletes the entire per-sector claim folder tree under the active folder.
// It is safe to call even if the directory does not exist.
func (l priorityLog) ClearRegistrySectorClaims(ctx context.Context) error {
	base := l.replicationTracker.formatActiveFolderEntity(regionSignalFolder)
	fio := NewFileIO()
	if !fio.Exists(ctx, base) {
		return nil
	}
	return fio.RemoveAll(ctx, base)
}
