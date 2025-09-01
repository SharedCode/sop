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
	priorityLogFileExtension       = ".plg"
	priorityLogBackupFileExtension = ".plb"
	priorityLogMinAgeInMin         = 5
)

// priorityLog persists per-transaction payloads that guide prioritized replication/work.
// Files are stored under the active folder with a .plg extension and are aged before batching.
type priorityLog struct {
	replicationTracker *replicationTracker
	tid                sop.UUID
}

// IsEnabled reports whether priority logging is enabled.
func (l priorityLog) IsEnabled() bool {
	return true
}

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
	if ba, err := fio.ReadFile(ctx, filename); err != nil {
		return nil, err
	} else {
		var data []sop.RegistryPayload[sop.Handle]
		err := encoding.DefaultMarshaler.Unmarshal(ba, &data)
		return data, err
	}
}

// GetBatch returns up to batchSize oldest priority log entries ready for processing.
// Entries are considered ready when their last-modified time is older than
// priorityLogMinAgeInMin from the current hour (capped to the hour).
func (l priorityLog) GetBatch(ctx context.Context, batchSize int) ([]sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]], error) {
	mh, _ := time.Parse(DateHourLayout, sop.Now().Format(DateHourLayout))
	cappedHour := mh.Add(-time.Duration(priorityLogMinAgeInMin * time.Minute))

	f := func(de os.DirEntry) bool {
		info, _ := de.Info()

		fts := info.ModTime().Format(DateHourLayout)
		ft, _ := time.Parse(DateHourLayout, fts)
		filename := info.Name()
		_, err := sop.ParseUUID(filename[0 : len(filename)-len(priorityLogFileExtension)])
		if err != nil {
			return false
		}
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

	// 25 is default batch size.
	if batchSize <= 0 {
		batchSize = 25
	}

	// Get the oldest first & so on...
	res := make([]sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]], 0, batchSize)
	for i := range batchSize {
		if i == len(files) {
			break
		}
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
		res = append(res, sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]]{
			Key:   tid,
			Value: r,
		})
	}
	return res, nil
}

// Remove deletes the priority log for a transaction, if present.
func (l priorityLog) Remove(ctx context.Context, tid sop.UUID) error {
	fio := newFileIO(sop.FileIOErrorFailoverQualified)
	filename := l.replicationTracker.formatActiveFolderEntity(fmt.Sprintf("%s%c%s%s", logFolder, os.PathSeparator, tid.String(), priorityLogFileExtension))
	if fio.Exists(ctx, filename) {
		return fio.Remove(ctx, filename)
	}
	return nil
}

// WriteBackup writes a backup copy of the priority log payload.
// Similar to Add, this ignores errors (best-effort) and returns nil.
func (l priorityLog) WriteBackup(ctx context.Context, tid sop.UUID, payload []byte) error {
	filename := l.replicationTracker.formatActiveFolderEntity(fmt.Sprintf("%s%c%s%s", logFolder, os.PathSeparator, tid.String(), priorityLogBackupFileExtension))
	fio := NewFileIO()
	fio.WriteFile(ctx, filename, payload, permission)
	return nil
}

// RemoveBackup deletes the backup copy of the priority log payload.
func (l priorityLog) RemoveBackup(ctx context.Context, tid sop.UUID) error {
	filename := l.replicationTracker.formatActiveFolderEntity(fmt.Sprintf("%s%c%s%s", logFolder, os.PathSeparator, tid.String(), priorityLogBackupFileExtension))
	fio := NewFileIO()
	fio.Remove(ctx, filename)
	return nil
}
