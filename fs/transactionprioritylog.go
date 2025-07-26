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

type priorityLog struct {
	replicationTracker *replicationTracker
	tid                sop.UUID
}

func (l priorityLog) IsEnabled() bool {
	return true
}

// Add transaction log w/ payload blob to the transaction log file.
func (l priorityLog) Add(ctx context.Context, tid sop.UUID, payload []byte) error {
	filename := l.replicationTracker.formatActiveFolderEntity(fmt.Sprintf("%s%c%s%s", logFolder, os.PathSeparator, tid.String(), priorityLogFileExtension))
	fio := NewFileIO()
	fio.WriteFile(ctx, filename, payload, permission)
	return nil
}

// Log commit changes to its own log file separate than the rest of transaction logs.
// This is a special log file only used during "reinstate" of drives back for replication.
func (l priorityLog) LogCommitChanges(ctx context.Context, stores []sop.StoreInfo, newRootNodesHandles, addedNodesHandles, updatedNodesHandles, removedNodesHandles []sop.RegistryPayload[sop.Handle]) error {
	return l.replicationTracker.logCommitChanges(ctx, l.tid, stores, newRootNodesHandles, addedNodesHandles, updatedNodesHandles, removedNodesHandles)
}

// Fetch the transaction priority logs details given a tranasction ID.
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

	// 25 is defaut batch size.
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

// Remove will delete transaction log(t_log) records given a transaction ID(tid).
func (l priorityLog) Remove(ctx context.Context, tid sop.UUID) error {
	fio := NewFileIO()
	filename := l.replicationTracker.formatActiveFolderEntity(fmt.Sprintf("%s%c%s%s", logFolder, os.PathSeparator, tid.String(), priorityLogFileExtension))
	if fio.Exists(ctx, filename) {
		return fio.Remove(ctx, filename)
	}
	return nil
}

func (l priorityLog) WriteBackup(ctx context.Context, tid sop.UUID, payload []byte) error {
	filename := l.replicationTracker.formatActiveFolderEntity(fmt.Sprintf("%s%c%s%s", logFolder, os.PathSeparator, tid.String(), priorityLogBackupFileExtension))
	fio := NewFileIO()
	fio.WriteFile(ctx, filename, payload, permission)
	return nil
}

func (l priorityLog) RemoveBackup(ctx context.Context, tid sop.UUID) error {
	filename := l.replicationTracker.formatActiveFolderEntity(fmt.Sprintf("%s%c%s%s", logFolder, os.PathSeparator, tid.String(), priorityLogBackupFileExtension))
	fio := NewFileIO()
	fio.Remove(ctx, filename)
	return nil
}
