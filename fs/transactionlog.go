package fs

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/encoding"
)

const (
	// DateHourLayout is the time layout used for hour-bucketed log folders and files.
	DateHourLayout   = "2006-01-02T15"
	logFileExtension = ".log"
	logFolder        = "translogs"
)

// TransactionLog writes per-transaction logs to local storage and supports scanning and cleanup
// of aged records. It also coordinates with a priority log for replication scenarios.
type TransactionLog struct {
	priorityLog
	hourLockKey *sop.LockKey
	cache       sop.Cache
	file        *os.File
	encoder     *json.Encoder
	writer      *bufio.Writer
}

var ageLimit float64 = 70

// NewTransactionLog constructs a TransactionLog bound to the provided cache and replication tracker.
func NewTransactionLog(cache sop.Cache, rt *replicationTracker) *TransactionLog {
	return &TransactionLog{
		cache:       cache,
		hourLockKey: cache.CreateLockKeys([]string{"HBP"})[0],
		priorityLog: priorityLog{
			replicationTracker: rt,
		},
	}
}

// PriorityLog returns the FS-backed priority log for commit-change logging.
func (tl *TransactionLog) PriorityLog() sop.TransactionPriorityLog {
	return tl.priorityLog
}

// Add appends a commit record with payload to this transaction's log file, creating the file if needed.
func (tl *TransactionLog) Add(ctx context.Context, tid sop.UUID, commitFunction int, payload []byte) error {
	if tl.file == nil {
		tl.tid = tid
		filename := tl.format(tid)

		f, err := os.Create(filename)
		if err != nil {
			fio := NewFileIO()
			baseFolder := tl.replicationTracker.formatActiveFolderEntity(logFolder)
			if !fio.Exists(ctx, baseFolder) {
				fio.MkdirAll(ctx, baseFolder, permission)
			}
			f, err = os.Create(filename)
			if err != nil {
				return err
			}
		}
		tl.file = f
		tl.writer = bufio.NewWriter(f)
		tl.encoder = json.NewEncoder(tl.writer)
	}
	// Append the log entry.
	if err := tl.encoder.Encode(sop.KeyValuePair[int, []byte]{
		Key:   commitFunction,
		Value: payload,
	}); err != nil {
		tl.writer.Flush()
		tl.file.Close()
		tl.file = nil
		return err
	}
	tl.writer.Flush()

	return nil
}

// Remove deletes the log file for the specified transaction ID.
func (tl *TransactionLog) Remove(ctx context.Context, tid sop.UUID) error {

	if tl.tid == tid && tl.file != nil {
		tl.file.Close()
		tl.file = nil
	}
	return os.Remove(tl.format(tid))
}

// NewUUID returns a new unique identifier for correlating log files.
func (tl *TransactionLog) NewUUID() sop.UUID {
	return sop.NewUUID()
}

// GetOne claims one expired transaction hour bucket and returns a tid and its records for cleanup processing.
func (tl *TransactionLog) GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, []byte], error) {
	duration := time.Duration(7 * time.Hour)

	hlk := []*sop.LockKey{tl.hourLockKey}
	if ok, _, err := tl.cache.Lock(ctx, duration, hlk); !ok || err != nil {
		return sop.NilUUID, "", nil, nil
	}

	hour, tid, err := tl.getOne(ctx)
	if err != nil {
		tl.cache.Unlock(ctx, hlk)
		return sop.NilUUID, hour, nil, err
	}
	if tid.IsNil() {
		// Unlock the hour.
		tl.cache.Unlock(ctx, hlk)
		return sop.NilUUID, "", nil, nil
	}

	r, err := tl.getLogsDetails(tid)
	if err != nil {
		tl.cache.Unlock(ctx, hlk)
		return sop.NilUUID, "", nil, err
	}
	// Check one more time to remove potential (.1%) race condition issue.
	if ok, err := tl.cache.IsLocked(ctx, hlk); !ok || err != nil {
		tl.cache.Unlock(ctx, hlk)
		// Just return nils as we can't attain a lock.
		return sop.NilUUID, "", nil, nil
	}
	return sop.UUID(tid), hour, r, nil
}

// GetOneOfHour returns a tid and records for the specified hour if the bucket is within the TTL window.
func (tl *TransactionLog) GetOneOfHour(ctx context.Context, hour string) (sop.UUID, []sop.KeyValuePair[int, []byte], error) {
	if hour == "" {
		return sop.NilUUID, nil, nil
	}

	t, err := time.Parse(DateHourLayout, hour)
	if err != nil {
		return sop.NilUUID, nil, err
	}

	// Put a max time of three hours for a given cleanup processor.
	mh, _ := time.Parse(DateHourLayout, sop.Now().Format(DateHourLayout))
	if mh.Sub(t).Hours() > 4 {
		// Unlock the hour to allow open opportunity to claim the next cleanup processing.
		// Capping to 4th hour(Redis cache is set to 7hrs) maintains only one cleaner process at a time.
		tl.cache.Unlock(ctx, []*sop.LockKey{tl.hourLockKey})
		return sop.NilUUID, nil, nil
	}

	var tid sop.UUID
	_, tid, err = tl.getOne(ctx)
	if err != nil {
		return tid, nil, err
	}

	if tid.IsNil() {
		// Unlock the hour.
		tl.cache.Unlock(ctx, []*sop.LockKey{tl.hourLockKey})
		return sop.NilUUID, nil, nil
	}
	r, err := tl.getLogsDetails(tid)

	return tid, r, err
}

func (tl *TransactionLog) getOne(ctx context.Context) (string, sop.UUID, error) {
	mh, _ := time.Parse(DateHourLayout, sop.Now().Format(DateHourLayout))
	cappedHour := mh.Add(-time.Duration(time.Duration(ageLimit) * time.Minute))

	f := func(de os.DirEntry) bool {
		info, _ := de.Info()

		fts := info.ModTime().Format(DateHourLayout)
		ft, _ := time.Parse(DateHourLayout, fts)
		filename := info.Name()
		_, err := sop.ParseUUID(filename[0 : len(filename)-len(logFileExtension)])
		if err != nil {
			return false
		}
		return cappedHour.Compare(ft) >= 0
	}

	fn := tl.replicationTracker.formatActiveFolderEntity(logFolder)
	files, err := getFilesSortedDescByModifiedTime(ctx, fn, logFileExtension, f)
	if err != nil || len(files) == 0 {
		return "", sop.NilUUID, err
	}

	// Get the oldest first.
	filename := files[0].Name()
	tid, _ := sop.ParseUUID(filename[0 : len(filename)-len(logFileExtension)])
	return files[0].ModTime.Format(DateHourLayout), tid, nil
}

func (tl *TransactionLog) getLogsDetails(tid sop.UUID) ([]sop.KeyValuePair[int, []byte], error) {

	filename := tl.format(tid)
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	r := make([]sop.KeyValuePair[int, []byte], 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		var data sop.KeyValuePair[int, []byte]

		err := encoding.DefaultMarshaler.Unmarshal([]byte(line), &data)
		if err != nil {
			log.Error(fmt.Sprintf("error unmarshaling JSON: %v", err))
			continue // Skip to the next line if there's an error
		}
		r = append(r, data)
	}

	if err := scanner.Err(); err != nil {
		return r, fmt.Errorf("error reading file: %v", err)
	}

	return r, nil
}

func (tl *TransactionLog) format(tid sop.UUID) string {
	return tl.replicationTracker.formatActiveFolderEntity(fmt.Sprintf("%s%c%s%s", logFolder, os.PathSeparator, tid.String(), logFileExtension))
}

// Directory files' reader.

// FileInfoWithModTime associates a DirEntry with its modification timestamp for sorting purposes.
type FileInfoWithModTime struct {
	os.DirEntry
	ModTime time.Time
}

// ByModTime sorts FileInfoWithModTime by modification time.
type ByModTime []FileInfoWithModTime

func (fis ByModTime) Len() int {
	return len(fis)
}

func (fis ByModTime) Swap(i, j int) {
	fis[i], fis[j] = fis[j], fis[i]
}

func (fis ByModTime) Less(i, j int) bool {
	return fis[i].ModTime.Before(fis[j].ModTime)
}

// getFilesSortedDescByModifiedTime lists files in descending order by modification time,
// filtered by extension and an optional predicate. Directory will be created if missing.
func getFilesSortedDescByModifiedTime(ctx context.Context, directoryPath string, fileSuffix string, filter func(os.DirEntry) bool) ([]FileInfoWithModTime, error) {
	fio := NewFileIO()

	// Auto create the directory path if not exists.
	if !fio.Exists(ctx, directoryPath) {
		if err := fio.MkdirAll(ctx, directoryPath, permission); err != nil {
			return nil, err
		}
	}
	files, err := fio.ReadDir(ctx, directoryPath)
	if err != nil && len(files) == 0 {
		return nil, fmt.Errorf("error reading directory: %v", err)
	}

	fileInfoWithTimes := make([]FileInfoWithModTime, 0, len(files)/2)
	for _, file := range files {
		inf, _ := file.Info()
		if strings.HasSuffix(file.Name(), fileSuffix) {
			if filter != nil {
				if !filter(file) {
					continue
				}
			}
			fileInfoWithTimes = append(fileInfoWithTimes, FileInfoWithModTime{file, inf.ModTime()})
		}
	}

	sort.Sort(ByModTime(fileInfoWithTimes))

	return fileInfoWithTimes, err
}
