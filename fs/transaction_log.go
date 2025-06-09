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

	"github.com/SharedCode/sop"
)

const (
	// DateHourLayout format mask string.
	DateHourLayout   = "2006-01-02T15"
	logFileExtension = ".log"
)

type TransactionLog struct {
	hourLockKey        *sop.LockKey
	cache              sop.Cache
	replicationTracker *replicationTracker
	tid                sop.UUID
	file               *os.File
	encoder            *json.Encoder
	writer             *bufio.Writer
}

var ageLimit float64 = 70

// NewTransactionLog instantiates a new TransactionLog instance.
func NewTransactionLog(cache sop.Cache, rt *replicationTracker) *TransactionLog {
	return &TransactionLog{
		cache:              cache,
		hourLockKey:        cache.CreateLockKeys([]string{"HBP"})[0],
		replicationTracker: rt,
	}
}

// Add transaction log w/ payload blob to the transaction log file.
func (tl *TransactionLog) Add(ctx context.Context, tid sop.UUID, commitFunction int, payload []byte) error {
	if tl.file == nil {
		tl.tid = tid
		filename := tl.format(tid)
		f, err := os.Create(filename)
		if err != nil {
			return err
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

// Remove will delete transaction log(t_log) records given a transaction ID(tid).
func (tl *TransactionLog) Remove(ctx context.Context, tid sop.UUID) error {
	if tl.tid == tid && tl.file != nil {
		tl.file.Close()
		tl.file = nil
	}
	return os.Remove(tl.format(tid))
}

// NewUUID generates a new sop UUID, currently a pass-through to google's uuid package.
func (tl *TransactionLog) NewUUID() sop.UUID {
	return sop.NewUUID()
}

// GetOne fetches an expired Transaction ID(TID), the hour it was created in and transaction logs for this TID.
func (tl *TransactionLog) GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, []byte], error) {
	duration := time.Duration(7 * time.Hour)

	hlk := []*sop.LockKey{tl.hourLockKey}
	if ok, err := tl.cache.Lock(ctx, duration, hlk); !ok || err != nil {
		return sop.NilUUID, "", nil, nil
	}

	hour, tid, err := tl.getOne()
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

func (tl *TransactionLog) GetLogsDetails(ctx context.Context, hour string) (sop.UUID, []sop.KeyValuePair[int, []byte], error) {
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
	_, tid, err = tl.getOne()
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

// Log commit changes to its own log file separate than the rest of transaction logs.
// This is a special log file only used during "reinstate" of drives back for replication.
func (tl *TransactionLog) LogCommitChanges(ctx context.Context, stores []sop.StoreInfo, newRootNodesHandles, addedNodesHandles, updatedNodesHandles, removedNodesHandles []sop.RegistryPayload[sop.Handle]) {
	tl.replicationTracker.logCommitChanges(tl.tid, stores, newRootNodesHandles, addedNodesHandles, updatedNodesHandles, removedNodesHandles)
}

func (tl *TransactionLog) getOne() (string, sop.UUID, error) {

	mh, _ := time.Parse(DateHourLayout, sop.Now().Format(DateHourLayout))
	cappedHour := mh.Add(-time.Duration(time.Duration(ageLimit) * time.Minute))

	files, err := getFilesSortedByModifiedTime(tl.replicationTracker.getActiveBaseFolder())
	if err != nil {
		return "", sop.NilUUID, err
	}

	// Get the oldest first.
	for i := 0; i < len(files); i++ {
		// 70 minute capped hour as transaction has a max of 60min "commit time". 10 min
		// gap ensures no issue due to overlapping.
		fts := files[i].ModTime.Format(DateHourLayout)
		ft, _ := time.Parse(DateHourLayout, fts)
		if cappedHour.Compare(ft) >= 0 {
			filename := files[i].Name()
			tid, err := sop.ParseUUID(filename[0 : len(filename)-len(logFileExtension)])
			if err != nil {
				continue
			}
			return fts, tid, nil
		} else {
			break
		}
	}

	return "", sop.NilUUID, nil
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

		err := json.Unmarshal([]byte(line), &data)
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
	return tl.replicationTracker.formatActiveFolderEntity(fmt.Sprintf("%s%s", tid.String(), logFileExtension))
}

// Directory files' reader.

// FileInfoWithModTime struct to hold FileInfo and modified time for sorting
type FileInfoWithModTime struct {
	os.DirEntry
	ModTime time.Time
}

// ByModTime implements sort.Interface for []FileInfoWithModTime based on ModTime
type ByModTime []FileInfoWithModTime

func (fis ByModTime) Len() int {
	return len(fis)
}

func (fis ByModTime) Swap(i, j int) {
	fis[i], fis[j] = fis[j], fis[i]
}

func (fis ByModTime) Less(i, j int) bool {
	return fis[i].ModTime.After(fis[j].ModTime)
}

// Reads a directory then returns the filenames sorted in descending order as driven by the files' modified time.
func getFilesSortedByModifiedTime(directoryPath string) ([]FileInfoWithModTime, error) {
	files, err := os.ReadDir(directoryPath)
	if err != nil {
		return nil, fmt.Errorf("error reading directory: %v", err)
	}

	fileInfoWithTimes := make([]FileInfoWithModTime, 0, len(files))
	for _, file := range files {
		inf, _ := file.Info()
		if strings.HasSuffix(file.Name(), logFileExtension) {
			fileInfoWithTimes = append(fileInfoWithTimes, FileInfoWithModTime{file, inf.ModTime()})
		}
	}

	sort.Sort(ByModTime(fileInfoWithTimes))

	return fileInfoWithTimes, nil
}
