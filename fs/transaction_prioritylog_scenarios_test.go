package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
	"github.com/sharedcode/sop/encoding"
)

// This scenarios suite merges all behaviors previously covered across the various
// prioritylog_* and transactionlog_* test files. It targets:
//  - PriorityLog basic lifecycle (Add/Get/Remove, backups, IsEnabled)
//  - PriorityLog batching (age gating, default batch size, batchSize<=0 path, malformed filename skip)
//  - PriorityLog edge branches (missing file -> nil,nil; unmarshal error; empty eligible set; folder auto-create)
//  - TransactionLog Add / Remove (including second Remove), NewUUID
//  - TransactionLog GetOne (no files, eligible selection, records decoding)
//  - TransactionLog GetOneOfHour (empty hour fast path, bad hour parse error)
//  - getFilesSortedDescByModifiedTime directory creation + filter behavior
//  - Commit changes logging path when enabled via replicationTracker.LogCommitChanges
//  - fileDirectIO close error path

// helper to build a TransactionLog + replication tracker pair.
func newTLRT(t *testing.T) (*TransactionLog, *replicationTracker) {
	t.Helper()
	ctx := context.Background()
	baseA := filepath.Join(t.TempDir(), "a")
	baseB := filepath.Join(t.TempDir(), "b")
	rt, err := NewReplicationTracker(ctx, []string{baseA, baseB}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}
	tl := NewTransactionLog(mocks.NewMockClient(), rt)
	return tl, rt
}

func TestTransactionAndPriorityLog_Scenarios(t *testing.T) {
	ctx := context.Background()
	tl, rt := newTLRT(t)
	pl := tl.PriorityLog()

	// PriorityLog IsEnabled
	if !pl.IsEnabled() {
		t.Fatalf("PriorityLog IsEnabled expected true")
	}

	// Priority Add/Get with proper JSON
	tid1 := sop.NewUUID()
	payload1 := []sop.RegistryPayload[sop.Handle]{
		{RegistryTable: "rt1", BlobTable: "bt1", IDs: []sop.Handle{{LogicalID: sop.NewUUID()}}},
	}
	ba1, _ := encoding.DefaultMarshaler.Marshal(payload1)
	if err := pl.Add(ctx, tid1, ba1); err != nil {
		t.Fatalf("Priority Add: %v", err)
	}
	got, err := pl.Get(ctx, tid1)
	if err != nil || len(got) != 1 || got[0].RegistryTable != "rt1" {
		t.Fatalf("Priority Get mismatch: %+v err=%v", got, err)
	}

	// WriteBackup / RemoveBackup
	if err := pl.WriteBackup(ctx, tid1, ba1); err != nil {
		t.Fatalf("Priority WriteBackup: %v", err)
	}
	if err := pl.RemoveBackup(ctx, tid1); err != nil {
		t.Fatalf("Priority RemoveBackup: %v", err)
	}

	// Remove existing then again (graceful)
	if err := pl.Remove(ctx, tid1); err != nil {
		t.Fatalf("Priority Remove: %v", err)
	}
	if err := pl.Remove(ctx, tid1); err != nil {
		t.Fatalf("Priority Remove (second) unexpected err: %v", err)
	}

	// Edge: Get missing => nil,nil
	if r, e := pl.Get(ctx, sop.NewUUID()); e != nil || r != nil {
		t.Fatalf("expected nil,nil for missing priority log, got %v,%v", r, e)
	}

	// Unmarshal error path: write non-json file and Get -> error
	tidBad := sop.NewUUID()
	badFn := rt.formatActiveFolderEntity(filepath.Join(logFolder, tidBad.String()+priorityLogFileExtension))
	if err := os.MkdirAll(filepath.Dir(badFn), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(badFn, []byte("not-json"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if r, e := pl.Get(ctx, tidBad); e == nil || r != nil {
		t.Fatalf("expected unmarshal error, got %v,%v", r, e)
	}

	// Batching scenarios
	// 1. Folder auto-create & nil return when empty
	rtEmptyBase := filepath.Join(t.TempDir(), "emptyA")
	rtEmpty, err := NewReplicationTracker(ctx, []string{rtEmptyBase, filepath.Join(t.TempDir(), "b")}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("rtEmpty: %v", err)
	}
	plEmpty := NewTransactionLog(mocks.NewMockClient(), rtEmpty).PriorityLog()
	if batch, e := plEmpty.GetBatch(ctx, 5); e != nil || batch != nil {
		t.Fatalf("expected nil batch on empty folder, got %v err=%v", batch, e)
	}

	// 2. Ready vs not-ready (age gating) + batch size limit
	oldTid := sop.NewUUID()
	newTid := sop.NewUUID()
	logDir := rt.formatActiveFolderEntity(logFolder)
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("mkdir logDir: %v", err)
	}
	oldFile := filepath.Join(logDir, oldTid.String()+priorityLogFileExtension)
	newFile := filepath.Join(logDir, newTid.String()+priorityLogFileExtension)
	if err := os.WriteFile(oldFile, []byte("[]"), 0o644); err != nil {
		t.Fatalf("write old: %v", err)
	}
	if err := os.WriteFile(newFile, []byte("[]"), 0o644); err != nil {
		t.Fatalf("write new: %v", err)
	}
	nowHour, _ := time.Parse(DateHourLayout, time.Now().Format(DateHourLayout))
	past := nowHour.Add(-time.Duration((priorityLogMinAgeInMin + 10) * time.Minute))
	if err := os.Chtimes(oldFile, past, past); err != nil {
		t.Fatalf("chtimes old: %v", err)
	}
	batchOld, err := pl.GetBatch(ctx, 10)
	if err != nil || len(batchOld) == 0 || batchOld[0].Key != oldTid {
		t.Fatalf("expected oldTid in batch, got %v err=%v", batchOld, err)
	}

	// 3. Default batch size path (batchSize<=0) with multiple aged files
	agedIDs := []sop.UUID{sop.NewUUID(), sop.NewUUID(), sop.NewUUID()}
	for _, id := range agedIDs {
		fn := filepath.Join(logDir, id.String()+priorityLogFileExtension)
		if err := os.WriteFile(fn, []byte("[]"), 0o644); err != nil {
			t.Fatalf("write aged: %v", err)
		}
		past2 := time.Now().Add(-2 * time.Hour)
		if err := os.Chtimes(fn, past2, past2); err != nil {
			t.Fatalf("chtimes aged: %v", err)
		}
	}
	if batchDef, e := pl.GetBatch(ctx, 0); e != nil || len(batchDef) == 0 {
		t.Fatalf("expected default batch items, got %v err=%v", batchDef, e)
	}

	// 4. Malformed filename skip
	badName := filepath.Join(logDir, "badname"+priorityLogFileExtension)
	if err := os.WriteFile(badName, []byte("[]"), 0o644); err != nil {
		t.Fatalf("write badname: %v", err)
	}
	if batchAfterBad, e := pl.GetBatch(ctx, 0); e != nil || len(batchAfterBad) == 0 {
		t.Fatalf("expected non-empty batch after malformed, got %v err=%v", batchAfterBad, e)
	}

	// TransactionLog Add / Remove / NewUUID
	tidTx := tl.NewUUID()
	if tidTx.IsNil() {
		t.Fatalf("NewUUID returned nil")
	}
	payloads := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	for i, p := range payloads {
		if err := tl.Add(ctx, tidTx, i, p); err != nil {
			t.Fatalf("Tx Add %d: %v", i, err)
		}
	}
	if err := tl.Remove(ctx, tidTx); err != nil {
		t.Fatalf("Tx Remove: %v", err)
	}
	_ = tl.Remove(ctx, tidTx) // second path (ignore error)

	// Create another eligible transaction log file & an ineligible one to test GetOne
	elig := sop.NewUUID()
	inelig := sop.NewUUID()
	if err := tl.Add(ctx, elig, 1, []byte("e1")); err != nil {
		t.Fatalf("Add elig: %v", err)
	}
	if err := tl.Add(ctx, inelig, 1, []byte("i1")); err != nil {
		t.Fatalf("Add inelig: %v", err)
	}
	fnElig := rt.formatActiveFolderEntity(filepath.Join(logFolder, elig.String()+logFileExtension))
	agedTime := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(fnElig, agedTime, agedTime); err != nil {
		t.Fatalf("chtimes elig: %v", err)
	}

	// GetOne when eligible exists
	tidGot, hour, recs, err := tl.GetOne(ctx)
	if err != nil || tidGot.IsNil() || hour == "" || len(recs) == 0 {
		t.Fatalf("GetOne expected populated, got tid=%v hour=%s recs=%v err=%v", tidGot, hour, recs, err)
	}

	// GetOne with no eligible (fresh TL instance, empty folder)
	tlEmpty := NewTransactionLog(mocks.NewMockClient(), rt)
	_ = tlEmpty.Remove(ctx, sop.NewUUID()) // no-op just to touch path
	// Temporarily point to new empty folder
	rt2base := filepath.Join(t.TempDir(), "c")
	rt2, _ := NewReplicationTracker(ctx, []string{rt2base, filepath.Join(t.TempDir(), "d")}, false, mocks.NewMockClient())
	tlNoFiles := NewTransactionLog(mocks.NewMockClient(), rt2)
	if tid, hr, rr, e := tlNoFiles.GetOne(ctx); e != nil || !tid.IsNil() || hr != "" || rr != nil {
		t.Fatalf("GetOne expected empty on no files, got %v %s %v %v", tid, hr, rr, e)
	}

	// GetOneOfHour variants
	if tid, recs, e := tl.GetOneOfHour(ctx, ""); e != nil || !tid.IsNil() || recs != nil {
		t.Fatalf("GetOneOfHour empty hour mismatch")
	}
	if _, _, e := tl.GetOneOfHour(ctx, "bad-hour"); e == nil {
		t.Fatalf("expected parse error for bad hour")
	}

	// Commit changes logging path
	rt.LogCommitChanges = true
	tidCommit := sop.NewUUID()
	tl.tid = tidCommit
	if err := tl.PriorityLog().LogCommitChanges(ctx, nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("LogCommitChanges: %v", err)
	}
	commitFn := rt.formatActiveFolderEntity(filepath.Join(commitChangesLogFolder, tidCommit.String()+logFileExtension))
	if _, err := os.Stat(commitFn); err != nil {
		t.Fatalf("commit log missing: %v", err)
	}

	// getFilesSortedDescByModifiedTime helper coverage (dir create + filter)
	helperDir := filepath.Join(t.TempDir(), "logs-sub")
	files, err := getFilesSortedDescByModifiedTime(ctx, helperDir, ".xyz", func(de os.DirEntry) bool { return true })
	if err != nil || len(files) != 0 {
		t.Fatalf("helper initial create mismatch: %v len=%d", err, len(files))
	}
	f1 := filepath.Join(helperDir, "a"+priorityLogFileExtension)
	f2 := filepath.Join(helperDir, "b"+priorityLogFileExtension)
	if err := os.WriteFile(f1, []byte("1"), 0o644); err != nil {
		t.Fatalf("write f1: %v", err)
	}
	if err := os.WriteFile(f2, []byte("2"), 0o644); err != nil {
		t.Fatalf("write f2: %v", err)
	}
	pastH := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(f1, pastH, pastH); err != nil {
		t.Fatalf("chtimes f1: %v", err)
	}
	files, err = getFilesSortedDescByModifiedTime(ctx, helperDir, priorityLogFileExtension, func(de os.DirEntry) bool { return de.Name() != "b"+priorityLogFileExtension })
	if err != nil || len(files) != 1 {
		t.Fatalf("helper filtered mismatch: %v len=%d", err, len(files))
	}

	// fileDirectIO close error branch
	prevSim := DirectIOSim
	DirectIOSim = failingCloseDirectIO{}
	defer func() { DirectIOSim = prevSim }()
	fio := newFileDirectIO()
	name := filepath.Join(t.TempDir(), "c.dat")
	if err := fio.open(ctx, name, os.O_RDWR, 0o644); err != nil {
		t.Fatalf("directIO open: %v", err)
	}
	if err := fio.close(); err == nil || err.Error() != "close fail" {
		t.Fatalf("expected close fail error, got %v", err)
	}

	// Commit log write error path: remove commit folder after enabling logging to force internal write error.
	{
		cache := mocks.NewMockClient()
		active := t.TempDir()
		passive := t.TempDir()
		prevGlobal := GlobalReplicationDetails
		GlobalReplicationDetails = &ReplicationTrackedDetails{FailedToReplicate: true, ActiveFolderToggler: true}
		GlobalReplicationDetails.LogCommitChanges = true
		t.Cleanup(func() { GlobalReplicationDetails = prevGlobal })
		rtErr, err := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
		if err != nil {
			t.Fatalf("rtErr: %v", err)
		}
		rtErr.LogCommitChanges = true
		ms := NewManageStoreFolder(NewFileIO())
		sr, err := NewStoreRepository(ctx, rtErr, ms, cache, MinimumModValue)
		if err != nil {
			t.Fatalf("sr: %v", err)
		}
		si := sop.NewStoreInfo(sop.StoreOptions{Name: "tx1", SlotLength: 10})
		if err := sr.Add(ctx, *si); err != nil {
			t.Fatalf("Add store: %v", err)
		}
		commitDir := rtErr.formatActiveFolderEntity(commitChangesLogFolder)
		if err := os.MkdirAll(commitDir, 0o755); err != nil {
			t.Fatalf("mkdir commitDir: %v", err)
		}
		if err := os.RemoveAll(commitDir); err != nil {
			t.Fatalf("remove commitDir: %v", err)
		}
		reg := NewRegistry(true, MinimumModValue, rtErr, cache)
		h := sop.NewHandle(sop.NewUUID())
		_ = reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: si.RegistryTable, IDs: []sop.Handle{h}}})
		if _, err := os.Stat(filepath.Join(active, commitChangesLogFolder)); !os.IsNotExist(err) {
			t.Fatalf("expected commit log dir absent to reflect write error path, err=%v", err)
		}
	}
}

// Local copy of failingCloseDirectIO (legacy file stubbed out)
type failingCloseDirectIO struct{ DirectIO }

func (f failingCloseDirectIO) Close(file *os.File) error { return errors.New("close fail") }
func (f failingCloseDirectIO) Open(ctx context.Context, filename string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(filename, flag|os.O_CREATE, perm)
}
func (f failingCloseDirectIO) WriteAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
	return file.WriteAt(block, offset)
}
func (f failingCloseDirectIO) ReadAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
	return file.ReadAt(block, offset)
}
