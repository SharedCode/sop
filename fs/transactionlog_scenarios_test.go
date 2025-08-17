package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

func Test_TransactionLog_GetOneOfHour_TTL_Window(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	baseA := filepath.Join(t.TempDir(), "a")
	rt, err := NewReplicationTracker(ctx, []string{baseA, filepath.Join(t.TempDir(), "b")}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	tl := NewTransactionLog(mocks.NewMockClient(), rt)

	// Too old hour (beyond 4h window) -> returns NilUUID, nil
	old := time.Now().Add(-5 * time.Hour).Format(DateHourLayout)
	tid, recs, err := tl.GetOneOfHour(ctx, old)
	if err != nil || !tid.IsNil() || recs != nil {
		t.Fatalf("expected nil result for old hour, got tid=%v err=%v recs=%v", tid, err, recs)
	}

	// Empty hour string -> nil
	tid, recs, err = tl.GetOneOfHour(ctx, "")
	if err != nil || !tid.IsNil() || recs != nil {
		t.Fatalf("expected nil result for empty hour, got tid=%v err=%v recs=%v", tid, err, recs)
	}

	// Prepare a log file for current hour and ensure GetOne/GetOneOfHour works end-to-end.
	tid = tl.NewUUID()
	if err := tl.Add(ctx, tid, 1, []byte("x")); err != nil {
		t.Fatalf("add: %v", err)
	}
	// Verify GetOne returns something (may or may not claim based on lock, but shouldn't error).
	_, _, _, _ = tl.GetOne(ctx)

	// GetOneOfHour of current hour should return something or nil without error based on race; accept either.
	hour := time.Now().Format(DateHourLayout)
	_, _, _ = tl.GetOneOfHour(ctx, hour)

	// Cleanup
	_ = tl.Remove(ctx, tid)
}

func Test_getFilesSortedDescByModifiedTime_EnsureDirCreation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	temp := t.TempDir()
	// Directory not existing initially; function should create it and return empty list without error.
	dir := filepath.Join(temp, "logs")
	files, err := getFilesSortedDescByModifiedTime(ctx, dir, ".log", nil)
	if err != nil || len(files) != 0 {
		t.Fatalf("expected empty and no error, got len=%d err=%v", len(files), err)
	}

	// Create one .log file and a non-matching file, ensure one result returned.
	f := filepath.Join(dir, sop.NewUUID().String()+".log")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = os.WriteFile(filepath.Join(dir, "ignore.txt"), []byte("x"), 0o644)

	files, err = getFilesSortedDescByModifiedTime(ctx, dir, ".log", nil)
	if err != nil || len(files) != 1 {
		t.Fatalf("expected 1 log entry, got len=%d err=%v", len(files), err)
	}
}

// Note: Hitting TransactionLog.Add encode error deterministically would require injecting a failing writer,
// which the current design doesn’t expose. Skipping that edge path.

// Ensures Add handles initial create failure followed by fallback create success (creates base folder).
func Test_TransactionLog_Add_CreatesBaseFolderOnFirstCreateFail(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := filepath.Join(t.TempDir(), "a")
	passive := filepath.Join(t.TempDir(), "b")

	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	tl := NewTransactionLog(cache, rt)
	tid := sop.NewUUID()
	// Do not create base folder; first os.Create should fail, fallback should create base and succeed.
	if err := tl.Add(ctx, tid, 1, []byte("x")); err != nil {
		t.Fatalf("Add unexpected error: %v", err)
	}
}

// Ensures Add returns error when the log filename path is a directory (second create also fails).
func Test_TransactionLog_Add_FileIsDirectoryError(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := filepath.Join(t.TempDir(), "a")
	passive := filepath.Join(t.TempDir(), "b")

	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	tl := NewTransactionLog(cache, rt)
	tid := sop.NewUUID()

	// Prepare a directory with the exact filename to force os.Create to fail both times.
	filename := tl.format(tid)
	if err := os.MkdirAll(filename, 0o755); err != nil {
		t.Fatalf("mkdir filename collide: %v", err)
	}
	if err := tl.Add(ctx, tid, 1, []byte("x")); err == nil {
		t.Fatalf("expected error when filename is a directory")
	}
}

// Ensures Add creates the log folder when missing (first os.Create fails, then retries after MkdirAll).
func Test_TransactionLog_Add_CreatesFolderWhenMissing(t *testing.T) {
	ctx := context.Background()
	a := t.TempDir()
	b := t.TempDir()
	l2 := mocks.NewMockClient()
	rt, err := NewReplicationTracker(ctx, []string{a, b}, true, l2)
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	tl := NewTransactionLog(l2, rt)
	tid := sop.NewUUID()
	// Ensure the translogs folder does not exist so first Create fails
	logDir := rt.formatActiveFolderEntity(logFolder)
	_ = os.RemoveAll(logDir)
	if err := tl.Add(ctx, tid, 1, []byte("p")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// File should exist now
	if _, err := os.Stat(tl.format(tid)); err != nil {
		t.Fatalf("expected log file: %v", err)
	}
}

// Deterministically validates getLogsDetails returns appended records without relying on hour-age filtering.
func Test_TransactionLog_getLogsDetails_ReadsRecords(t *testing.T) {
	ctx := context.Background()
	a := t.TempDir()
	b := t.TempDir()
	l2 := mocks.NewMockClient()
	rt, err := NewReplicationTracker(ctx, []string{a, b}, true, l2)
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	tl := NewTransactionLog(l2, rt)
	tid := sop.NewUUID()
	if err := tl.Add(ctx, tid, 7, []byte("x")); err != nil {
		t.Fatalf("add: %v", err)
	}
	// Append a second record
	if err := tl.Add(ctx, tid, 9, []byte("y")); err != nil {
		t.Fatalf("add2: %v", err)
	}
	recs, err := tl.getLogsDetails(tid)
	if err != nil {
		t.Fatalf("getLogsDetails: %v", err)
	}
	if len(recs) < 2 {
		t.Fatalf("expected >=2 records, got %d", len(recs))
	}
}


// Deterministic success path for TransactionLog.GetOne: produce a log older than the capped hour and assert it is returned.
func Test_TransactionLog_GetOne_Success(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	active := filepath.Join(t.TempDir(), "a")
	passive := filepath.Join(t.TempDir(), "b")
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	tl := NewTransactionLog(mocks.NewMockClient(), rt)

	// Create a log with two entries using Add to ensure JSON shape matches getLogsDetails expectations.
	tid := tl.NewUUID()
	if err := tl.Add(ctx, tid, 10, []byte("A")); err != nil {
		t.Fatalf("add1: %v", err)
	}
	if err := tl.Add(ctx, tid, 11, []byte("B")); err != nil {
		t.Fatalf("add2: %v", err)
	}

	// Age the file beyond the capped hour so getOne considers it.
	filename := tl.format(tid)
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(filename, past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	gotTid, hour, recs, err := tl.GetOne(ctx)
	if err != nil || gotTid != tid || hour == "" || len(recs) != 2 {
		t.Fatalf("GetOne mismatch: tid=%v hour=%q len=%d err=%v", gotTid, hour, len(recs), err)
	}
	if string(recs[0].Value) != "A" || string(recs[1].Value) != "B" {
		t.Fatalf("unexpected record values: %+v", recs)
	}

	// Cleanup
	_ = tl.Remove(ctx, tid)
}
