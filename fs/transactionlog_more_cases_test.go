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

// These additional tests focus on low / zero covered paths in transactionlog.go and
// transactionprioritylog.go. Table-ish style kept compact; each test targets
// a distinct set of branches to minimize file proliferation.

func newTLForTest(t *testing.T) (*TransactionLog, *replicationTracker, string) {
	t.Helper()
	ctx := context.Background()
	active := filepath.Join(t.TempDir(), "active")
	passive := filepath.Join(t.TempDir(), "passive")
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("replication tracker: %v", err)
	}
	tl := NewTransactionLog(mocks.NewMockClient(), rt)
	return tl, rt, active
}

func TestTransactionLogAddRemoveAndNewUUID(t *testing.T) {
	ctx := context.Background()
	tl, rt, _ := newTLForTest(t)

	tid := tl.NewUUID()
	if tid.IsNil() {
		t.Fatalf("expected non-nil uuid")
	}

	// Add multiple entries (exercise reuse of same file handle + encoder path)
	payloads := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	for i, p := range payloads {
		if err := tl.Add(ctx, tid, i, p); err != nil {
			t.Fatalf("Add #%d: %v", i, err)
		}
	}

	// Remove should close & delete
	if err := tl.Remove(ctx, tid); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	// Removing again should surface underlying os.Remove error only if file recreated; just call to touch line.
	_ = tl.Remove(ctx, tid) // ignore error

	// Create another (aged) log file to aid other tests indirectly.
	tid2 := sop.NewUUID()
	if err := tl.Add(ctx, tid2, 1, []byte("z")); err != nil {
		t.Fatalf("Add second: %v", err)
	}
	fn2 := rt.formatActiveFolderEntity(filepath.Join(logFolder, tid2.String()+logFileExtension))
	past := time.Now().Add(-time.Duration(int(ageLimit+2)) * time.Minute)
	if err := os.Chtimes(fn2, past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
}

func TestTransactionLogGetOneEligibleAndEmptyCases(t *testing.T) {
	ctx := context.Background()
	tl, rt, _ := newTLForTest(t)

	// Case 1: No files -> GetOne returns NilUUID
	if tid, hour, recs, err := tl.GetOne(ctx); err != nil || !tid.IsNil() || hour != "" || recs != nil {
		t.Fatalf("expected nil result when no eligible logs: tid=%v hour=%s recs=%v err=%v", tid, hour, recs, err)
	}

	// Create two log files; only age one enough to be eligible.
	eligible := sop.NewUUID()
	ineligible := sop.NewUUID()
	if err := tl.Add(ctx, eligible, 1, []byte("e1")); err != nil {
		t.Fatalf("Add elig: %v", err)
	}
	if err := tl.Add(ctx, ineligible, 1, []byte("i1")); err != nil {
		t.Fatalf("Add inelig: %v", err)
	}

	// Age only the eligible one sufficiently old to pass cappedHour comparison.
	fnElig := rt.formatActiveFolderEntity(filepath.Join(logFolder, eligible.String()+logFileExtension))
	agedTime := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(fnElig, agedTime, agedTime); err != nil {
		t.Fatalf("chtimes elig: %v", err)
	}

	// Invoke GetOne – should return eligible tid and records.
	tid, hour, recs, err := tl.GetOne(ctx)
	if err != nil {
		t.Fatalf("GetOne: %v", err)
	}
	if tid.IsNil() || len(recs) == 0 || hour == "" {
		t.Fatalf("expected populated result: tid=%v hour=%s recs=%v", tid, hour, recs)
	}
}

func TestTransactionLogGetOneOfHourVariants(t *testing.T) {
	ctx := context.Background()
	tl, _, _ := newTLForTest(t)

	// Empty hour -> fast path
	if tid, recs, err := tl.GetOneOfHour(ctx, ""); err != nil || !tid.IsNil() || recs != nil {
		t.Fatalf("empty hour case mismatch")
	}

	// Invalid hour format -> error
	if _, _, err := tl.GetOneOfHour(ctx, "bad-hour"); err == nil {
		t.Fatalf("expected parse error for bad hour")
	}
}

func TestPriorityLogLifecycleAndBatchDefaults(t *testing.T) {
	ctx := context.Background()
	tl, rt, _ := newTLForTest(t)
	pl := tl.PriorityLog()
	if !pl.IsEnabled() {
		t.Fatalf("IsEnabled expected true")
	}

	// Add three payloads and age them beyond priorityLogMinAgeInMin to be eligible.
	tids := []sop.UUID{sop.NewUUID(), sop.NewUUID(), sop.NewUUID()}
	for i, id := range tids {
		// simple JSON array for payload (encoding validated elsewhere)
		payload := []byte(`[{}]`)
		if err := pl.Add(ctx, id, payload); err != nil {
			t.Fatalf("Add prio %d: %v", i, err)
		}
		fn := rt.formatActiveFolderEntity(filepath.Join(logFolder, id.String()+priorityLogFileExtension))
		// Need age beyond the hour-based cap to make them eligible; use 2 hours.
		past := time.Now().Add(-2 * time.Hour)
		if err := os.Chtimes(fn, past, past); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
	}

	// batchSize <=0 triggers default 25 path.
	batch, err := pl.GetBatch(ctx, 0)
	if err != nil {
		t.Fatalf("GetBatch: %v", err)
	}
	if len(batch) != len(tids) {
		t.Fatalf("expected %d batch items, got %d", len(tids), len(batch))
	}

	// Exercise Get (first id) & Remove (second) & WriteBackup/RemoveBackup (third)
	if _, err := pl.Get(ctx, tids[0]); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if err := pl.Remove(ctx, tids[1]); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := pl.WriteBackup(ctx, tids[2], []byte("x")); err != nil {
		t.Fatalf("WriteBackup: %v", err)
	}
	if err := pl.RemoveBackup(ctx, tids[2]); err != nil {
		t.Fatalf("RemoveBackup: %v", err)
	}
}

func TestGetFilesSortedDescByModifiedTimeHelper(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "logs-sub")
	// Directory does not exist yet -> helper should create it and return empty slice.
	files, err := getFilesSortedDescByModifiedTime(ctx, dir, ".xyz", func(de os.DirEntry) bool { return true })
	if err != nil {
		t.Fatalf("expected no error creating directory: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected 0 files, got %d", len(files))
	}

	// Create two fake files with suffix and different mod times to test ordering & filter skip.
	f1 := filepath.Join(dir, "a"+priorityLogFileExtension)
	f2 := filepath.Join(dir, "b"+priorityLogFileExtension)
	if err := os.WriteFile(f1, []byte("1"), 0o644); err != nil {
		t.Fatalf("write f1: %v", err)
	}
	if err := os.WriteFile(f2, []byte("2"), 0o644); err != nil {
		t.Fatalf("write f2: %v", err)
	}
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(f1, past, past); err != nil {
		t.Fatalf("chtimes f1: %v", err)
	}

	// Filter to only include .plg files (priority log suffix) and exclude one by name.
	files, err = getFilesSortedDescByModifiedTime(ctx, dir, priorityLogFileExtension, func(de os.DirEntry) bool { return de.Name() != "b"+priorityLogFileExtension })
	if err != nil {
		t.Fatalf("helper err: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 filtered file, got %d", len(files))
	}
}
