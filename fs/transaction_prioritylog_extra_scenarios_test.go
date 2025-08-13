package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
	"github.com/sharedcode/sop/encoding"
)

// Additional focused scenarios to lift coverage on lesser-hit branches.
func TestTransactionLog_GetOneOfHour_TooOldUnlock(t *testing.T) {
	ctx := context.Background()
	rt, err := NewReplicationTracker(ctx, []string{t.TempDir(), t.TempDir()}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	tl := NewTransactionLog(mocks.NewMockClient(), rt)

	oldHour := time.Now().Add(-5 * time.Hour).Format(DateHourLayout)
	tid, recs, err := tl.GetOneOfHour(ctx, oldHour)
	if err != nil {
		t.Fatalf("GetOneOfHour err: %v", err)
	}
	if !tid.IsNil() || recs != nil {
		t.Fatalf("expected nil result for too-old hour, got %v %v", tid, recs)
	}
}

func TestTransactionLog_getLogsDetails_UnmarshalSkip(t *testing.T) {
	ctx := context.Background()
	rt, err := NewReplicationTracker(ctx, []string{t.TempDir(), t.TempDir()}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	tl := NewTransactionLog(mocks.NewMockClient(), rt)

	tid := sop.NewUUID()
	filename := tl.format(tid)
	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	validKV := sop.KeyValuePair[int, []byte]{Key: 1, Value: []byte("x")}
	vb, _ := encoding.DefaultMarshaler.Marshal(validKV)
	content := []byte("not-json\n" + string(vb) + "\n")
	if err := os.WriteFile(filename, content, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	recs, err := tl.getLogsDetails(tid)
	if err != nil {
		t.Fatalf("getLogsDetails err: %v", err)
	}
	if len(recs) != 1 || recs[0].Key != 1 {
		t.Fatalf("expected 1 valid record, got %+v", recs)
	}
}

func TestPriorityLog_GetBatch_WithCorruptFileError(t *testing.T) {
	ctx := context.Background()
	rt, err := NewReplicationTracker(ctx, []string{t.TempDir(), t.TempDir()}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	pl := NewTransactionLog(mocks.NewMockClient(), rt).PriorityLog()

	dir := rt.formatActiveFolderEntity(logFolder)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// valid eligible file
	tidValid := sop.NewUUID()
	vf := filepath.Join(dir, tidValid.String()+priorityLogFileExtension)
	if err := os.WriteFile(vf, []byte("[]"), 0o644); err != nil {
		t.Fatalf("write valid: %v", err)
	}
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(vf, past, past); err != nil {
		t.Fatalf("chtimes valid: %v", err)
	}

	// corrupt eligible file triggers Get error -> batch returns partial + error
	tidBad := sop.NewUUID()
	bf := filepath.Join(dir, tidBad.String()+priorityLogFileExtension)
	if err := os.WriteFile(bf, []byte("not-json"), 0o644); err != nil {
		t.Fatalf("write bad: %v", err)
	}
	if err := os.Chtimes(bf, past.Add(1*time.Minute), past.Add(1*time.Minute)); err != nil {
		t.Fatalf("chtimes bad: %v", err)
	}

	batch, err := pl.GetBatch(ctx, 10)
	if err == nil {
		t.Fatalf("expected error due to corrupt file, got nil")
	}
	if len(batch) == 0 {
		t.Fatalf("expected at least one valid batch entry, got 0")
	}
}

// Focused micro-tests to lift remaining uncovered lines in transaction & priority log code.
func TestPriorityLog_BasicGetRemovePaths(t *testing.T) {
	ctx := context.Background()
	rt, err := NewReplicationTracker(ctx, []string{t.TempDir(), t.TempDir()}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	tl := NewTransactionLog(mocks.NewMockClient(), rt)
	pl := tl.PriorityLog()

	if !pl.IsEnabled() {
		t.Fatalf("expected enabled")
	}

	tid := sop.NewUUID()
	if recs, err := pl.Get(ctx, tid); err != nil || recs != nil {
		t.Fatalf("expected nil,nil get absent, got %v %v", recs, err)
	}
	if err := pl.Remove(ctx, tid); err != nil {
		t.Fatalf("remove absent: %v", err)
	}

	payload := []byte(`[{}]`)
	if err := pl.WriteBackup(ctx, tid, payload); err != nil {
		t.Fatalf("write backup: %v", err)
	}
	backupFile := rt.formatActiveFolderEntity(filepath.Join(logFolder, tid.String()+priorityLogBackupFileExtension))
	if _, err := os.Stat(backupFile); err != nil {
		t.Fatalf("expected backup file: %v", err)
	}
	if err := pl.RemoveBackup(ctx, tid); err != nil {
		t.Fatalf("remove backup: %v", err)
	}
}

func TestTransactionLog_RemoveClosesFile(t *testing.T) {
	ctx := context.Background()
	rt, err := NewReplicationTracker(ctx, []string{t.TempDir(), t.TempDir()}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	tl := NewTransactionLog(mocks.NewMockClient(), rt)
	tid := sop.NewUUID()
	if err := tl.Add(ctx, tid, 1, []byte("x")); err != nil {
		t.Fatalf("add: %v", err)
	}
	if tl.file == nil {
		t.Fatalf("expected file opened")
	}
	if err := tl.Remove(ctx, tid); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := os.Stat(tl.format(tid)); !os.IsNotExist(err) {
		t.Fatalf("expected file removed, stat err=%v", err)
	}
}

func TestTransactionLog_getOne_IgnoresInvalidFiles(t *testing.T) {
	ctx := context.Background()
	rt, err := NewReplicationTracker(ctx, []string{t.TempDir(), t.TempDir()}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	tl := NewTransactionLog(mocks.NewMockClient(), rt)
	base := rt.formatActiveFolderEntity(logFolder)
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	past := time.Now().Add(-2 * time.Hour)
	// invalid file (non-uuid prefix) should be ignored
	invalid := filepath.Join(base, "not-a-uuid"+logFileExtension)
	os.WriteFile(invalid, []byte("{}"), 0o644)
	os.Chtimes(invalid, past, past)
	// valid file
	tid := sop.NewUUID()
	valid := filepath.Join(base, tid.String()+logFileExtension)
	os.WriteFile(valid, []byte("{}"), 0o644)
	os.Chtimes(valid, past.Add(-1*time.Minute), past.Add(-1*time.Minute)) // slightly older so processed first

	hour, gotTid, err := tl.getOne(ctx)
	if err != nil {
		t.Fatalf("getOne err: %v", err)
	}
	if gotTid != tid {
		t.Fatalf("expected tid %s got %s", tid, gotTid)
	}
	if hour == "" {
		t.Fatalf("expected hour string")
	}
}
