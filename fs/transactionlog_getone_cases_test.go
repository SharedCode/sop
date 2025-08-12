package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
)

// TestTransactionLog_GetOneOfHour_TooOldHour exercises the guard where requested hour is older than TTL window (>4h) and returns nils.
func TestTransactionLog_GetOneOfHour_TooOldHour(t *testing.T) {
	ctx := context.Background()
	tl, _, _ := newTLForTest(t)
	oldHour := time.Now().Add(-5 * time.Hour).Format(DateHourLayout)
	tid, recs, err := tl.GetOneOfHour(ctx, oldHour)
	if err != nil || !tid.IsNil() || recs != nil {
		t.Fatalf("expected nils for old hour, got tid=%v recs=%v err=%v", tid, recs, err)
	}
}

// TestTransactionLog_GetOneOfHour_NoFilesCurrentHour requests current hour with no files -> nil results (unlock path).
func TestTransactionLog_GetOneOfHour_NoFilesCurrentHour(t *testing.T) {
	ctx := context.Background()
	tl, _, _ := newTLForTest(t)
	currHour := time.Now().Format(DateHourLayout)
	tid, recs, err := tl.GetOneOfHour(ctx, currHour)
	if err != nil || !tid.IsNil() || recs != nil {
		t.Fatalf("expected nils when no files present, got tid=%v recs=%v err=%v", tid, recs, err)
	}
}

// TestTransactionLog_GetOne_CorruptLinesSkipped creates an aged log file with corrupt JSON lines; unmarshal errors are skipped not fatal.
func TestTransactionLog_GetOne_CorruptLinesSkipped(t *testing.T) {
	ctx := context.Background()
	tl, rt, _ := newTLForTest(t)
	tid := sop.NewUUID()
	if err := tl.Add(ctx, tid, 1, []byte("good")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// Overwrite file with corrupt content.
	fn := rt.formatActiveFolderEntity(filepath.Join(logFolder, tid.String()+logFileExtension))
	if err := os.WriteFile(fn, []byte("not-json\nstill-bad"), 0o644); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	// Age file so it's eligible.
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(fn, past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	gotTid, hour, recs, err := tl.GetOne(ctx)
	if err != nil {
		t.Fatalf("GetOne err: %v", err)
	}
	if gotTid != tid || hour == "" {
		t.Fatalf("expected tid %v with hour, got tid=%v hour=%s", tid, gotTid, hour)
	}
	if len(recs) != 0 {
		t.Fatalf("expected 0 decoded recs due to corruption, got %d", len(recs))
	}
}
