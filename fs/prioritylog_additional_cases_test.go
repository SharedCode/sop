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

// TestPriorityLog_RemoveAndBackup ensures Remove and RemoveBackup handle present/absent files gracefully and that Add/WriteBackup ignore errors.
func TestPriorityLog_RemoveAndBackup(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
	pl := priorityLog{replicationTracker: rt}
	tid := sop.NewUUID()

	payload := []byte("data")
	if err := pl.Add(ctx, tid, payload); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := pl.WriteBackup(ctx, tid, payload); err != nil {
		t.Fatalf("WriteBackup: %v", err)
	}
	if err := pl.Remove(ctx, tid); err != nil {
		t.Fatalf("Remove existing: %v", err)
	}
	// Removing again should be nil.
	if err := pl.Remove(ctx, tid); err != nil {
		t.Fatalf("Remove absent: %v", err)
	}
	if err := pl.RemoveBackup(ctx, tid); err != nil {
		t.Fatalf("RemoveBackup existing: %v", err)
	}
	if err := pl.RemoveBackup(ctx, tid); err != nil {
		t.Fatalf("RemoveBackup absent: %v", err)
	}
}

// TestPriorityLog_GetBatchReadyAndNotReady crafts log files with timestamps to exercise aging filter and batch sizing.
func TestPriorityLog_GetBatchReadyAndNotReady(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
	pl := priorityLog{replicationTracker: rt}

	// Create log folder and two files: one old enough, one too recent.
	logDir := rt.formatActiveFolderEntity(logFolder)
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("mkdir logDir: %v", err)
	}
	oldTid := sop.NewUUID()
	newTid := sop.NewUUID()
	oldFile := filepath.Join(logDir, oldTid.String()+priorityLogFileExtension)
	newFile := filepath.Join(logDir, newTid.String()+priorityLogFileExtension)
	if err := os.WriteFile(oldFile, []byte("[]"), 0o644); err != nil {
		t.Fatalf("write old: %v", err)
	}
	if err := os.WriteFile(newFile, []byte("[]"), 0o644); err != nil {
		t.Fatalf("write new: %v", err)
	}
	// Set mod times: old file older than min age threshold, new file current.
	// Align to past hour minus threshold to satisfy cappedHour.Compare(ft) >= 0
	nowHour, _ := time.Parse(DateHourLayout, time.Now().Format(DateHourLayout))
	past := nowHour.Add(-time.Duration((priorityLogMinAgeInMin + 10) * time.Minute))
	if err := os.Chtimes(oldFile, past, past); err != nil {
		t.Fatalf("chtimes old: %v", err)
	}
	// Leave new file with current mod time.

	batch, err := pl.GetBatch(ctx, 10)
	if err != nil {
		t.Fatalf("GetBatch: %v", err)
	}
	// Only oldTid should appear.
	if len(batch) != 1 || batch[0].Key != oldTid {
		t.Fatalf("expected 1 old item, got %d (%v)", len(batch), batch)
	}
}
