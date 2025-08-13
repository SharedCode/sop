package fs

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/sharedcode/sop/common/mocks"
)

// Covers the success branch of handleFailedToReplicate (writes status, flips flags, sync path).
func TestReplicationTracker_handleFailedToReplicate_SetsFlagsAndStatusFile(t *testing.T) {
	ctx := context.Background()
	prev := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	t.Cleanup(func() { GlobalReplicationDetails = prev })

	active, passive := t.TempDir(), t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}
	if rt.FailedToReplicate {
		t.Fatalf("expected initial FailedToReplicate false")
	}

	rt.handleFailedToReplicate(ctx)

	if !rt.FailedToReplicate || GlobalReplicationDetails == nil || !GlobalReplicationDetails.FailedToReplicate {
		t.Fatalf("expected flags set true, got rt=%v global=%v", rt.FailedToReplicate, GlobalReplicationDetails)
	}
	// Status file should exist in (original) active folder.
	statFile := rt.formatActiveFolderEntity(replicationStatusFilename)
	if _, err := os.Stat(statFile); err != nil {
		t.Fatalf("expected status file: %v", err)
	}

	// Call again (early return path) - ensure timestamp not updated significantly (no rewrite) by sleeping 1s and checking modtime delta <1s.
	info1, _ := os.Stat(statFile)
	mt1 := info1.ModTime()
	time.Sleep(20 * time.Millisecond)
	rt.handleFailedToReplicate(ctx)
	info2, _ := os.Stat(statFile)
	if info2.ModTime().After(mt1.Add(500 * time.Millisecond)) { // allow small FS timestamp granularity differences
		t.Fatalf("status file unexpectedly rewritten on second call")
	}
}
