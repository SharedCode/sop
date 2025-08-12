package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop/common/mocks"
)

// TestReplicationTracker_HandleFailedToReplicateWriteStatusFail converts status path to directory
// to force writeReplicationStatus failure path inside handleFailedToReplicate, ensuring it logs and continues.
func TestReplicationTracker_HandleFailedToReplicateWriteStatusFail(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	active := t.TempDir()
	passive := t.TempDir()

	GlobalReplicationDetails = nil
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}

	// Ensure healthy start.
	if rt.FailedToReplicate {
		t.Fatalf("expected healthy start")
	}

	// Pre-create a directory at the active status file path so writeReplicationStatus fails (cannot overwrite dir with file).
	statusPath := rt.formatActiveFolderEntity(replicationStatusFilename)
	if err := os.MkdirAll(statusPath, 0o755); err != nil {
		t.Fatalf("mkdir status dir: %v", err)
	}

	// Trigger failure -> should attempt writeReplicationStatus (fail) yet set flags.
	rt.handleFailedToReplicate(ctx)
	if !rt.FailedToReplicate {
		t.Fatalf("expected FailedToReplicate true even after write failure")
	}

	// Confirm still a directory.
	fi, err := os.Stat(statusPath)
	if err != nil || !fi.IsDir() {
		t.Fatalf("expected status path directory remains; err=%v", err)
	}

	// Clean up to avoid side effects for subsequent tests.
	os.RemoveAll(filepath.Join(active, replicationStatusFilename))
}
