package fs

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop/common/mocks"
)

// Covers syncWithL2Cache push branch where values are already in cache and equal (early return),
// then diverge (update performed).
func TestReplicationTrackerSyncWithL2CachePushEqualThenDiverge(t *testing.T) {
	ctx := context.Background()
	active := filepath.Join(t.TempDir(), "a")
	passive := filepath.Join(t.TempDir(), "b")
	cache := mocks.NewMockClient()

	// Initial tracker with replication ON.
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}

	// Seed global and push first time (cache empty -> set).
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: false}
	if err := rt.syncWithL2Cache(ctx, true); err != nil {
		t.Fatalf("initial push: %v", err)
	}

	// Push again with identical value: should early return (no change); we just assert no error.
	if err := rt.syncWithL2Cache(ctx, true); err != nil {
		t.Fatalf("push identical: %v", err)
	}

	// Diverge global (FailedToReplicate true) and push; should update cache without error.
	GlobalReplicationDetails.FailedToReplicate = true
	if err := rt.syncWithL2Cache(ctx, true); err != nil {
		t.Fatalf("push diverged: %v", err)
	}
}
