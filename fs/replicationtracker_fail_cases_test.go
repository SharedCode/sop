package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop/common/mocks"
)

// TestReplicationTracker_HandleFailedToReplicate_RemoteAlreadyFailed exercises branch where global marks failure after L2 pull.
func TestReplicationTracker_HandleFailedToReplicate_RemoteAlreadyFailed(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := filepath.Join(t.TempDir(), "a")
	os.MkdirAll(active, 0o755)
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: true}
	rt, _ := NewReplicationTracker(ctx, []string{active}, true, cache)
	// Local not failed yet; global is failed -> early return sets local flag true.
	rt.handleFailedToReplicate(ctx)
	if !rt.FailedToReplicate {
		t.Fatalf("expected local failure copied from global")
	}
}

// (removed mockCacheErr: no longer needed for current coverage focus)

// TestReplicationTracker_Failover_GuardBranches covers failover guard early returns where global already flipped.
func TestReplicationTracker_Failover_GuardBranches(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := filepath.Join(t.TempDir(), "a")
	passive := filepath.Join(t.TempDir(), "b")
	os.MkdirAll(active, 0o755)
	os.MkdirAll(passive, 0o755)
	// Global already flipped relative to local.
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: false, FailedToReplicate: false}
	rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	rt.ActiveFolderToggler = true
	if err := rt.failover(ctx); err != nil {
		t.Fatalf("guard failover returned error: %v", err)
	}
	// Now simulate global changes mid-call: align toggler then set after sync.
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: false}
	rt2, _ := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	// Flip global to opposite just before calling to trigger second guard.
	GlobalReplicationDetails.ActiveFolderToggler = !rt2.ActiveFolderToggler
	if err := rt2.failover(ctx); err != nil {
		t.Fatalf("post-sync guard failover error: %v", err)
	}
}
