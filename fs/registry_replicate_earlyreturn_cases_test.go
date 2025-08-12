package fs

import (
	"context"
	"os"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestRegistryReplicateEarlyReturn covers early return conditions:
// 1) replication disabled
// 2) FailedToReplicate already true
// Ensures no passive side artifacts are created and no error returned.
func TestRegistryReplicateEarlyReturn(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()

	active := t.TempDir()
	passive := t.TempDir()

	// Case 1: replicate disabled
	GlobalReplicationDetails = nil
	rtNoRep, err := NewReplicationTracker(ctx, []string{active, passive}, false, cache)
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}

	reg1 := NewRegistry(true, MinimumModValue, rtNoRep, cache)
	h := sop.NewHandle(sop.NewUUID())
	if err := reg1.Replicate(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "t1", IDs: []sop.Handle{h}}}, nil, nil, nil); err != nil {
		t.Fatalf("Replicate (replicate disabled) returned error: %v", err)
	}
	// Passive should remain empty (no registry table folder created)
	if entries, _ := os.ReadDir(passive); len(entries) != 0 {
		t.Fatalf("expected passive empty for replicate disabled, got %d entries", len(entries))
	}

	// Case 2: FailedToReplicate true (skip replication)
	active2 := t.TempDir()
	passive2 := t.TempDir()
	GlobalReplicationDetails = &ReplicationTrackedDetails{FailedToReplicate: true, ActiveFolderToggler: true}
	rtFail, err := NewReplicationTracker(ctx, []string{active2, passive2}, true, cache)
	if err != nil {
		t.Fatalf("tracker2: %v", err)
	}
	rtFail.FailedToReplicate = true

	reg2 := NewRegistry(true, MinimumModValue, rtFail, cache)
	h2 := sop.NewHandle(sop.NewUUID())
	if err := reg2.Replicate(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "t2", IDs: []sop.Handle{h2}}}, nil, nil, nil); err != nil {
		t.Fatalf("Replicate (FailedToReplicate) returned error: %v", err)
	}
	if entries, _ := os.ReadDir(passive2); len(entries) != 0 {
		t.Fatalf("expected passive2 empty for FailedToReplicate, got %d entries", len(entries))
	}
}
