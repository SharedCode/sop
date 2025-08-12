package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop/common/mocks"
)

// TestReplicationTracker_ReadStatus_PassiveOnly covers branch where active missing, passive present and readable -> flip toggler.
func TestReplicationTracker_ReadStatus_PassiveOnly(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := filepath.Join(t.TempDir(), "a")
	passive := filepath.Join(t.TempDir(), "b")
	if err := os.MkdirAll(active, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(passive, 0o755); err != nil {
		t.Fatal(err)
	}
	// Write status file only to passive.
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: false, FailedToReplicate: true}
	payloadPassive := *GlobalReplicationDetails
	rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	// Ensure active toggler starts true (active points to index 0 path) so we can observe flip.
	rt.ActiveFolderToggler = true
	fnPassive := filepath.Join(passive, replicationStatusFilename)
	if err := rt.writeReplicationStatus(ctx, fnPassive); err != nil {
		t.Fatalf("write passive: %v", err)
	}
	// Remove any active file to force branch.
	_ = os.Remove(filepath.Join(active, replicationStatusFilename))
	if err := rt.readStatusFromHomeFolder(ctx); err != nil {
		t.Fatalf("readStatusFromHomeFolder: %v", err)
	}
	if rt.ActiveFolderToggler != false {
		t.Fatalf("expected toggler flipped to false")
	}
	// Loaded details should match payloadPassive.
	if rt.FailedToReplicate != payloadPassive.FailedToReplicate {
		t.Fatalf("expected failed replicate true")
	}
}

// TestReplicationTracker_ReadStatus_PassiveNewer covers branch comparing timestamps and flipping when passive newer.
func TestReplicationTracker_ReadStatus_PassiveNewer(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := filepath.Join(t.TempDir(), "a")
	passive := filepath.Join(t.TempDir(), "b")
	os.MkdirAll(active, 0o755)
	os.MkdirAll(passive, 0o755)
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: false}
	rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	// Write older file to passive first, then newer to active so passive is older and should NOT flip.
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: false, FailedToReplicate: true}
	_ = rt.writeReplicationStatus(ctx, filepath.Join(passive, replicationStatusFilename))
	time.Sleep(10 * time.Millisecond)
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: false}
	_ = rt.writeReplicationStatus(ctx, filepath.Join(active, replicationStatusFilename))
	if err := rt.readStatusFromHomeFolder(ctx); err != nil {
		t.Fatalf("read: %v", err)
	}
	// Active newer => stay true
	if rt.ActiveFolderToggler != true {
		t.Fatalf("expected stay on active (true)")
	}
}

// TestReplicationTracker_ReadStatus_StatErrorActiveThenPassiveExists forces stat error on active (remove after first file exists check)
// so fallback stat(passive) path triggers toggler flip.
func TestReplicationTracker_ReadStatus_StatErrorActiveThenPassiveExists(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := filepath.Join(t.TempDir(), "a")
	passive := filepath.Join(t.TempDir(), "b")
	os.MkdirAll(active, 0o755)
	os.MkdirAll(passive, 0o755)
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: false}
	rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	// Write both files, then delete active to cause first os.Stat to error.
	_ = rt.writeReplicationStatus(ctx, filepath.Join(active, replicationStatusFilename))
	_ = rt.writeReplicationStatus(ctx, filepath.Join(passive, replicationStatusFilename))
	if err := os.Remove(filepath.Join(active, replicationStatusFilename)); err != nil {
		t.Fatalf("remove active: %v", err)
	}
	if err := rt.readStatusFromHomeFolder(ctx); err != nil {
		t.Fatalf("read: %v", err)
	}
	if rt.ActiveFolderToggler != false {
		t.Fatalf("expected flip due to missing active after stat error")
	}
}

// TestReplicationTracker_ReadStatus_NoFiles ensures early return when neither file exists.
func TestReplicationTracker_ReadStatus_NoFiles(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := filepath.Join(t.TempDir(), "a")
	passive := filepath.Join(t.TempDir(), "b")
	os.MkdirAll(active, 0o755)
	os.MkdirAll(passive, 0o755)
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true}
	rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	if err := rt.readStatusFromHomeFolder(ctx); err != nil {
		t.Fatalf("read: %v", err)
	}
	if rt.ActiveFolderToggler != true {
		t.Fatalf("toggler should remain true")
	}
}
