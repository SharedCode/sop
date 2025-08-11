package fs

import (
    "context"
    "testing"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/common/mocks"
)

// Covers startLoggingCommitChanges path (write status + push to L2)
func TestReplicationTrackerStartLoggingCommitChanges(t *testing.T) {
    ctx := context.Background()
    l2 := mocks.NewMockClient()
    base := t.TempDir()
    rt, err := NewReplicationTracker(ctx, []string{base}, true, l2)
    if err != nil { t.Fatalf("rt: %v", err) }

    if err := rt.startLoggingCommitChanges(ctx); err != nil {
        t.Fatalf("startLoggingCommitChanges: %v", err)
    }
    if !rt.LogCommitChanges {
        t.Fatalf("expected LogCommitChanges true")
    }
}

// Covers readStatusFromHomeFolder alternative branches: missing active but present passive.
func TestReplicationTrackerReadStatusFromHomeFolderBranches(t *testing.T) {
    ctx := context.Background()
    l2 := mocks.NewMockClient()
    active := t.TempDir()
    passive := t.TempDir()
    // Pre-write status to passive only
    rtInit, _ := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
    rtInit.ActiveFolderToggler = false // make passive active for write
    if err := rtInit.writeReplicationStatus(ctx, rtInit.formatActiveFolderEntity(replicationStatusFilename)); err != nil {
        t.Fatalf("seed writeReplicationStatus: %v", err)
    }

    // Now create a new tracker which should flip active toggler after reading from passive
    rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
    if err != nil { t.Fatalf("rt: %v", err) }
    // The readStatusFromHomeFolder is called inside NewReplicationTracker; ensure it didn't error.
    _ = rt
}

// Covers SetTransactionID trivial setter.
func TestReplicationTrackerSetTransactionID(t *testing.T) {
    ctx := context.Background()
    l2 := mocks.NewMockClient()
    base := t.TempDir()
    rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
    id := sop.NewUUID()
    rt.SetTransactionID(id)
}
