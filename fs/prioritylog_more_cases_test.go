package fs

import (
    "context"
    "os"
    "path/filepath"
    "testing"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/common/mocks"
)

// When the log folder doesn't exist, GetBatch should create it and return nil without error.
func TestPriorityLogGetBatchNoFolder(t *testing.T) {
    ctx := context.Background()
    base := filepath.Join(t.TempDir(), "a")
    // Ensure base does not yet contain the log folder.
    rt, err := NewReplicationTracker(ctx, []string{base, filepath.Join(t.TempDir(), "b")}, false, mocks.NewMockClient())
    if err != nil { t.Fatalf("rt: %v", err) }
    tl := NewTransactionLog(mocks.NewMockClient(), rt)
    plog := tl.PriorityLog()

    batch, err := plog.GetBatch(ctx, 5)
    if err != nil { t.Fatalf("GetBatch: %v", err) }
    if batch != nil {
        t.Fatalf("expected nil batch when no logs present, got %v", batch)
    }
}

// Replicate should return an error when passive write fails; use a read-only passive dir.
func TestStoreRepositoryReplicatePassiveWriteFails(t *testing.T) {
    ctx := context.Background()
    l2 := mocks.NewMockClient()
    active := t.TempDir()
    passive := t.TempDir()
    // Ensure a clean global replication state for this test to avoid interference
    // from other tests toggling the active/passive mapping.
    prev := GlobalReplicationDetails
    GlobalReplicationDetails = nil
    t.Cleanup(func(){ GlobalReplicationDetails = prev })
    // make passive store folder read-only to force write failure
    storeName := "ro1"
    if err := os.MkdirAll(filepath.Join(passive, storeName), 0o555); err != nil { t.Fatalf("mkdir passive ro: %v", err) }

    rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
    sr, err := NewStoreRepository(ctx, rt, nil, l2, MinimumModValue)
    if err != nil { t.Fatalf("NewStoreRepository: %v", err) }

    si := sop.NewStoreInfo(sop.StoreOptions{Name: storeName, SlotLength: 10})
    // Active write is fine; prepare replicate payload with non-zero count
    si.Count = 1
    if err := sr.Replicate(ctx, []sop.StoreInfo{*si}); err == nil {
        // restore perms for cleanup then fail
        _ = os.Chmod(filepath.Join(passive, storeName), 0o755)
        t.Fatalf("expected replicate error due to ro passive folder")
    }
    _ = os.Chmod(filepath.Join(passive, storeName), 0o755)
}
