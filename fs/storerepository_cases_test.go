package fs

import (
    "context"
    "testing"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/common/mocks"
)

// Basic StoreRepository happy paths: Add/Get/GetAll/Update/Remove and Replicate no-op when disabled.
func TestStoreRepositoryBasicFlow(t *testing.T) {
    ctx := context.Background()
    l2 := mocks.NewMockClient()
    base := t.TempDir()
    rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)

    sr, err := NewStoreRepository(ctx, rt, nil, l2, MinimumModValue)
    if err != nil { t.Fatalf("NewStoreRepository: %v", err) }

    // Add stores
    si := sop.NewStoreInfo(sop.StoreOptions{Name: "s1", SlotLength: 10})
    sj := sop.NewStoreInfo(sop.StoreOptions{Name: "s2", SlotLength: 8})
    if err := sr.Add(ctx, *si, *sj); err != nil { t.Fatalf("Add: %v", err) }

    // GetAll should return two names
    names, err := sr.GetAll(ctx)
    if err != nil || len(names) != 2 { t.Fatalf("GetAll: %v, %v", names, err) }

    // Get should load from disk+cache
    got, err := sr.Get(ctx, "s1", "s2")
    if err != nil || len(got) != 2 { t.Fatalf("Get: %v, %v", got, err) }

    // Update: change CountDelta and write back
    si2 := got[0]
    si2.CountDelta = 5
    if _, err := sr.Update(ctx, []sop.StoreInfo{si2}); err != nil { t.Fatalf("Update: %v", err) }

    // Remove one
    if err := sr.Remove(ctx, "s1"); err != nil { t.Fatalf("Remove: %v", err) }

    // Replicate disabled should be no-op
    if err := sr.Replicate(ctx, []sop.StoreInfo{si2}); err != nil { t.Fatalf("Replicate disabled: %v", err) }
}

// Cover GetRegistryHashModValue path reading from file when not preset.
func TestStoreRepositoryGetRegistryHashModValueRead(t *testing.T) {
    ctx := context.Background()
    l2 := mocks.NewMockClient()
    active := t.TempDir()
    passive := t.TempDir()
    rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, l2)

    // Initialize with a value; constructor should write it once.
    sr, err := NewStoreRepository(ctx, rt, nil, l2, 123)
    if err != nil { t.Fatalf("NewStoreRepository: %v", err) }

    // New SR with zero forces read via fileIOWithReplication
    sr2, err := NewStoreRepository(ctx, rt, nil, l2, 0)
    if err != nil { t.Fatalf("NewStoreRepository2: %v", err) }

    v, err := sr2.GetRegistryHashModValue(ctx)
    if err != nil || v == 0 { t.Fatalf("GetRegistryHashModValue: %d, %v", v, err) }

    // Silence unused var
    _ = sr
}
