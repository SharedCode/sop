package fs

import (
    "context"
    "testing"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/common/mocks"
)

// Covers Update (with per-key locks), UpdateNoLocks, Remove cache eviction path, and Replicate no-op when replication disabled.
func TestRegistryUpdateVariantsAndRemove(t *testing.T) {
    ctx := context.Background()
    l2 := mocks.NewMockClient()
    base := t.TempDir()
    rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
    r := NewRegistry(true, hashMod, rt, l2)

    // Seed two handles
    h1 := sop.NewHandle(sop.NewUUID())
    h2 := sop.NewHandle(sop.NewUUID())
    if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{
        {RegistryTable: "regx", IDs: []sop.Handle{h1, h2}},
    }); err != nil { t.Fatalf("add: %v", err) }

    // Update using Update (locks)
    h1.Version = 1
    if err := r.Update(ctx, []sop.RegistryPayload[sop.Handle]{
        {RegistryTable: "regx", IDs: []sop.Handle{h1}},
    }); err != nil { t.Fatalf("update: %v", err) }

    // Update using UpdateNoLocks
    h2.Version = 2
    if err := r.UpdateNoLocks(ctx, true, []sop.RegistryPayload[sop.Handle]{
        {RegistryTable: "regx", IDs: []sop.Handle{h2}},
    }); err != nil { t.Fatalf("update nolocks: %v", err) }

    // Remove one ID
    if err := r.Remove(ctx, []sop.RegistryPayload[sop.UUID]{
        {RegistryTable: "regx", IDs: []sop.UUID{h1.LogicalID}},
    }); err != nil { t.Fatalf("remove: %v", err) }

    // Replicate no-op when replicate=false
    if err := r.Replicate(ctx, nil, nil, nil, nil); err != nil { t.Fatalf("replicate disabled: %v", err) }

    r.Close()
}

// Covers Replicate branch toggling to passive and calling add/set/remove without errors.
func TestRegistryReplicateWritesToPassive(t *testing.T) {
    ctx := context.Background()
    l2 := mocks.NewMockClient()
    active := t.TempDir()
    passive := t.TempDir()
    rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
    r := NewRegistry(true, hashMod, rt, l2)

    // Prepare payloads
    hNew := sop.NewHandle(sop.NewUUID())
    hAdd := sop.NewHandle(sop.NewUUID())
    hUpd := sop.NewHandle(sop.NewUUID())
    hDel := sop.NewHandle(sop.NewUUID())

    // Active writes to create initial state
    if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{
        {RegistryTable: "regy", IDs: []sop.Handle{hAdd, hUpd, hDel}},
    }); err != nil { t.Fatalf("seed add: %v", err) }

    // Replicate payloads to passive
    if err := r.Replicate(ctx,
        []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regy", IDs: []sop.Handle{hNew}}},
        []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regy", IDs: []sop.Handle{hAdd}}},
        []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regy", IDs: []sop.Handle{hUpd}}},
        []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regy", IDs: []sop.Handle{hDel}}},
    ); err != nil { t.Fatalf("replicate: %v", err) }

    r.Close()
}
