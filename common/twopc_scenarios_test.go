package common

// Consolidated two-phase commit transaction scenarios.
// Sources merged: twopc_preconditions_test.go, twopc_lock_merge_paths_test.go,
// twophasecommittransaction_small_test.go

import (
    "context"
    "testing"
    "time"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/btree"
    "github.com/sharedcode/sop/cache"
    "github.com/sharedcode/sop/common/mocks"
)

// ---- Preconditions and basic flow ----
func Test_TwoPC_Preconditions_And_BeginClose(t *testing.T) {
    // default maxTime path (<=0)
    tr, err := NewTwoPhaseCommitTransaction(sop.ForReading, 0, false, mockNodeBlobStore, mockStoreRepository, mockRegistry, mockRedisCache, mocks.NewMockTransactionLog())
    if err != nil {
        t.Fatalf("ctor error: %v", err)
    }
    if tr == nil {
        t.Fatalf("expected non-nil transaction")
    }

    // verify Begin/HasBegun and Close paths minimally
    if err := tr.Begin(); err != nil {
        t.Fatalf("begin err: %v", err)
    }
    if !tr.HasBegun() {
        t.Fatalf("HasBegun should be true after Begin")
    }
    if err := tr.Close(); err != nil {
        t.Fatalf("close err: %v", err)
    }

    // accessors
    if tr.GetStoreRepository() == nil {
        t.Fatalf("GetStoreRepository returned nil")
    }
    // Seed some stores and verify GetStores forwards
    _ = mockStoreRepository.Add(context.Background(), *sop.NewStoreInfo(sop.StoreOptions{Name: "s1", SlotLength: 8}))
    _ = mockStoreRepository.Add(context.Background(), *sop.NewStoreInfo(sop.StoreOptions{Name: "s2", SlotLength: 8}))
    names, err := tr.GetStores(context.Background())
    if err != nil || len(names) < 2 {
        t.Fatalf("GetStores got=%v err=%v", names, err)
    }

    // max cap path (>1h gets capped)
    tr2, err := NewTwoPhaseCommitTransaction(sop.ForReading, 3*time.Hour, false, mockNodeBlobStore, mockStoreRepository, mockRegistry, mockRedisCache, mocks.NewMockTransactionLog())
    if err != nil || tr2 == nil {
        t.Fatalf("ctor(>1h) err=%v tr2=%v", err, tr2)
    }
}

func Test_TwoPC_Begin_Errors_And_Rollback_Preconditions(t *testing.T) {
    ctx := context.Background()
    tr := &Transaction{phaseDone: -1}
    if err := tr.Begin(); err != nil {
        t.Fatalf("first begin should succeed: %v", err)
    }
    if err := tr.Begin(); err == nil {
        t.Fatalf("second begin should fail")
    }

    tr2 := &Transaction{phaseDone: 2}
    if err := tr2.Begin(); err == nil {
        t.Fatalf("begin after done should fail")
    }

    // Rollback precondition when not begun
    tr3 := &Transaction{phaseDone: -1}
    if err := tr3.Rollback(ctx, nil); err == nil {
        t.Fatalf("Rollback should fail when not begun")
    }
}

func Test_TwoPC_Phase1_And_Phase2_Preconditions(t *testing.T) {
    ctx := context.Background()

    t.Run("phase1_errors_when_not_begun", func(t *testing.T) {
        tx, _ := newMockTwoPhaseCommitTransaction(t, sop.ForWriting, -1, true)
        if err := tx.Phase1Commit(ctx); err == nil {
            t.Fatalf("expected error when Phase1Commit called before Begin")
        }
    })

    t.Run("phase1_no_check_returns_nil", func(t *testing.T) {
        tr, _ := newMockTransaction(t, sop.NoCheck, -1)
        if err := tr.Begin(); err != nil {
            t.Fatalf("begin failed: %v", err)
        }
        if err := tr.GetPhasedTransaction().(*Transaction).Phase1Commit(ctx); err != nil {
            t.Fatalf("expected nil error in NoCheck mode, got %v", err)
        }
    })

    t.Run("phase1_reader_calls_conflict_check_only", func(t *testing.T) {
        tr, _ := newMockTransaction(t, sop.ForReading, -1)
        if err := tr.Begin(); err != nil {
            t.Fatalf("begin failed: %v", err)
        }
        if err := tr.GetPhasedTransaction().(*Transaction).Phase1Commit(ctx); err != nil {
            t.Fatalf("expected nil for reader Phase1Commit, got %v", err)
        }
    })

    t.Run("phase2_errors_when_not_begun", func(t *testing.T) {
        tx, _ := newMockTwoPhaseCommitTransaction(t, sop.ForWriting, -1, true)
        if err := tx.Phase2Commit(ctx); err == nil {
            t.Fatalf("expected error when Phase2Commit called before Begin")
        }
    })

    t.Run("phase2_reader_returns_nil", func(t *testing.T) {
        tr, _ := newMockTransaction(t, sop.ForReading, -1)
        if err := tr.Begin(); err != nil {
            t.Fatalf("begin failed: %v", err)
        }
        tr.GetPhasedTransaction().(*Transaction).phaseDone = 1
        if err := tr.GetPhasedTransaction().(*Transaction).Phase2Commit(ctx); err != nil {
            t.Fatalf("expected nil for reader Phase2Commit, got %v", err)
        }
    })

    t.Run("phase2_errors_when_phase1_not_done", func(t *testing.T) {
        tr := &Transaction{phaseDone: -1}
        if err := tr.Begin(); err != nil {
            t.Fatalf("begin err: %v", err)
        }
        if err := tr.Phase2Commit(ctx); err == nil {
            t.Fatalf("Phase2Commit should error when phase 1 not invoked")
        }
    })
}

// ---- Lock/merge and helper paths ----
func Test_TwoPC_Merge_Unlock_And_HandleSectorLockTimeout(t *testing.T) {
    ctx := context.Background()
    redis := mocks.NewMockClient()
    cache.NewGlobalCache(redis, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)

    tx := &Transaction{l2Cache: redis, l1Cache: cache.GetGlobalCache()}

    // Build minimal stores and nodes
    so := sop.StoreOptions{Name: "st", SlotLength: 2}
    si := sop.NewStoreInfo(so)
    n1 := &btree.Node[PersonKey, Person]{ID: sop.NewUUID(), Version: 1}
    n2 := &btree.Node[PersonKey, Person]{ID: sop.NewUUID(), Version: 1}
    updated := []sop.Tuple[*sop.StoreInfo, []any]{{First: si, Second: []any{n1}}}
    removed := []sop.Tuple[*sop.StoreInfo, []any]{{First: si, Second: []any{n2}}}

    // Merge keys, should create two lock keys
    tx.mergeNodesKeys(ctx, updated, removed)
    if !tx.areNodesKeysLocked() || len(tx.nodesKeys) != 2 {
        t.Fatalf("expected 2 merged lock keys, got %+v", tx.nodesKeys)
    }

    // Merge again with only one of the two IDs to exercise release of obsolete lock
    updated2 := []sop.Tuple[*sop.StoreInfo, []any]{{First: si, Second: []any{n1}}}
    removed2 := []sop.Tuple[*sop.StoreInfo, []any]{}
    tx.mergeNodesKeys(ctx, updated2, removed2)
    if !tx.areNodesKeysLocked() || len(tx.nodesKeys) != 1 {
        t.Fatalf("expected 1 lock key after merge, got %+v", tx.nodesKeys)
    }

    // Unlock
    if err := tx.unlockNodesKeys(ctx); err != nil {
        t.Fatalf("unlockNodesKeys error: %v", err)
    }
    if tx.areNodesKeysLocked() {
        t.Fatalf("nodes keys should be unlocked")
    }

    // handleRegistrySectorLockTimeout branch where we acquire lock and priorityRollback returns nil -> nil
    tx.registry = mocks.NewMockRegistry(false)
    tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
    tx.logger = tl
    ud := &sop.LockKey{Key: tx.l2Cache.FormatLockKey("z"), LockID: sop.NewUUID()}
    _ = tx.l2Cache.Set(ctx, ud.Key, ud.LockID.String(), time.Minute)
    err := sop.Error{Code: sop.RestoreRegistryFileSectorFailure, UserData: ud}
    if out := tx.handleRegistrySectorLockTimeout(ctx, err); out != nil {
        t.Fatalf("expected nil error after handleRegistrySectorLockTimeout, got %v", out)
    }
}

// ---- Delete obsolete entries smoke ----
func Test_TwoPC_DeleteObsoleteEntries_Smoke(t *testing.T) {
    ctx := context.Background()
    tr := &Transaction{blobStore: mockNodeBlobStore, registry: mockRegistry, l1Cache: cache.GetGlobalCache()}
    del := []sop.RegistryPayload[sop.UUID]{{IDs: []sop.UUID{sop.NewUUID()}}}
    unused := []sop.BlobsPayload[sop.UUID]{{Blobs: []sop.UUID{sop.NewUUID()}}}
    if err := tr.deleteObsoleteEntries(ctx, del, unused); err != nil {
        t.Fatalf("deleteObsoleteEntries err: %v", err)
    }
}
