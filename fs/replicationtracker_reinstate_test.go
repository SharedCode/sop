package fs

import (
    "context"
    "path/filepath"
    "testing"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/common/mocks"
    "github.com/sharedcode/sop/encoding"
)

// Focused tests for ReinstateFailedDrives flow and helpers, kept in a separate file to keep per-file length <350 lines.
func TestReinstateFailedDrivesFlow(t *testing.T) {
    ctx := context.Background()
    l2 := mocks.NewMockClient()

    GlobalReplicationDetails = nil

    active := t.TempDir()
    passive := t.TempDir()
    rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
    if err != nil { t.Fatalf("NewReplicationTracker: %v", err) }

    // Mark as failed so ReinstateFailedDrives is allowed.
    rt.ReplicationTrackedDetails.FailedToReplicate = true
    if GlobalReplicationDetails != nil {
        GlobalReplicationDetails.FailedToReplicate = true
    }

    // Seed registry hash-mod value via StoreRepository initialization.
    sr, err := NewStoreRepository(ctx, rt, nil, l2, MinimumModValue)
    if err != nil { t.Fatalf("NewStoreRepository: %v", err) }

    // Seed store list and one store info in active.
    si := sop.NewStoreInfo(sop.StoreOptions{Name: "s1", SlotLength: 10})
    if err := sr.Add(ctx, *si); err != nil { t.Fatalf("Add store: %v", err) }

    // Also create a small registry footprint in active to exercise copy routine end-to-end.
    reg := NewRegistry(true, MinimumModValue, rt, l2)
    lid := sop.NewUUID()
    h := sop.Handle{LogicalID: lid}
    if err := reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: si.RegistryTable, IDs: []sop.Handle{h}}}); err != nil {
        t.Fatalf("registry add: %v", err)
    }
    if err := reg.Close(); err != nil { t.Fatalf("registry close: %v", err) }

    if err := rt.ReinstateFailedDrives(ctx); err != nil {
        t.Fatalf("ReinstateFailedDrives: %v", err)
    }

    if GlobalReplicationDetails == nil || GlobalReplicationDetails.FailedToReplicate || GlobalReplicationDetails.LogCommitChanges {
        t.Fatalf("expected replication flags cleared; got %+v", GlobalReplicationDetails)
    }

    // Passive should now have the store list and storeinfo written.
    if !NewFileIO().Exists(ctx, filepath.Join(passive, storeListFilename)) {
        t.Fatalf("expected passive to contain %s", storeListFilename)
    }
    if !NewFileIO().Exists(ctx, filepath.Join(passive, "s1", storeInfoFilename)) {
        t.Fatalf("expected passive to contain s1/%s", storeInfoFilename)
    }
}

func TestFastForwardProcessesLogs(t *testing.T) {
    ctx := context.Background()
    l2 := mocks.NewMockClient()

    GlobalReplicationDetails = nil

    active := t.TempDir()
    passive := t.TempDir()
    rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
    if err != nil { t.Fatalf("NewReplicationTracker: %v", err) }

    // Ensure reg hash-mod file exists by initializing SR once with a value.
    if _, err := NewStoreRepository(ctx, rt, nil, l2, MinimumModValue); err != nil {
        t.Fatalf("NewStoreRepository: %v", err)
    }

    // Create one store and cache it so fastForward sees Count from cache path as well.
    sr2, _ := NewStoreRepository(ctx, rt, nil, l2, 0)
    s := sop.NewStoreInfo(sop.StoreOptions{Name: "s2", SlotLength: 8})
    if err := sr2.Add(ctx, *s); err != nil { t.Fatalf("Add: %v", err) }

    // Create a commit log file in active folder.
    tid := sop.NewUUID()
    payload := sop.Tuple[[]sop.StoreInfo, [][]sop.RegistryPayload[sop.Handle]]{
        First:  []sop.StoreInfo{*s},
        Second: [][]sop.RegistryPayload[sop.Handle]{{}, {}, {}, {}},
    }
    ba, _ := encoding.DefaultMarshaler.Marshal(payload)
    fn := rt.formatActiveFolderEntity(filepath.Join(commitChangesLogFolder, tid.String()+logFileExtension))
    if err := NewFileIO().WriteFile(ctx, fn, ba, permission); err != nil {
        t.Fatalf("write commit log: %v", err)
    }

    // First call should process and delete the log.
    found, err := rt.fastForward(ctx)
    if err != nil { t.Fatalf("fastForward: %v", err) }
    if !found { t.Fatalf("expected to find and process a log file") }

    // Second call should find none.
    found, err = rt.fastForward(ctx)
    if err != nil { t.Fatalf("fastForward 2: %v", err) }
    if found { t.Fatalf("expected no more logs to process") }
}

func TestTurnOnReplicationUpdatesStatus(t *testing.T) {
    ctx := context.Background()
    l2 := mocks.NewMockClient()

    GlobalReplicationDetails = nil

    active := t.TempDir()
    passive := t.TempDir()
    rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
    if err != nil { t.Fatalf("NewReplicationTracker: %v", err) }

    // Pretend we were in failure+logging mode.
    GlobalReplicationDetails.FailedToReplicate = true
    GlobalReplicationDetails.LogCommitChanges = true

    if err := rt.turnOnReplication(ctx); err != nil {
        t.Fatalf("turnOnReplication: %v", err)
    }

    if GlobalReplicationDetails.FailedToReplicate || GlobalReplicationDetails.LogCommitChanges {
        t.Fatalf("expected flags cleared; got %+v", GlobalReplicationDetails)
    }

    // Status file should exist in active.
    if !NewFileIO().Exists(ctx, rt.formatActiveFolderEntity(replicationStatusFilename)) {
        t.Fatalf("expected replication status file in active folder")
    }
}
