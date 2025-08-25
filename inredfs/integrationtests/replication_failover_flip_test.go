//go:build integration
// +build integration

package integrationtests

import (
    "context"
    "fmt"
    "os"
    "path/filepath"
    "testing"
    "time"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/fs"
    "github.com/sharedcode/sop/inredfs"
)

// Test_ActiveSide_FailoverFlip_Then_Reinstate_FastForward uses the DirectIO simulator to inject
// EIO on active registry WriteAt, causing a failover (toggler flip). It then restores IO and
// runs reinstate, verifying delta fast-forwarded writes are present and flags cleared.
func Test_ActiveSide_FailoverFlip_Then_Reinstate_FastForward(t *testing.T) {
    ctx := context.Background()

    // Isolated stores for this test
    isolatedStores := []string{
        fmt.Sprintf("%s%cdisk8", dataPath, os.PathSeparator),
        fmt.Sprintf("%s%cdisk9", dataPath, os.PathSeparator),
    }
    isolatedEC := map[string]fs.ErasureCodingConfig{
        "": {
            DataShardsCount:   2,
            ParityShardsCount: 2,
            BaseFolderPathsAcrossDrives: []string{
                fmt.Sprintf("%s%cdisk10", dataPath, os.PathSeparator),
                fmt.Sprintf("%s%cdisk11", dataPath, os.PathSeparator),
                fmt.Sprintf("%s%cdisk12", dataPath, os.PathSeparator),
                fmt.Sprintf("%s%cdisk13", dataPath, os.PathSeparator),
            },
            RepairCorruptedShards: true,
        },
    }
    to, _ := inredfs.NewTransactionOptionsWithReplication(sop.ForWriting, -1, fs.MinimumModValue, isolatedStores, isolatedEC)

    table := "ec_failover_flip_it"

    // Clean environment
    sanitizeIsolatedReplicationBases(t)
    cleanupStoreEverywhere(table)
    cleanupECShards(table)
    cleanupStoreRepository(table, isolatedStores)

    // Ensure store exists and seed an item
    if err := ensureStoreExists(ctx, to, table); err != nil { t.Fatalf("ensure store exists: %v", err) }
    tx, _ := inredfs.NewTransactionWithReplication(ctx, to)
    _ = tx.Begin()
    b, _ := inredfs.OpenBtreeWithReplication[int, string](ctx, table, tx, nil)
    _, _ = b.Upsert(ctx, 1, "one")
    _ = tx.Commit(ctx)

    // Determine active registry path and arm simulator to fail writes after a couple of successes
    activeBase := activeBaseFolder()
    regDir := filepath.Join(activeBase, table)
    // Use the table directory as prefix; segment filenames live under it. Use the special restore-sector
    // failure code to force failover even if rollback succeeds.
    ArmActiveRegistryRestoreSectorFail(regDir, 0, true)
    defer ResetDirectIOSim()

    initialActive := false
    if fs.GlobalReplicationDetails != nil { initialActive = fs.GlobalReplicationDetails.ActiveFolderToggler }

    // Attempt a write that will hit the simulated EIO
    tx2, _ := inredfs.NewTransactionWithReplication(ctx, to)
    _ = tx2.Begin()
    b2, _ := inredfs.OpenBtreeWithReplication[int, string](ctx, table, tx2, nil)
    // upsert a new key; commit will hit registry write and fail
    _, _ = b2.Upsert(ctx, 2, "two")
    _ = tx2.Commit(ctx) // expect error via replication path; failover handler will be invoked

    // Wait for toggler flip
    deadline := time.Now().Add(3 * time.Second)
    for time.Now().Before(deadline) {
        if fs.GlobalReplicationDetails != nil && fs.GlobalReplicationDetails.ActiveFolderToggler != initialActive {
            break
        }
        time.Sleep(10 * time.Millisecond)
    }
    if fs.GlobalReplicationDetails == nil || fs.GlobalReplicationDetails.ActiveFolderToggler == initialActive {
        t.Fatalf("expected active folder toggler flip after EIO injection; got %+v", func() any { if fs.GlobalReplicationDetails==nil {return nil}; return *fs.GlobalReplicationDetails }())
    }
    if !fs.GlobalReplicationDetails.FailedToReplicate {
        t.Fatalf("expected FailedToReplicate=true after failover")
    }

    // Disarm failures and run reinstate; write deltas during reinstate
    ResetDirectIOSim()
    reinstateErr := make(chan error, 1)
    go func(){ reinstateErr <- inredfs.ReinstateFailedDrives(ctx, isolatedStores) }()

    // Write some deltas while reinstate runs
    for i := 3; i <= 5; i++ {
        tw, _ := inredfs.NewTransactionWithReplication(ctx, to)
        _ = tw.Begin()
        bw, _ := inredfs.OpenBtreeWithReplication[int, string](ctx, table, tw, nil)
        _, _ = bw.Upsert(ctx, i, fmt.Sprintf("delta-%d", i))
        _ = tw.Commit(ctx)
        time.Sleep(15 * time.Millisecond)
    }

    if err := <-reinstateErr; err != nil { t.Fatalf("reinstate error: %v", err) }

    if err := waitForFailedFlagFalse(2 * time.Second); err != nil {
        t.Fatalf("expected FailedToReplicate=false after reinstate: %v", err)
    }

    got := readAll(ctx, to, table, t)
    // The upsert of key=2 failed due to injected failover; expect only seed and deltas written during reinstate.
    want := map[int]string{1:"one", 3:"delta-3", 4:"delta-4", 5:"delta-5"}
    for k,v := range want {
        if gv, ok := got[k]; !ok || gv != v { t.Fatalf("missing or mismatch for key %d: got %q want %q ; all=%v", k, gv, v, got) }
    }
}
