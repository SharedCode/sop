//go:build integration
// +build integration

package integrationtests

import (
    "context"
    "testing"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/fs"
    "github.com/sharedcode/sop/inredfs"
)

func Test_TwoPhaseCommit_RolledBack_Short(t *testing.T) {
    ctx := context.Background()
    to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
    t1, _ := inredfs.NewTransaction(ctx, to)
    t1.Begin()

    b3, _ := inredfs.NewBtree[int, string](ctx, sop.StoreOptions{
        Name: "twophase2_short", SlotLength: 8, IsValueDataInNodeSegment: true,
    }, t1, nil)
    orig := b3.Count()
    b3.Add(ctx, 1, "a")
    b3.Add(ctx, 2, "b")
    if b3.Count() != orig+2 { t.Fatalf("count got %d want %d", b3.Count(), orig+2) }

    tp := t1.GetPhasedTransaction()
    if err := tp.Phase1Commit(ctx); err != nil { t.Fatalf("phase1: %v", err) }
    if err := tp.Rollback(ctx, nil); err != nil { t.Fatalf("rollback: %v", err) }

    // Verify rolled back
    t1, _ = inredfs.NewTransaction(ctx, to)
    t1.Begin()
    b3, _ = inredfs.OpenBtree[int, string](ctx, "twophase2_short", t1, nil)
    if b3.Count() != orig { t.Fatalf("after rollback count got %d want %d", b3.Count(), orig) }
    _ = t1.Commit(ctx)
}
