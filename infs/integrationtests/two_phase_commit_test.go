//go:build integration
// +build integration

package integrationtests

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/fs"
	"github.com/sharedcode/sop/infs"
)

func Test_TwoPhaseCommit_RolledBack_Short(t *testing.T) {
	ctx := context.Background()
	to := sop.TransactionOptions{StoresFolders: []string{dataPath}, Mode: sop.ForWriting, MaxTime: -1, RegistryHashModValue: fs.MinimumModValue}

	// 1. Create store and commit.
	t0, _ := infs.NewTransaction(ctx, to)
	t0.Begin(ctx)
	b3, err := infs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name: "twophase2_short", SlotLength: 8, IsValueDataInNodeSegment: true,
	}, t0, nil)
	if err != nil {
		t.Fatalf("NewBtree failed: %v", err)
	}
	_ = t0.Commit(ctx)

	// 2. Add items and rollback.
	t1, _ := infs.NewTransaction(ctx, to)
	t1.Begin(ctx)

	b3, err = infs.OpenBtree[int, string](ctx, "twophase2_short", t1, nil)
	if err != nil {
		t.Fatalf("OpenBtree failed: %v", err)
	}
	orig := b3.Count()
	b3.Add(ctx, 1, "a")
	b3.Add(ctx, 2, "b")
	if b3.Count() != orig+2 {
		t.Fatalf("count got %d want %d", b3.Count(), orig+2)
	}

	tp := t1.GetPhasedTransaction()
	if err := tp.Phase1Commit(ctx); err != nil {
		t.Fatalf("phase1: %v", err)
	}
	if err := tp.Rollback(ctx, nil); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// 3. Verify rolled back
	t1, _ = infs.NewTransaction(ctx, to)
	t1.Begin(ctx)
	b3, err = infs.OpenBtree[int, string](ctx, "twophase2_short", t1, nil)
	if err != nil {
		t.Fatalf("OpenBtree verification failed: %v", err)
	}
	if b3.Count() != orig {
		t.Fatalf("after rollback count got %d want %d", b3.Count(), orig)
	}
	_ = t1.Commit(ctx)
}
