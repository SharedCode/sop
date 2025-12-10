package infs

import (
	"context"
	"os"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/cache"
	_ "github.com/sharedcode/sop/cache"
)

func init() {
	sop.RegisterL2CacheFactory(sop.InMemory, cache.NewL2InMemoryCache)
}

func TestOnCommit_FiresOnCommit(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sop-test-oncommit-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()
	opts := sop.TransactionOptions{StoresFolders: []string{tmpDir}, Mode: sop.ForWriting, MaxTime: -1, CacheType: sop.InMemory}

	trans, err := NewTransaction(ctx, opts)
	if err != nil {
		t.Fatalf("NewTransaction failed: %v", err)
	}

	if err := trans.Begin(ctx); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	fired := false
	trans.OnCommit(func(ctx context.Context) error {
		fired = true
		return nil
	})

	if err := trans.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if !fired {
		t.Error("OnCommit hook was not fired after Commit")
	}
}

func TestOnCommit_DoesNotFireOnRollback(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sop-test-oncommit-rollback-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()
	opts := sop.TransactionOptions{StoresFolders: []string{tmpDir}, Mode: sop.ForWriting, MaxTime: -1, CacheType: sop.InMemory}

	trans, err := NewTransaction(ctx, opts)
	if err != nil {
		t.Fatalf("NewTransaction failed: %v", err)
	}

	if err := trans.Begin(ctx); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	fired := false
	trans.OnCommit(func(ctx context.Context) error {
		fired = true
		return nil
	})

	if err := trans.Rollback(ctx); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	if fired {
		t.Error("OnCommit hook was fired after Rollback, expected it not to")
	}
}
