package common_test

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/infs"
)

func TestOnCommit_FiresOnCommit(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	trans, err := infs.NewTransaction(ctx, infs.TransationOptions{
		Mode:             sop.ForWriting,
		MaxTime:          -1,
		StoresBaseFolder: tmpDir,
		Cache:            cache.NewInMemoryCache(),
	})
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
	ctx := context.Background()
	tmpDir := t.TempDir()
	trans, err := infs.NewTransaction(ctx, infs.TransationOptions{
		Mode:             sop.ForWriting,
		MaxTime:          -1,
		StoresBaseFolder: tmpDir,
		Cache:            cache.NewInMemoryCache(),
	})
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
