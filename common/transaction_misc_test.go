package common

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

func Test_Transaction_UnlockNodesKeys_NoKeys_NoOp(t *testing.T) {
	ctx := context.Background()
	tx := &Transaction{l2Cache: mocks.NewMockClient()}
	// No keys set
	if err := tx.unlockNodesKeys(ctx); err != nil {
		t.Fatalf("expected nil error when no keys present, got %v", err)
	}
	// With a key set, should clear nodesKeys and return nil
	lk := tx.l2Cache.CreateLockKeys([]string{"k1"})
	// Mark as ours to allow Unlock to delete
	lk[0].IsLockOwner = true
	tx.nodesKeys = lk
	if err := tx.unlockNodesKeys(ctx); err != nil {
		t.Fatalf("unexpected error unlocking nodes keys: %v", err)
	}
	if tx.nodesKeys != nil {
		t.Fatalf("expected nodesKeys to be nil after unlock")
	}
}

func Test_Transaction_AreNodesKeysLocked_Toggles(t *testing.T) {
	tx := &Transaction{}
	if tx.areNodesKeysLocked() {
		t.Fatalf("expected false when nodesKeys is nil")
	}
	tx.nodesKeys = []*sop.LockKey{{Key: "Lk"}}
	if !tx.areNodesKeysLocked() {
		t.Fatalf("expected true when nodesKeys is set")
	}
}

func Test_Transaction_MergeNodesKeys_EmptyReleasesExisting(t *testing.T) {
	ctx := context.Background()
	tx := &Transaction{l2Cache: mocks.NewMockClient()}
	// Prime with an existing key
	tx.nodesKeys = tx.l2Cache.CreateLockKeys([]string{"k2"})
	tx.nodesKeys[0].IsLockOwner = true
	// Calling with no updated/removed should unlock and nil nodesKeys
	tx.mergeNodesKeys(ctx, nil, nil)
	if tx.nodesKeys != nil {
		t.Fatalf("expected nodesKeys to be nil after mergeNodesKeys with empty inputs")
	}
}

// processExpired cases covered in transactionlogger_unit_test table-driven tests.
