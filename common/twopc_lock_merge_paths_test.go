package common

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/common/mocks"
)

// Covers mergeNodesKeys paths and unlockNodesKeys/areNodesKeysLocked helpers deterministically.
func Test_Transaction_MergeAndUnlockNodesKeys(t *testing.T) {
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
}

// Covers handleRegistrySectorLockTimeout: when lock is obtained and priorityRollback returns nil, error becomes nil.
func Test_Transaction_HandleRegistrySectorLockTimeout_Succeeds(t *testing.T) {
	ctx := context.Background()
	redis := mocks.NewMockClient()
	cache.NewGlobalCache(redis, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	// Transaction with a logger that has a no-op PriorityLog and a mock l2
	tx := &Transaction{l2Cache: redis, l1Cache: cache.GetGlobalCache()}
	// Inject required dependencies for priorityRollback
	tx.registry = mocks.NewMockRegistry(false)
	// Inject a transaction logger instance
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	tx.logger = tl

	// Create an error with a LockKey user data and seed it in cache to simulate a stale owner
	ud := &sop.LockKey{Key: tx.l2Cache.FormatLockKey("z"), LockID: sop.NewUUID()}
	_ = tx.l2Cache.Set(ctx, ud.Key, ud.LockID.String(), time.Minute)
	err := sop.Error{Code: sop.RestoreRegistryFileSectorFailure, UserData: ud}

	// Call and expect nil (priorityRollback no-ops and unlocks)
	if out := tx.handleRegistrySectorLockTimeout(ctx, err); out != nil {
		t.Fatalf("expected nil error after handleRegistrySectorLockTimeout, got %v", out)
	}
}
