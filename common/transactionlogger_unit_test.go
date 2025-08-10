package common

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/common/mocks"
)

func Test_TransactionLogger_AcquireLocks_Succeeds(t *testing.T) {
	ctx := context.Background()
	// Minimal transaction with mock L2 cache
	txn := &Transaction{l2Cache: mockRedisCache}
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)

	// Build a single store/handle payload
	lid := sop.NewUUID()
	h := sop.NewHandle(lid)
	stores := []sop.RegistryPayload[sop.Handle]{
		{IDs: []sop.Handle{h}},
	}
	keys, err := tl.acquireLocks(ctx, txn, sop.NewUUID(), stores)
	if err != nil {
		t.Fatalf("acquireLocks err: %v", err)
	}
	if len(keys) != 1 || !keys[0].IsLockOwner {
		t.Fatalf("unexpected lock keys: %+v", keys)
	}
	// Cleanup
	_ = txn.l2Cache.Unlock(ctx, keys)
}

func Test_TransactionLogger_PriorityRollback_NoOps(t *testing.T) {
	ctx := context.Background()
	// Registry mock returns nil on UpdateNoLocks; priority log is no-op
	txn := &Transaction{registry: mocks.NewMockRegistry(false)}
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	if err := tl.priorityRollback(ctx, txn, sop.NewUUID()); err != nil {
		t.Fatalf("priorityRollback returned error: %v", err)
	}
}

func Test_TransactionLogger_DoPriorityRollbacks_Empty(t *testing.T) {
	ctx := context.Background()
	txn := &Transaction{l2Cache: mockRedisCache}
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	ok, err := tl.doPriorityRollbacks(ctx, txn)
	if err != nil {
		t.Fatalf("doPriorityRollbacks error: %v", err)
	}
	if ok {
		t.Fatalf("expected no work consumed, got ok=true")
	}
}

// New tests added below.

func Test_TransactionLogger_AcquireLocks_PartialLockFails(t *testing.T) {
	ctx := context.Background()
	mrc := mocks.NewMockClient()
	txn := &Transaction{l2Cache: mrc}
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)

	// Two IDs to lock; pre-lock one with a different owner to force partial lock/conflict.
	id1 := sop.NewUUID()
	id2 := sop.NewUUID()
	stores := []sop.RegistryPayload[sop.Handle]{
		{IDs: []sop.Handle{sop.NewHandle(id1), sop.NewHandle(id2)}},
	}
	otherOwner := sop.NewUUID()
	// Pre-populate one lock key with a different owner.
	k := txn.l2Cache.CreateLockKeys([]string{id1.String()})[0].Key
	_ = txn.l2Cache.Set(ctx, k, otherOwner.String(), time.Minute)

	_, err := tl.acquireLocks(ctx, txn, sop.NewUUID(), stores)
	if err == nil {
		t.Fatalf("expected error for partial/conflicting lock, got nil")
	}
	if se, ok := err.(sop.Error); !ok || se.Code != sop.RestoreRegistryFileSectorFailure {
		t.Fatalf("expected RestoreRegistryFileSectorFailure, got: %v", err)
	}
}

func Test_TransactionLogger_AcquireLocks_TakeoverDeadOwner(t *testing.T) {
	ctx := context.Background()
	mrc := mocks.NewMockClient()
	txn := &Transaction{l2Cache: mrc}
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)

	// Simulate keys locked by a dead transaction with id=tid; acquireLocks should take over.
	tid := sop.NewUUID()
	id1 := sop.NewUUID()
	id2 := sop.NewUUID()
	stores := []sop.RegistryPayload[sop.Handle]{
		{IDs: []sop.Handle{sop.NewHandle(id1), sop.NewHandle(id2)}},
	}
	// Pre-populate both lock keys with the same dead-owner tid.
	k1 := txn.l2Cache.CreateLockKeys([]string{id1.String()})[0].Key
	k2 := txn.l2Cache.CreateLockKeys([]string{id2.String()})[0].Key
	_ = txn.l2Cache.Set(ctx, k1, tid.String(), time.Minute)
	_ = txn.l2Cache.Set(ctx, k2, tid.String(), time.Minute)

	keys, err := tl.acquireLocks(ctx, txn, tid, stores)
	if err != nil {
		t.Fatalf("acquireLocks takeover err: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
	for _, k := range keys {
		if !k.IsLockOwner {
			t.Fatalf("expected takeover to mark IsLockOwner=true, got: %+v", k)
		}
	}
	_ = txn.l2Cache.Unlock(ctx, keys)
}

func Test_TransactionLogger_Rollback_FinalizeCommit_Path(t *testing.T) {
	ctx := context.Background()
	// Build a concrete Transaction with mocks
	twoPhase, _ := newMockTwoPhaseCommitTransaction(t, sop.ForWriting, -1, true)
	tx := twoPhase.(*Transaction)
	// Use a fresh logger to call rollback directly
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)

	// Compose the finalizeCommit payload with some obsolete entries and tracked items
	blobID := sop.NewUUID()
	regID := sop.NewUUID()
	pl := sop.Tuple[sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]], []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]]{
		First: sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{
			First: []sop.RegistryPayload[sop.UUID]{
				{RegistryTable: "rt", IDs: []sop.UUID{regID}},
			},
			Second: []sop.BlobsPayload[sop.UUID]{
				{BlobTable: "bt", Blobs: []sop.UUID{blobID}},
			},
		},
		Second: []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]{
			{First: true, Second: sop.BlobsPayload[sop.UUID]{BlobTable: "it", Blobs: []sop.UUID{blobID}}},
		},
	}

	logs := []sop.KeyValuePair[int, []byte]{
		{Key: finalizeCommit, Value: toByteArray(pl)},
	}

	if err := tl.rollback(ctx, tx, sop.NewUUID(), logs); err != nil {
		t.Fatalf("rollback finalizeCommit path returned error: %v", err)
	}
}

func Test_TransactionLogger_Rollback_FinalizeCommit_DeletesAll(t *testing.T) {
	ctx := context.Background()
	// Local mocks to avoid interfering with package-level globals
	localRedis := mocks.NewMockClient()
	cache.NewGlobalCache(localRedis, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	localBlobs := mocks.NewMockBlobStore()
	localReg := mocks.NewMockRegistry(false)
	// Minimal transaction with only the deps used by finalize path
	tx := &Transaction{l2Cache: localRedis, l1Cache: cache.GetGlobalCache(), blobStore: localBlobs, registry: localReg}
	// Fresh logger to drive rollback directly
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)

	// Seed: one obsolete node blob, one registry ID, and one tracked item value blob + cache
	nodeBlobID := sop.NewUUID()
	itemBlobID := sop.NewUUID()
	regID := sop.NewUUID()
	// Add blobs
	_ = localBlobs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
		{BlobTable: "bt", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: nodeBlobID, Value: []byte("n")}}},
		{BlobTable: "it", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: itemBlobID, Value: []byte("v")}}},
	})
	// Add value cache for tracked item
	_ = localRedis.SetStruct(ctx, formatItemKey(itemBlobID.String()), &Person{Email: "e"}, time.Minute)
	// Add a registry handle so Remove will act on it
	_ = localReg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rt", IDs: []sop.Handle{sop.NewHandle(regID)}}})

	// Build finalizeCommit payload; lastCommittedFunctionLog = deleteTrackedItemsValues
	pl := sop.Tuple[sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]], []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]]{
		First: sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{
			First:  []sop.RegistryPayload[sop.UUID]{{RegistryTable: "rt", IDs: []sop.UUID{regID}}},
			Second: []sop.BlobsPayload[sop.UUID]{{BlobTable: "bt", Blobs: []sop.UUID{nodeBlobID}}},
		},
		Second: []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]{{First: true, Second: sop.BlobsPayload[sop.UUID]{BlobTable: "it", Blobs: []sop.UUID{itemBlobID}}}},
	}
	logs := []sop.KeyValuePair[int, []byte]{
		{Key: finalizeCommit, Value: toByteArray(pl)},
		{Key: deleteTrackedItemsValues, Value: nil},
	}
	if err := tl.rollback(ctx, tx, sop.NewUUID(), logs); err != nil {
		t.Fatalf("rollback error: %v", err)
	}
	// Assert node blob deleted
	if ba, _ := localBlobs.GetOne(ctx, "bt", nodeBlobID); len(ba) != 0 {
		t.Fatalf("node blob not deleted")
	}
	// Assert tracked item blob deleted
	if ba, _ := localBlobs.GetOne(ctx, "it", itemBlobID); len(ba) != 0 {
		t.Fatalf("item blob not deleted")
	}
	// Assert value cache removed
	var pv Person
	if ok, _ := localRedis.GetStruct(ctx, formatItemKey(itemBlobID.String()), &pv); ok {
		t.Fatalf("value cache not deleted")
	}
	// Assert registry remove occurred
	got, _ := localReg.Get(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "rt", IDs: []sop.UUID{regID}}})
	if len(got) > 0 && len(got[0].IDs) > 0 {
		t.Fatalf("registry ID not removed")
	}
}

func Test_TransactionLogger_ProcessExpired_NoLogs(t *testing.T) {
	ctx := context.Background()
	// Empty mock transaction log returns nil/empty
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	// Minimal transaction just to satisfy signature
	tx := &Transaction{}
	if err := tl.processExpiredTransactionLogs(ctx, tx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func Test_TransactionLogger_Rollback_CommitStoreInfo_Path(t *testing.T) {
	ctx := context.Background()
	localReg := mocks.NewMockRegistry(false)
	localBlobs := mocks.NewMockBlobStore()
	localRedis := mocks.NewMockClient()
	cache.NewGlobalCache(localRedis, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	tx := &Transaction{registry: localReg, blobStore: localBlobs, l2Cache: localRedis, l1Cache: cache.GetGlobalCache(), StoreRepository: mocks.NewMockStoreRepository()}
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)

	// Seed a store and create a rollback log for commitStoreInfo with one StoreInfo entry
	name := "st1"
	_ = tx.StoreRepository.Add(ctx, sop.StoreInfo{Name: name})
	stores := []sop.StoreInfo{{Name: name, CountDelta: 10}}
	logs := []sop.KeyValuePair[int, []byte]{
		{Key: commitStoreInfo, Value: toByteArray(stores)},
		// Simulate that later function was not reached (so > commitStoreInfo)
	}
	// lastCommittedFunctionLog greater than commitStoreInfo to trigger rollback
	last := []sop.KeyValuePair[int, []byte]{
		{Key: finalizeCommit, Value: nil},
	}
	logs = append(logs, last...)

	if err := tl.rollback(ctx, tx, sop.NewUUID(), logs); err != nil {
		t.Fatalf("rollback error: %v", err)
	}
}
