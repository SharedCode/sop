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

// Covers rollback() branches for commitAddedNodes, commitRemovedNodes, and commitUpdatedNodes.
func Test_TransactionLogger_Rollback_AddedRemovedUpdated_Branches(t *testing.T) {
	ctx := context.Background()

	// Isolated mocks per test
	localRedis := mocks.NewMockClient()
	cache.NewGlobalCache(localRedis, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	localBlobs := mocks.NewMockBlobStore()
	localReg := mocks.NewMockRegistry(false)

	// Minimal transaction and injected node repository backend
	tx := &Transaction{l2Cache: localRedis, l1Cache: cache.GetGlobalCache(), blobStore: localBlobs, registry: localReg}
	nr := &nodeRepositoryBackend{transaction: tx, l2Cache: localRedis, l1Cache: cache.GetGlobalCache()}
	tx.btreesBackend = []btreeBackend{{nodeRepository: nr}}

	// Prepare IDs for each branch
	addedID := sop.NewUUID()
	removedLID := sop.NewUUID()
	updatedTempID := sop.NewUUID()

	// Seed state for commitAddedNodes rollback: blob and cache present, registry has handle
	_ = localBlobs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "bt_add", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: addedID, Value: []byte("a")}}}})
	_ = localRedis.SetStruct(ctx, nr.formatKey(addedID.String()), &btree.Node[PersonKey, Person]{ID: addedID}, time.Minute)
	_ = localReg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rt_add", IDs: []sop.Handle{sop.NewHandle(addedID)}}})

	// Seed state for commitRemovedNodes rollback: registry has deleted flag and WIP timestamp
	hRemoved := sop.NewHandle(removedLID)
	hRemoved.IsDeleted = true
	hRemoved.WorkInProgressTimestamp = 123
	localReg.(*mocks.Mock_vid_registry).Lookup[removedLID] = hRemoved

	// Seed state for commitUpdatedNodes rollback: staged inactive blob and cache present
	_ = localBlobs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "bt_upd", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: updatedTempID, Value: []byte("t")}}}})
	_ = localRedis.SetStruct(ctx, nr.formatKey(updatedTempID.String()), &btree.Node[PersonKey, Person]{ID: updatedTempID}, time.Minute)

	// Build log payloads
	addedPayload := sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{
		First:  []sop.RegistryPayload[sop.UUID]{{RegistryTable: "rt_add", IDs: []sop.UUID{addedID}}},
		Second: []sop.BlobsPayload[sop.UUID]{{BlobTable: "bt_add", Blobs: []sop.UUID{addedID}}},
	}
	removedPayload := []sop.RegistryPayload[sop.UUID]{{RegistryTable: "rt_rem", IDs: []sop.UUID{removedLID}}}
	updatedPayload := []sop.BlobsPayload[sop.UUID]{{BlobTable: "bt_upd", Blobs: []sop.UUID{updatedTempID}}}

	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	logs := []sop.KeyValuePair[int, []byte]{
		{Key: commitAddedNodes, Value: toByteArray(addedPayload)},
		{Key: commitRemovedNodes, Value: toByteArray(removedPayload)},
		{Key: commitUpdatedNodes, Value: toByteArray(updatedPayload)},
		// Last entry controls lastCommittedFunctionLog
		{Key: finalizeCommit, Value: nil},
	}

	if err := tl.rollback(ctx, tx, sop.NewUUID(), logs); err != nil {
		t.Fatalf("rollback returned error: %v", err)
	}

	// Verify commitAddedNodes rollback effects: blob removed, cache evicted, registry ID unregistered
	if ba, _ := localBlobs.GetOne(ctx, "bt_add", addedID); len(ba) != 0 {
		t.Fatalf("added blob not removed")
	}
	var out btree.Node[PersonKey, Person]
	if ok, _ := localRedis.GetStruct(ctx, nr.formatKey(addedID.String()), &out); ok {
		t.Fatalf("added cache not evicted")
	}
	g1, _ := localReg.Get(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "rt_add", IDs: []sop.UUID{addedID}}})
	if len(g1) > 0 && len(g1[0].IDs) > 0 {
		t.Fatalf("registry ID from added not removed")
	}

	// Verify commitRemovedNodes rollback effects: flags cleared
	if got := localReg.(*mocks.Mock_vid_registry).Lookup[removedLID]; got.IsDeleted || got.WorkInProgressTimestamp != 0 {
		t.Fatalf("removed flags not cleared: %+v", got)
	}

	// Verify commitUpdatedNodes rollback effects: temp blob and cache removed
	if ba, _ := localBlobs.GetOne(ctx, "bt_upd", updatedTempID); len(ba) != 0 {
		t.Fatalf("updated temp blob not removed")
	}
	if ok, _ := localRedis.GetStruct(ctx, nr.formatKey(updatedTempID.String()), &out); ok {
		t.Fatalf("updated temp cache not removed")
	}
}

// Covers rollback() branch for commitNewRootNodes and commitTrackedItemsValues.
func Test_TransactionLogger_Rollback_NewRootAndTrackedValues(t *testing.T) {
	ctx := context.Background()

	localRedis := mocks.NewMockClient()
	cache.NewGlobalCache(localRedis, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	localBlobs := mocks.NewMockBlobStore()
	localReg := mocks.NewMockRegistry(false)

	tx := &Transaction{l2Cache: localRedis, l1Cache: cache.GetGlobalCache(), blobStore: localBlobs, registry: localReg}
	nr := &nodeRepositoryBackend{transaction: tx, l2Cache: localRedis, l1Cache: cache.GetGlobalCache()}
	tx.btreesBackend = []btreeBackend{{nodeRepository: nr}}

	// Tie the transaction logger so rollbackNewRootNodes can inspect committedState for registry removal.
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	tx.logger = tl
	tl.committedState = finalizeCommit

	// New root node setup
	rootLID := sop.NewUUID()
	rootNodeBlobID := sop.NewUUID()
	_ = localBlobs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "bt_root", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: rootNodeBlobID, Value: []byte("r")}}}})
	// Cache under logical key to match rollback deletion behavior
	_ = localRedis.SetStruct(ctx, nr.formatKey(rootLID.String()), &btree.Node[PersonKey, Person]{ID: rootNodeBlobID}, time.Minute)
	// Register logical handle so Remove occurs when committedState > commitNewRootNodes
	_ = localReg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rt_root", IDs: []sop.Handle{sop.NewHandle(rootLID)}}})

	rootPayload := sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{
		First:  []sop.RegistryPayload[sop.UUID]{{RegistryTable: "rt_root", IDs: []sop.UUID{rootLID}}},
		Second: []sop.BlobsPayload[sop.UUID]{{BlobTable: "bt_root", Blobs: []sop.UUID{rootNodeBlobID}}},
	}

	// Tracked items values setup
	valBlobID := sop.NewUUID()
	_ = localBlobs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "bt_val", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: valBlobID, Value: []byte("v")}}}})
	_ = localRedis.SetStruct(ctx, formatItemKey(valBlobID.String()), &Person{Email: "x"}, time.Minute)
	trackedVals := []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]{{First: true, Second: sop.BlobsPayload[sop.UUID]{BlobTable: "bt_val", Blobs: []sop.UUID{valBlobID}}}}

	logs := []sop.KeyValuePair[int, []byte]{
		{Key: commitNewRootNodes, Value: toByteArray(rootPayload)},
		{Key: commitTrackedItemsValues, Value: toByteArray(trackedVals)},
		{Key: finalizeCommit, Value: nil},
	}

	if err := tl.rollback(ctx, tx, sop.NewUUID(), logs); err != nil {
		t.Fatalf("rollback returned error: %v", err)
	}

	// Root: blob gone, cache evicted, registry removed
	if ba, _ := localBlobs.GetOne(ctx, "bt_root", rootNodeBlobID); len(ba) != 0 {
		t.Fatalf("root blob not removed")
	}
	var n btree.Node[PersonKey, Person]
	if ok, _ := localRedis.GetStruct(ctx, nr.formatKey(rootLID.String()), &n); ok {
		t.Fatalf("root cache not evicted")
	}
	g, _ := localReg.Get(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "rt_root", IDs: []sop.UUID{rootLID}}})
	if len(g) > 0 && len(g[0].IDs) > 0 {
		t.Fatalf("root registry not removed")
	}

	// Tracked values: blob removed and cache key removed
	if ba, _ := localBlobs.GetOne(ctx, "bt_val", valBlobID); len(ba) != 0 {
		t.Fatalf("tracked value blob not removed")
	}
	var pv Person
	if ok, _ := localRedis.GetStruct(ctx, formatItemKey(valBlobID.String()), &pv); ok {
		t.Fatalf("tracked value cache not removed")
	}
}
