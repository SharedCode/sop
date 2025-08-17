package common

import (
	"context"
	"errors"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/common/mocks"
)

// Covers refetchAndMergeClosure version-mismatch detection path.
func Test_RefetchAndMerge_VersionMismatch_Error(t *testing.T) {
	ctx := context.Background()
	// Build store with value data in node segment so update path uses UpdateCurrentItem
	so := sop.StoreOptions{Name: "rfm_ver_mismatch", SlotLength: 4, IsValueDataInNodeSegment: true}
	ns := sop.NewStoreInfo(so)
	si := StoreInterface[PersonKey, Person]{}
	sr := mocks.NewMockStoreRepository()
	_ = sr.Add(ctx, *ns)

	// Fresh global cache
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	tr := &Transaction{registry: mocks.NewMockRegistry(false), l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: mocks.NewMockBlobStore(), logger: newTransactionLogger(mocks.NewMockTransactionLog(), false), StoreRepository: sr}
	si.ItemActionTracker = newItemActionTracker[PersonKey, Person](ns, tr.l2Cache, tr.blobStore, tr.logger)
	nrw := newNodeRepository[PersonKey, Person](tr, ns)
	si.NodeRepository = nrw
	si.backendNodeRepository = nrw.nodeRepositoryBackend
	b3, err := btree.New(ns, &si.StoreInterface, Compare)
	if err != nil {
		t.Fatal(err)
	}

	// Seed an item and then simulate update with stale versionInDB
	pk, pv := newPerson("vu1", "vv1", "m", "v@x", "p")
	if ok, err := b3.Add(ctx, pk, pv); !ok || err != nil {
		t.Fatalf("seed add err: %v", err)
	}
	if ok, _ := b3.Find(ctx, pk, false); !ok {
		t.Fatal("seed find err")
	}
	cur, _ := b3.GetCurrentItem(ctx)
	// Track an update but with mismatching saved version
	stale := cur
	stale.Version = cur.Version // btree's current version
	si.ItemActionTracker.(*itemActionTracker[PersonKey, Person]).items[cur.ID] = cacheItem[PersonKey, Person]{
		lockRecord:  lockRecord{LockID: sop.NewUUID(), Action: updateAction},
		item:        &stale,
		versionInDB: cur.Version - 1, // older than current -> mismatch
	}

	// Seed MRU and Handle for root so repo.get can fetch after reset
	rootID := b3.StoreInfo.RootNodeID
	if cn, ok := nrw.nodeRepositoryBackend.localCache[rootID]; ok && cn.node != nil {
		cache.GetGlobalCache().SetNodeToMRU(ctx, rootID, cn.node, ns.CacheConfig.NodeCacheDuration)
		cache.GetGlobalCache().Handles.Set([]sop.KeyValuePair[sop.UUID, sop.Handle]{{Key: rootID, Value: sop.NewHandle(rootID)}})
	} else {
		t.Fatalf("expected root node in local cache for MRU seed")
	}
	// Sync StoreRepository
	upd := *ns
	upd.RootNodeID = b3.StoreInfo.RootNodeID
	upd.Count = b3.StoreInfo.Count
	_ = sr.Add(ctx, upd)

	closure := refetchAndMergeClosure(&si, b3, sr)
	if err := closure(ctx); err == nil {
		t.Fatalf("expected version mismatch error, got nil")
	}
}

// failingBlobStoreAdd returns an error on Add to exercise error path in commitUpdatedNodes.
type failingBlobStoreAdd struct{}

func (f failingBlobStoreAdd) GetOne(ctx context.Context, blobName string, blobID sop.UUID) ([]byte, error) {
	return nil, nil
}
func (f failingBlobStoreAdd) Add(ctx context.Context, storesblobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	return errors.New("blob add err")
}
func (f failingBlobStoreAdd) Update(ctx context.Context, storesblobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	return nil
}
func (f failingBlobStoreAdd) Remove(ctx context.Context, storesBlobsIDs []sop.BlobsPayload[sop.UUID]) error {
	return nil
}

// Covers commitUpdatedNodes returning false on version mismatch.
func Test_CommitUpdatedNodes_VersionMismatch_ReturnsFalse_2(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	reg := mocks.NewMockRegistry(false)
	bs := mocks.NewMockBlobStore()
	sr := mocks.NewMockStoreRepository()
	tx := &Transaction{registry: reg, l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: bs, logger: newTransactionLogger(mocks.NewMockTransactionLog(), true), StoreRepository: sr}
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "upd_ver_mismatch", SlotLength: 4})
	nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache(), count: si.Count}

	lid := sop.NewUUID()
	h := sop.NewHandle(lid)
	h.Version = 2 // backend version newer than node's version
	if err := reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: si.RegistryTable, IDs: []sop.Handle{h}}}); err != nil {
		t.Fatalf("reg add err: %v", err)
	}
	n := &btree.Node[PersonKey, Person]{ID: lid, Version: 1}
	nodes := []sop.Tuple[*sop.StoreInfo, []interface{}]{{First: si, Second: []interface{}{n}}}
	ok, handles, err := nr.commitUpdatedNodes(ctx, nodes)
	if err != nil {
		t.Fatalf("commitUpdatedNodes err: %v", err)
	}
	if ok || handles != nil {
		t.Fatalf("expected ok=false and nil handles on version mismatch, got ok=%v handles=%v", ok, handles)
	}
}

// Covers commitUpdatedNodes error from UpdateNoLocks and from blobStore.Add.
func Test_CommitUpdatedNodes_UpdateNoLocks_And_BlobAdd_Errors(t *testing.T) {
	ctx := context.Background()
	// Case 1: UpdateNoLocks error
	{
		l2 := mocks.NewMockClient()
		cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
		// Use a registry that always errors on UpdateNoLocks, regardless of allOrNothing flag
		regErr := &errAllUpdateNoLocks{Mock_vid_registry: &mocks.Mock_vid_registry{Lookup: make(map[sop.UUID]sop.Handle)}}
		bs := mocks.NewMockBlobStore()
		tx := &Transaction{registry: regErr, l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: bs, logger: newTransactionLogger(mocks.NewMockTransactionLog(), true)}
		si := sop.NewStoreInfo(sop.StoreOptions{Name: "upd_unl_err", SlotLength: 4})
		nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache(), count: si.Count}
		lid := sop.NewUUID()
		h := sop.NewHandle(lid)
		h.Version = 1
		if err := regErr.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: si.RegistryTable, IDs: []sop.Handle{h}}}); err != nil {
			t.Fatalf("reg add err: %v", err)
		}
		n := &btree.Node[PersonKey, Person]{ID: lid, Version: 1}
		nodes := []sop.Tuple[*sop.StoreInfo, []interface{}]{{First: si, Second: []interface{}{n}}}
		ok, _, err := nr.commitUpdatedNodes(ctx, nodes)
		if err == nil || ok {
			t.Fatalf("expected UpdateNoLocks error and ok=false, got ok=%v err=%v", ok, err)
		}
	}

	// Case 2: blobStore.Add error
	{
		l2 := mocks.NewMockClient()
		cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
		reg := mocks.NewMockRegistry(false)
		bs := failingBlobStoreAdd{}
		tx := &Transaction{registry: reg, l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: bs, logger: newTransactionLogger(mocks.NewMockTransactionLog(), true)}
		si := sop.NewStoreInfo(sop.StoreOptions{Name: "upd_blob_err", SlotLength: 4})
		nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache(), count: si.Count}
		lid := sop.NewUUID()
		h := sop.NewHandle(lid)
		h.Version = 1
		if err := reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: si.RegistryTable, IDs: []sop.Handle{h}}}); err != nil {
			t.Fatalf("reg add err: %v", err)
		}
		n := &btree.Node[PersonKey, Person]{ID: lid, Version: 1}
		nodes := []sop.Tuple[*sop.StoreInfo, []interface{}]{{First: si, Second: []interface{}{n}}}
		ok, _, err := nr.commitUpdatedNodes(ctx, nodes)
		if err == nil || ok {
			t.Fatalf("expected blobStore.Add error and ok=false, got ok=%v err=%v", ok, err)
		}
	}
}

// errAllUpdateNoLocks wraps the default mock registry but forces UpdateNoLocks to fail
// to exercise the error path in commitUpdatedNodes regardless of the allOrNothing flag.
type errAllUpdateNoLocks struct{ *mocks.Mock_vid_registry }

func (e *errAllUpdateNoLocks) UpdateNoLocks(ctx context.Context, allOrNothing bool, storesHandles []sop.RegistryPayload[sop.Handle]) error {
	return errors.New("induced error on UpdateNoLocks")
}

// Compose rollback logs to cover multiple branches where possible.
func Test_TransactionLogger_Rollback_With_DeleteObsoleteEntries(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	trLog := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	reg := mocks.NewMockRegistry(false)
	bs := mocks.NewMockBlobStore()
	sr := mocks.NewMockStoreRepository()
	tnx := &Transaction{l2Cache: l2, l1Cache: cache.GetGlobalCache(), registry: reg, blobStore: bs, logger: trLog, StoreRepository: sr}
	// Provide a minimal btreesBackend to satisfy calls during deleteObsoleteEntries
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "rb_del_ob", SlotLength: 4})
	nr := &nodeRepositoryBackend{transaction: tnx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache()}
	tnx.btreesBackend = []btreeBackend{{nodeRepository: nr, getStoreInfo: func() *sop.StoreInfo { return si }}}

	// Build payload for finalizeCommit that deleteObsoleteEntries uses
	vids := []sop.RegistryPayload[sop.UUID]{{RegistryTable: si.RegistryTable, IDs: []sop.UUID{sop.NewUUID()}}}
	bibs := []sop.BlobsPayload[sop.UUID]{{BlobTable: si.BlobTable, Blobs: []sop.UUID{sop.NewUUID()}}}
	tracked := []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]{{First: true, Second: sop.BlobsPayload[sop.UUID]{BlobTable: si.BlobTable, Blobs: []sop.UUID{}}}}
	fin := toByteArray(sop.Tuple[sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]], []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]]{
		First:  sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: vids, Second: bibs},
		Second: tracked,
	})

	// lastCommittedFunctionLog must be deleteObsoleteEntries (place it last)
	logs := []sop.KeyValuePair[int, []byte]{
		{Key: commitStoreInfo, Value: toByteArray([]sop.StoreInfo{*si})},
		{Key: finalizeCommit, Value: fin},
		{Key: deleteObsoleteEntries, Value: nil},
	}

	if err := trLog.rollback(ctx, tnx, trLog.transactionID, logs); err != nil {
		t.Fatalf("rollback err: %v", err)
	}
}

func Test_TransactionLogger_Rollback_Covers_Other_Branches(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	trLog := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	reg := mocks.NewMockRegistry(false)
	bs := mocks.NewMockBlobStore()
	sr := mocks.NewMockStoreRepository()
	tnx := &Transaction{l2Cache: l2, l1Cache: cache.GetGlobalCache(), registry: reg, blobStore: bs, logger: trLog, StoreRepository: sr}

	// Provide btreesBackend with a usable nodeRepository
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "rb_other", SlotLength: 4})
	nr := &nodeRepositoryBackend{transaction: tnx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache()}
	tnx.btreesBackend = []btreeBackend{{nodeRepository: nr, getStoreInfo: func() *sop.StoreInfo { return si }}}

	// Prepare payloads for different branches
	addV := toByteArray(sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{
		First:  []sop.RegistryPayload[sop.UUID]{{RegistryTable: "rt_add", IDs: []sop.UUID{sop.NewUUID()}}},
		Second: []sop.BlobsPayload[sop.UUID]{{BlobTable: "bt_add", Blobs: []sop.UUID{sop.NewUUID()}}},
	})
	rmV := toByteArray([]sop.RegistryPayload[sop.UUID]{{RegistryTable: "rt_rm", IDs: []sop.UUID{sop.NewUUID()}}})
	updV := toByteArray([]sop.BlobsPayload[sop.UUID]{{BlobTable: "bt_upd", Blobs: []sop.UUID{sop.NewUUID()}}})
	rootV := toByteArray(sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{
		First:  []sop.RegistryPayload[sop.UUID]{{RegistryTable: "rt_root", IDs: []sop.UUID{sop.NewUUID()}}},
		Second: []sop.BlobsPayload[sop.UUID]{{BlobTable: "bt_root", Blobs: []sop.UUID{sop.NewUUID()}}},
	})
	trkV := toByteArray([]sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]{{First: true, Second: sop.BlobsPayload[sop.UUID]{BlobTable: "bt_i", Blobs: []sop.UUID{}}}})
	// finalize payload for deleteTrackedItemsValues branch
	fin := toByteArray(sop.Tuple[sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]], []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]]{
		First:  sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: []sop.RegistryPayload[sop.UUID]{}, Second: []sop.BlobsPayload[sop.UUID]{}},
		Second: []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]{{First: false, Second: sop.BlobsPayload[sop.UUID]{}}},
	})

	// Make the last entry deleteTrackedItemsValues so conditions (>) are satisfied for other branches
	logs := []sop.KeyValuePair[int, []byte]{
		{Key: commitStoreInfo, Value: toByteArray([]sop.StoreInfo{*si})},
		{Key: commitAddedNodes, Value: addV},
		{Key: commitRemovedNodes, Value: rmV},
		{Key: commitUpdatedNodes, Value: updV},
		{Key: commitNewRootNodes, Value: rootV},
		{Key: commitTrackedItemsValues, Value: trkV},
		{Key: finalizeCommit, Value: fin},
		{Key: deleteTrackedItemsValues, Value: nil},
	}

	if err := trLog.rollback(ctx, tnx, trLog.transactionID, logs); err != nil {
		t.Fatalf("rollback err: %v", err)
	}
}
