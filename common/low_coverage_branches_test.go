package common

import (
    "context"
    "fmt"
    "testing"
    "time"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/btree"
    "github.com/sharedcode/sop/cache"
    "github.com/sharedcode/sop/common/mocks"
)

// Covers transactionLogger.rollback branches for commitStoreInfo, commitAdded/Removed/Updated/NewRoot.
func Test_TransactionLogger_Rollback_Multiple_Phases(t *testing.T) {
    ctx := context.Background()
    // Wire minimal tx with caches, blob, store repo, registry, and backend repo required by rollback branches.
    l2 := mocks.NewMockClient()
    cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
    tx := &Transaction{l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: mocks.NewMockBlobStore(), StoreRepository: mocks.NewMockStoreRepository(), registry: mocks.NewMockRegistry(false)}
    si := sop.NewStoreInfo(sop.StoreOptions{Name: "rb_multi", SlotLength: 4})
    nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache(), count: si.Count}
    tx.btreesBackend = []btreeBackend{{nodeRepository: nr}}

    tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
    // Attach logger to transaction so rollback helpers can read committedState, etc.
    tx.logger = tl

    // Build rollback log set; last key high to enable all earlier-phase rollbacks.
    // StoreInfo update
    sis := []sop.StoreInfo{{Name: si.Name}}
    // Added nodes payload
    addVids := []sop.RegistryPayload[sop.UUID]{{RegistryTable: si.RegistryTable, BlobTable: si.BlobTable, IDs: []sop.UUID{sop.NewUUID()}}}
    addBibs := []sop.BlobsPayload[sop.UUID]{{BlobTable: si.BlobTable, Blobs: []sop.UUID{sop.NewUUID()}}}
    addTuple := sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: addVids, Second: addBibs}
    // Removed nodes payload
    rmVids := []sop.RegistryPayload[sop.UUID]{{RegistryTable: si.RegistryTable, IDs: []sop.UUID{sop.NewUUID()}}}
    // Updated nodes payload (inactive blob IDs to remove)
    updBibs := []sop.BlobsPayload[sop.UUID]{{BlobTable: si.BlobTable, Blobs: []sop.UUID{sop.NewUUID()}}}
    // New root nodes payload
    nrTuple := addTuple

    logs := []sop.KeyValuePair[int, []byte]{
        {Key: commitStoreInfo, Value: toByteArray(sis)},
        {Key: commitAddedNodes, Value: toByteArray(addTuple)},
        {Key: commitRemovedNodes, Value: toByteArray(rmVids)},
        {Key: commitUpdatedNodes, Value: toByteArray(updBibs)},
        {Key: commitNewRootNodes, Value: toByteArray(nrTuple)},
        {Key: deleteObsoleteEntries, Value: nil}, // highest stage to unlock earlier branches
    }

    if err := tl.rollback(ctx, tx, sop.NewUUID(), logs); err != nil {
        t.Fatalf("rollback multi-phase err: %v", err)
    }
}

// Covers checkTrackedItems: (a) cache error path, (b) get-get compatibility with different LockIDs.
// getErrCache wraps Cache and returns an error from GetStruct.
type getErrCache struct{ sop.Cache }

func (g getErrCache) GetStruct(ctx context.Context, key string, target interface{}) (bool, error) {
    return false, fmt.Errorf("getstruct err")
}

func Test_ItemActionTracker_CheckTrackedItems_Error_And_GetCompat(t *testing.T) {
    ctx := context.Background()

    // (a) error path
    errCache := struct{ sop.Cache }{Cache: mocks.NewMockClient()}
    si1 := sop.NewStoreInfo(sop.StoreOptions{Name: "iat_chk_err", SlotLength: 2})
    trk1 := newItemActionTracker[PersonKey, Person](si1, getErrCache{Cache: errCache.Cache}, mocks.NewMockBlobStore(), newTransactionLogger(mocks.NewMockTransactionLog(), false))
    id1 := sop.NewUUID()
    pk1, p1 := newPerson("a", "b", "m", "a@x", "p")
    it1 := &btree.Item[PersonKey, Person]{ID: id1, Key: pk1, Value: &p1, Version: 1}
    _ = trk1.Update(ctx, it1)
    if err := trk1.checkTrackedItems(ctx); err == nil {
        t.Fatalf("expected error from GetStruct in checkTrackedItems")
    }

    // (b) get-get compatibility
    l2 := mocks.NewMockClient()
    si2 := sop.NewStoreInfo(sop.StoreOptions{Name: "iat_chk_getget", SlotLength: 2})
    trk2 := newItemActionTracker[PersonKey, Person](si2, l2, mocks.NewMockBlobStore(), newTransactionLogger(mocks.NewMockTransactionLog(), false))
    id2 := sop.NewUUID()
    pk2, p2 := newPerson("c", "d", "m", "c@x", "p")
    it2 := &btree.Item[PersonKey, Person]{ID: id2, Key: pk2, Value: &p2, Version: 1}
    // Track as get
    it2.ValueNeedsFetch = false
    if err := trk2.Get(ctx, it2); err != nil {
        t.Fatalf("Get err: %v", err)
    }
    // Seed redis with a different owner but action getAction
    lr := lockRecord{LockID: sop.NewUUID(), Action: getAction}
    _ = l2.SetStruct(ctx, l2.FormatLockKey(id2.String()), &lr, time.Minute)
    if err := trk2.checkTrackedItems(ctx); err != nil {
        t.Fatalf("get-get compatibility should pass, err: %v", err)
    }
}

// Covers refetchAndMergeClosure updateAction separate-segment success path.
func Test_RefetchAndMerge_Update_SeparateSegment_Succeeds_Alt(t *testing.T) {
    ctx := context.Background()
    so := sop.StoreOptions{Name: "rfm_upd_sep", SlotLength: 4, IsUnique: true, IsValueDataInNodeSegment: false}
    ns := sop.NewStoreInfo(so)
    sr := mocks.NewMockStoreRepository()
    _ = sr.Add(ctx, *ns)

    l2 := mocks.NewMockClient()
    cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
    tr := &Transaction{registry: mocks.NewMockRegistry(false), l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: mocks.NewMockBlobStore(), logger: newTransactionLogger(mocks.NewMockTransactionLog(), false), StoreRepository: sr}

    si := StoreInterface[PersonKey, Person]{}
    si.ItemActionTracker = newItemActionTracker[PersonKey, Person](ns, tr.l2Cache, tr.blobStore, tr.logger)
    nrw := newNodeRepository[PersonKey, Person](tr, ns)
    si.NodeRepository = nrw
    si.backendNodeRepository = nrw.nodeRepositoryBackend
    b3, err := btree.New(ns, &si.StoreInterface, Compare)
    if err != nil {
        t.Fatalf("btree.New error: %v", err)
    }

    // Seed an item then prepare an update with a new item ID (separate segment behavior)
    pk, pv := newPerson("uu", "vv", "m", "u@x", "p")
    if ok, err := b3.Add(ctx, pk, pv); !ok || err != nil {
        t.Fatalf("seed add err: %v", err)
    }
    // Persist StoreInfo refresh
    ns.RootNodeID = b3.StoreInfo.RootNodeID
    ns.Count = 1
    _ = sr.Add(ctx, *ns)
    if ok, _ := b3.Find(ctx, pk, false); !ok {
        t.Fatal("seed find err")
    }
    cur, _ := b3.GetCurrentItem(ctx)
    // Seed MRU and Handle for root so repo.get can fetch after refetch resets caches
    rootID := b3.StoreInfo.RootNodeID
    if cn, ok := nrw.nodeRepositoryBackend.localCache[rootID]; ok && cn.node != nil {
        cache.GetGlobalCache().SetNodeToMRU(ctx, rootID, cn.node, ns.CacheConfig.NodeCacheDuration)
        cache.GetGlobalCache().Handles.Set([]sop.KeyValuePair[sop.UUID, sop.Handle]{{Key: rootID, Value: sop.NewHandle(rootID)}})
    } else {
        t.Fatalf("expected root node in local cache for MRU seed")
    }
    // New value under new item ID to be merged
    _, pv2 := newPerson("uu", "vv", "m", "u2@x", "p")
    newID := sop.NewUUID()
    ci := cacheItem[PersonKey, Person]{
        lockRecord:  lockRecord{LockID: sop.NewUUID(), Action: updateAction},
        item:        &btree.Item[PersonKey, Person]{ID: newID, Key: pk, Value: &pv2, Version: cur.Version},
        versionInDB: cur.Version,
    }
    // Use the existing current item ID as map key so FindWithID(uuid) finds the record.
    si.ItemActionTracker.(*itemActionTracker[PersonKey, Person]).items[cur.ID] = ci

    if err := refetchAndMergeClosure(&si, b3, sr)(ctx); err != nil {
        t.Fatalf("refetch update separate-segment err: %v", err)
    }
    // Expect tracker to have migrated to newID and mark persisted
    got, ok := si.ItemActionTracker.(*itemActionTracker[PersonKey, Person]).items[newID]
    if !ok || !got.persisted {
        t.Fatalf("expected persisted=true under newID after merge; ok=%v persisted=%v", ok, got.persisted)
    }
}
