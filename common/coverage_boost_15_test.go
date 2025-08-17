package common

import (
    "context"
    "testing"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/btree"
    "github.com/sharedcode/sop/cache"
    "github.com/sharedcode/sop/common/mocks"
)

// Covers refetchAndMergeClosure remove path when values are in a separate segment.
func Test_RefetchAndMerge_Remove_SeparateSegment_Succeeds(t *testing.T) {
    ctx := context.Background()
    so := sop.StoreOptions{Name: "rfm_rm_sep", SlotLength: 4, IsUnique: true, IsValueDataInNodeSegment: false}
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

    // Seed an item; persist StoreInfo; seed MRU and Handle for root
    pk, pv := newPerson("rm", "sep", "m", "x@y", "p")
    if ok, err := b3.Add(ctx, pk, pv); !ok || err != nil {
        t.Fatalf("seed add err: %v", err)
    }
    if ok, _ := b3.Find(ctx, pk, false); !ok {
        t.Fatal("seed find err")
    }
    cur, _ := b3.GetCurrentItem(ctx)
    // Sync StoreRepository and MRU/Handle so refetch can find root after cache reset
    upd := *ns
    upd.RootNodeID = b3.StoreInfo.RootNodeID
    upd.Count = b3.StoreInfo.Count
    _ = sr.Add(ctx, upd)
    rootID := b3.StoreInfo.RootNodeID
    if cn, ok := nrw.nodeRepositoryBackend.localCache[rootID]; ok && cn.node != nil {
        cache.GetGlobalCache().SetNodeToMRU(ctx, rootID, cn.node, ns.CacheConfig.NodeCacheDuration)
        cache.GetGlobalCache().Handles.Set([]sop.KeyValuePair[sop.UUID, sop.Handle]{{Key: rootID, Value: sop.NewHandle(rootID)}})
    } else {
        t.Fatalf("expected root node in local cache for MRU seed")
    }

    // Track removal action
    si.ItemActionTracker.(*itemActionTracker[PersonKey, Person]).items[cur.ID] = cacheItem[PersonKey, Person]{
        lockRecord:  lockRecord{LockID: sop.NewUUID(), Action: removeAction},
        item:        &cur,
        versionInDB: cur.Version,
    }

    if err := refetchAndMergeClosure(&si, b3, sr)(ctx); err != nil {
        t.Fatalf("refetch remove separate-segment err: %v", err)
    }
    if ok, _ := b3.Find(ctx, pk, false); ok {
        t.Fatal("expected item removed after refetch+remove (separate segment)")
    }
}

// blobStoreSpy tracks Add calls; used to assert no-op when actively persisted values are enabled.
type blobStoreSpy struct{ adds int }

func (s *blobStoreSpy) GetOne(ctx context.Context, blobTable string, blobID sop.UUID) ([]byte, error) { return nil, nil }
func (s *blobStoreSpy) Add(ctx context.Context, storesblobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
    s.adds++
    return nil
}
func (s *blobStoreSpy) Update(ctx context.Context, storesblobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error { return nil }
func (s *blobStoreSpy) Remove(ctx context.Context, storesBlobsIDs []sop.BlobsPayload[sop.UUID]) error { return nil }

// Covers commitTrackedItemsValues early-return when IsValueDataActivelyPersisted=true.
func Test_CommitTrackedItemsValues_ActivelyPersisted_NoOp(t *testing.T) {
    ctx := context.Background()
    so := sop.StoreOptions{Name: "iat_active_noop", SlotLength: 2, IsValueDataInNodeSegment: false, IsValueDataActivelyPersisted: true}
    ns := sop.NewStoreInfo(so)
    l2 := mocks.NewMockClient()
    cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
    spy := &blobStoreSpy{}
    tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)

    trk := newItemActionTracker[PersonKey, Person](ns, l2, spy, tl)
    // Seed a tracked item directly (avoid Add which actively persists in this mode)
    id := sop.NewUUID()
    pk, v := newPerson("a1", "b1", "m", "a1@x", "p")
    it := &btree.Item[PersonKey, Person]{ID: id, Key: pk, Value: &v, Version: 1}
    trk.items[id] = cacheItem[PersonKey, Person]{
        lockRecord:  lockRecord{LockID: sop.NewUUID(), Action: addAction},
        item:        it,
        versionInDB: it.Version,
    }
    // Commit values should be a no-op due to actively persisted flag
    if err := trk.commitTrackedItemsValues(ctx); err != nil {
        t.Fatalf("commitTrackedItemsValues err: %v", err)
    }
    if spy.adds != 0 {
        t.Fatalf("expected no blob Add when actively persisted; got %d", spy.adds)
    }
    // Ensure tracked item still has same ID and non-nil value (no mutation occurred)
    ci, ok := trk.items[id]
    if !ok || ci.item == nil || ci.item.ID != id || ci.item.Value == nil {
        t.Fatalf("expected tracked item unchanged; ok=%v idOk=%v valNil=%v", ok, ci.item != nil && ci.item.ID == id, ci.item == nil || ci.item.Value == nil)
    }
}
