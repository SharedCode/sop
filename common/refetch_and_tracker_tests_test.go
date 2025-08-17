package common

import (
    "context"
    "testing"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/btree"
    "github.com/sharedcode/sop/cache"
    "github.com/sharedcode/sop/common/mocks"
)

// Covers addAction error branch in refetchAndMergeClosure when values are out-of-node and key duplicates on unique tree.
func Test_RefetchAndMerge_Add_SeparateSegment_DuplicateKey_ReturnsError(t *testing.T) {
    ctx := context.Background()
    so := sop.StoreOptions{Name: "rfm_add_dup_out", SlotLength: 4, IsUnique: true, IsValueDataInNodeSegment: false}
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

    // Seed an item.
    pk, pv := newPerson("od", "k", "f", "a@b", "p")
    if ok, err := b3.Add(ctx, pk, pv); !ok || err != nil {
        t.Fatalf("seed Add error: ok=%v err=%v", ok, err)
    }
    // Refresh StoreInfo for refetch.
    ns.RootNodeID = b3.StoreInfo.RootNodeID
    ns.Count = 1
    _ = sr.Add(ctx, *ns)

    // Clear tracker and enqueue duplicate add in separate segment path.
    si.ItemActionTracker.(*itemActionTracker[PersonKey, Person]).items = map[sop.UUID]cacheItem[PersonKey, Person]{}
    dup := btree.Item[PersonKey, Person]{ID: sop.NewUUID(), Key: pk, Value: &pv}
    si.ItemActionTracker.(*itemActionTracker[PersonKey, Person]).items[dup.ID] = cacheItem[PersonKey, Person]{
        lockRecord: lockRecord{LockID: sop.NewUUID(), Action: addAction},
        item:       &dup,
    }

    if err := refetchAndMergeClosure(&si, b3, sr)(ctx); err == nil {
        t.Fatalf("expected duplicate error for separate segment add, got nil")
    }
}

// Covers itemActionTracker.Get path that fetches value from blob store and caches it when ValueNeedsFetch is true.
func Test_ItemActionTracker_Get_FetchesFromBlob_AndCaches(t *testing.T) {
    ctx := context.Background()
    so := sop.StoreOptions{Name: "iat_get_fetch", SlotLength: 4, IsUnique: true, IsValueDataInNodeSegment: false, IsValueDataGloballyCached: true}
    ns := sop.NewStoreInfo(so)
    l2 := mocks.NewMockClient()
    bs := mocks.NewMockBlobStore()
    trk := newItemActionTracker[PersonKey, Person](ns, l2, bs, newTransactionLogger(mocks.NewMockTransactionLog(), false))

    // Prepare an item that needs fetch and store its value in blob.
    p := Person{Gender: "m", Email: "e", Phone: "p", SSN: "s"}
    it := &btree.Item[PersonKey, Person]{ID: sop.NewUUID(), Key: PersonKey{Firstname: "f", Lastname: "l"}, Version: 1, ValueNeedsFetch: true}
    if err := bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{
        BlobTable: ns.BlobTable,
        Blobs:     []sop.KeyValuePair[sop.UUID, []byte]{{Key: it.ID, Value: toByteArray(p)}},
    }}); err != nil {
        t.Fatalf("blob add err: %v", err)
    }

    if err := trk.Get(ctx, it); err != nil {
        t.Fatalf("iat.Get err: %v", err)
    }
    if it.Value == nil || it.ValueNeedsFetch {
        t.Fatalf("expected value loaded and ValueNeedsFetch=false")
    }
}
