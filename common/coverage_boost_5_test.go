package common

import (
    "context"
    "errors"
    "fmt"
    "testing"
    "time"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/btree"
    "github.com/sharedcode/sop/cache"
    "github.com/sharedcode/sop/common/mocks"
	"github.com/sharedcode/sop/encoding"
)

// getErrCache wraps a cache and forces GetStruct/GetStructEx to return an error for testing.
type getErrCache struct{ sop.Cache }

func (g getErrCache) GetStruct(ctx context.Context, key string, out interface{}) (bool, error) {
    return false, fmt.Errorf("forced getstruct error")
}
func (g getErrCache) GetStructEx(ctx context.Context, key string, out interface{}, duration time.Duration) (bool, error) {
    return false, fmt.Errorf("forced getstruct error")
}

func Test_ItemActionTracker_CheckTrackedItems_Table(t *testing.T) {
    ctx := context.Background()
    si := sop.NewStoreInfo(sop.StoreOptions{Name: "iat_chk", SlotLength: 4})
    // Case: no tracked items -> nil
    trk := newItemActionTracker[int, int](si, mocks.NewMockClient(), mocks.NewMockBlobStore(), newTransactionLogger(mocks.NewMockTransactionLog(), false))
    if err := trk.checkTrackedItems(ctx); err != nil { t.Fatalf("expected nil for no tracked items, got %v", err) }

    // Case: addAction is skipped -> nil
    idAdd := sop.NewUUID()
    trk.items[idAdd] = cacheItem[int, int]{lockRecord: lockRecord{LockID: sop.NewUUID(), Action: addAction}, versionInDB: 0}
    if err := trk.checkTrackedItems(ctx); err != nil { t.Fatalf("expected nil for addAction, got %v", err) }

    // Case: GetStruct error -> lastErr returned
    ge := getErrCache{Cache: mocks.NewMockClient()}
    trk2 := newItemActionTracker[int, int](si, ge, mocks.NewMockBlobStore(), newTransactionLogger(mocks.NewMockTransactionLog(), false))
    id := sop.NewUUID()
    trk2.items[id] = cacheItem[int, int]{lockRecord: lockRecord{LockID: sop.NewUUID(), Action: getAction}, versionInDB: 1}
    if err := trk2.checkTrackedItems(ctx); err == nil { t.Fatalf("expected error from GetStruct path") }

    // Prepare a real cache with a lock record set
    l2 := mocks.NewMockClient()
    key := l2.FormatLockKey(id.String())
    lr := lockRecord{LockID: sop.NewUUID(), Action: getAction}
    if err := l2.SetStruct(ctx, key, &lr, time.Minute); err != nil { t.Fatal(err) }

    // Case: same lockID -> isLockOwner true, nil error
    trk3 := newItemActionTracker[int, int](si, l2, mocks.NewMockBlobStore(), newTransactionLogger(mocks.NewMockTransactionLog(), false))
    trk3.items[id] = cacheItem[int, int]{lockRecord: lr, versionInDB: 1}
    if err := trk3.checkTrackedItems(ctx); err != nil { t.Fatalf("expected nil for same lockID, got %v", err) }

    // Case: get/get compatibility -> nil
    lr2 := lockRecord{LockID: sop.NewUUID(), Action: getAction}
    if err := l2.SetStruct(ctx, key, &lr2, time.Minute); err != nil { t.Fatal(err) }
    trk4 := newItemActionTracker[int, int](si, l2, mocks.NewMockBlobStore(), newTransactionLogger(mocks.NewMockTransactionLog(), false))
    trk4.items[id] = cacheItem[int, int]{lockRecord: lockRecord{LockID: sop.NewUUID(), Action: getAction}, versionInDB: 1}
    if err := trk4.checkTrackedItems(ctx); err != nil { t.Fatalf("expected nil for get/get compatibility, got %v", err) }

    // Case: conflict (update vs get) -> error
    if err := l2.SetStruct(ctx, key, &lr2, time.Minute); err != nil { t.Fatal(err) }
    trk5 := newItemActionTracker[int, int](si, l2, mocks.NewMockBlobStore(), newTransactionLogger(mocks.NewMockTransactionLog(), false))
    trk5.items[id] = cacheItem[int, int]{lockRecord: lockRecord{LockID: sop.NewUUID(), Action: updateAction}, versionInDB: 1}
    if err := trk5.checkTrackedItems(ctx); err == nil { t.Fatalf("expected conflict error") }
}

func Test_TransactionLogger_Rollback_Branches_NewRoot_Removed_Updated_StoreInfo(t *testing.T) {
    ctx := context.Background()
    tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
    l2 := mocks.NewMockClient()
    cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)

    bs := mocks.NewMockBlobStore()
    rg := mocks.NewMockRegistry(false)
    sr := mocks.NewMockStoreRepository()

    // Minimal transaction with backend repo and caches
    tx := &Transaction{blobStore: bs, l2Cache: l2, l1Cache: cache.GetGlobalCache(), registry: rg, StoreRepository: sr}
    si := sop.NewStoreInfo(sop.StoreOptions{Name: "tl_rb", SlotLength: 4})
    nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache(), count: si.Count}
    tx.btreesBackend = []btreeBackend{{nodeRepository: nr, getStoreInfo: func() *sop.StoreInfo { return si }}}

    // Build logs for multiple branches; ensure lastCommittedFunctionLog is high enough to trigger rollbacks
    vids := []sop.RegistryPayload[sop.UUID]{{RegistryTable: si.RegistryTable, BlobTable: si.BlobTable, IDs: []sop.UUID{sop.NewUUID()}}}
    bibs := []sop.BlobsPayload[sop.UUID]{{BlobTable: si.BlobTable, Blobs: []sop.UUID{sop.NewUUID()}}}
    // finalize payload with both obsolete entries and tracked items values to cover those code paths
    finPayload := sop.Tuple[sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]], []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]]{
        First:  sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: vids, Second: bibs},
        Second: []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]{{First: false, Second: sop.BlobsPayload[sop.UUID]{BlobTable: si.BlobTable, Blobs: []sop.UUID{sop.NewUUID()}}}},
    }
    logs := []sop.KeyValuePair[int, []byte]{
        {Key: commitStoreInfo, Value: toByteArray([]sop.StoreInfo{*si})},
        {Key: commitAddedNodes, Value: toByteArray(sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: vids, Second: bibs})},
        {Key: commitRemovedNodes, Value: toByteArray(vids)},
        {Key: commitUpdatedNodes, Value: toByteArray(bibs)},
        {Key: commitNewRootNodes, Value: toByteArray(sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: vids, Second: bibs})},
        {Key: commitTrackedItemsValues, Value: toByteArray([]sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]{{First: true, Second: bibs[0]}})},
        {Key: finalizeCommit, Value: toByteArray(finPayload)},
        // Ensure lastCommittedFunctionLog is the highest we need (deleteObsoleteEntries)
        {Key: deleteObsoleteEntries, Value: nil},
    }

    if err := tl.rollback(ctx, tx, sop.NewUUID(), logs); err != nil {
        t.Fatalf("rollback matrix err: %v", err)
    }
}

func Test_TransactionLogger_Rollback_Finalize_TrackedItemsOnly(t *testing.T) {
	ctx := context.Background()
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	bs := mocks.NewMockBlobStore()
	rg := mocks.NewMockRegistry(false)
	sr := mocks.NewMockStoreRepository()
	tx := &Transaction{blobStore: bs, l2Cache: l2, l1Cache: cache.GetGlobalCache(), registry: rg, StoreRepository: sr}
	// Build finalize payload that has tracked item values to delete
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "tl_rb_vals", SlotLength: 4})
	tracked := []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]{{First: false, Second: sop.BlobsPayload[sop.UUID]{BlobTable: si.BlobTable, Blobs: []sop.UUID{sop.NewUUID()}}}}
	finPayload := sop.Tuple[sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]], []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]]{
		First:  sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{},
		Second: tracked,
	}
	logs := []sop.KeyValuePair[int, []byte]{
		// finalize first, then set lastCommittedFunctionLog to deleteTrackedItemsValues
		{Key: finalizeCommit, Value: toByteArray(finPayload)},
		{Key: commitTrackedItemsValues, Value: toByteArray(tracked)},
	}
	if err := tl.rollback(ctx, tx, sop.NewUUID(), logs); err != nil {
		t.Fatalf("rollback finalize tracked items-only err: %v", err)
	}
}

func Test_Phase2Commit_Error_Branches_LockedAndNotLocked(t *testing.T) {
	ctx := context.Background()
	// Common wiring
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	bs := mocks.NewMockBlobStore()
	sr := mocks.NewMockStoreRepository()

	// Case 1: nodesKeys locked -> priorityRollback path
	rgLocked := mocks.NewMockRegistry(true) // UpdateNoLocks(allOrNothing=true) will error
	tx1 := &Transaction{mode: sop.ForWriting, phaseDone: 1, StoreRepository: sr, registry: rgLocked, logger: newTransactionLogger(mocks.NewMockTransactionLog(), true), l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: bs}
	// Seed backend requirements minimally and backend repo for rollback paths
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "p2_err", SlotLength: 4})
	nr := &nodeRepositoryBackend{transaction: tx1, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache(), count: si.Count}
	tx1.btreesBackend = []btreeBackend{{
		nodeRepository:                   nr,
		getStoreInfo:                     func() *sop.StoreInfo { return si },
		hasTrackedItems:                  func() bool { return false },
		checkTrackedItems:                func(context.Context) error { return nil },
		lockTrackedItems:                 func(context.Context, time.Duration) error { return nil },
		unlockTrackedItems:               func(context.Context) error { return nil },
		commitTrackedItemsValues:         func(context.Context) error { return nil },
		getForRollbackTrackedItemsValues: func() *sop.BlobsPayload[sop.UUID] { return nil },
		getObsoleteTrackedItemsValues:    func() *sop.BlobsPayload[sop.UUID] { return nil },
		refetchAndMerge:                  func(context.Context) error { return nil },
	}}
	tx1.updatedNodeHandles = []sop.RegistryPayload[sop.Handle]{{RegistryTable: si.RegistryTable, IDs: []sop.Handle{sop.NewHandle(sop.NewUUID())}}}
	// Pretend nodes keys are locked
	lk := l2.CreateLockKeys([]string{"k1", "k2"})
	tx1.nodesKeys = lk
	if err := tx1.Phase2Commit(ctx); err == nil {
		t.Fatalf("expected Phase2Commit error with nodes locked")
	}

	// Case 2: nodesKeys not locked -> PriorityLog.Remove warn branch
	rgUnlocked := mocks.NewMockRegistry(true)
	tx2 := &Transaction{mode: sop.ForWriting, phaseDone: 1, StoreRepository: sr, registry: rgUnlocked, logger: newTransactionLogger(mocks.NewMockTransactionLog(), true), l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: bs}
	nr2 := &nodeRepositoryBackend{transaction: tx2, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache(), count: si.Count}
	tx2.btreesBackend = []btreeBackend{{
		nodeRepository:                   nr2,
		getStoreInfo:                     func() *sop.StoreInfo { return si },
		hasTrackedItems:                  func() bool { return false },
		checkTrackedItems:                func(context.Context) error { return nil },
		lockTrackedItems:                 func(context.Context, time.Duration) error { return nil },
		unlockTrackedItems:               func(context.Context) error { return nil },
		commitTrackedItemsValues:         func(context.Context) error { return nil },
		getForRollbackTrackedItemsValues: func() *sop.BlobsPayload[sop.UUID] { return nil },
		getObsoleteTrackedItemsValues:    func() *sop.BlobsPayload[sop.UUID] { return nil },
		refetchAndMerge:                  func(context.Context) error { return nil },
	}}
	tx2.updatedNodeHandles = []sop.RegistryPayload[sop.Handle]{{RegistryTable: si.RegistryTable, IDs: []sop.Handle{sop.NewHandle(sop.NewUUID())}}}
	if err := tx2.Phase2Commit(ctx); err == nil {
		t.Fatalf("expected Phase2Commit error with nodes not locked")
	}
}

func Test_ItemActionTracker_Get_TTL_And_NonTTL_BlobFallback(t *testing.T) {
	ctx := context.Background()
	// TTL case: IsValueDataGloballyCached + IsValueDataCacheTTL
	l2 := mocks.NewMockClient()
	bs := mocks.NewMockBlobStore()
	so := sop.StoreOptions{Name: "iat_get", SlotLength: 4, IsValueDataInNodeSegment: true, IsValueDataActivelyPersisted: false, IsValueDataGloballyCached: true}
	si := sop.NewStoreInfo(so)
	si.CacheConfig = sop.StoreCacheConfig{IsValueDataCacheTTL: true, ValueDataCacheDuration: time.Minute}
	trk := newItemActionTracker[PersonKey, Person](si, l2, bs, newTransactionLogger(mocks.NewMockTransactionLog(), false))
	pk, pv := newPerson("g1", "v1", "m", "g1@x", "p")
	id := sop.NewUUID()
	// Seed blob store with serialized value
	ba, _ := encoding.DefaultMarshaler.Marshal(pv)
	_ = bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: si.BlobTable, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: ba}}}})
	it := &btree.Item[PersonKey, Person]{ID: id, Key: pk, Version: 1, ValueNeedsFetch: true}
	if err := trk.Get(ctx, it); err != nil {
		t.Fatalf("Get TTL fallback err: %v", err)
	}
	if it.Value == nil || it.ValueNeedsFetch {
		t.Fatalf("expected value to be populated and ValueNeedsFetch=false")
	}
	// Non-TTL case: IsValueDataCacheTTL=false -> use GetStruct path
	si2 := sop.NewStoreInfo(so)
	si2.CacheConfig = sop.StoreCacheConfig{IsValueDataCacheTTL: false, ValueDataCacheDuration: time.Minute}
	trk2 := newItemActionTracker[PersonKey, Person](si2, l2, bs, newTransactionLogger(mocks.NewMockTransactionLog(), false))
	id2 := sop.NewUUID()
	ba2, _ := encoding.DefaultMarshaler.Marshal(pv)
	_ = bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: si2.BlobTable, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id2, Value: ba2}}}})
	it2 := &btree.Item[PersonKey, Person]{ID: id2, Key: pk, Version: 1, ValueNeedsFetch: true}
	if err := trk2.Get(ctx, it2); err != nil {
		t.Fatalf("Get non-TTL fallback err: %v", err)
	}
	if it2.Value == nil || it2.ValueNeedsFetch {
		t.Fatalf("expected value to be populated and ValueNeedsFetch=false (non-TTL)")
	}
}

func Test_RefetchAndMerge_Update_SeparateSegment_Succeeds(t *testing.T) {
	ctx := context.Background()
	// Build store with value data in separate segment
	so := sop.StoreOptions{Name: "rfm_upd_sep", SlotLength: 4, IsValueDataInNodeSegment: false}
	ns := sop.NewStoreInfo(so)
	si := StoreInterface[PersonKey, Person]{}
	sr := mocks.NewMockStoreRepository()
	_ = sr.Add(ctx, *ns)
	// Ensure a fresh global L1 cache backed by mock L2 for determinism
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	tr := &Transaction{registry: mocks.NewMockRegistry(false), l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: mocks.NewMockBlobStore(), logger: newTransactionLogger(mocks.NewMockTransactionLog(), false), StoreRepository: sr}
	si.ItemActionTracker = newItemActionTracker[PersonKey, Person](ns, tr.l2Cache, tr.blobStore, tr.logger)
	nrw := newNodeRepository[PersonKey, Person](tr, ns)
	si.NodeRepository = nrw
	si.backendNodeRepository = nrw.nodeRepositoryBackend
	b3, err := btree.New(ns, &si.StoreInterface, Compare)
	if err != nil { t.Fatal(err) }
	// Seed an item
	pk, pv := newPerson("u1", "v1", "m", "u1@x", "p")
	if ok, err := b3.Add(ctx, pk, pv); !ok || err != nil { t.Fatalf("seed add err: %v", err) }
	if ok, _ := b3.Find(ctx, pk, false); !ok { t.Fatal("seed find err") }
	cur, _ := b3.GetCurrentItem(ctx)
	// Prepare tracker to simulate update with same version
	cur2 := cur // copy
	cur2.Value = &pv
	// mark ValueNeedsFetch false, normal update path
	cur2.ValueNeedsFetch = false
	si.ItemActionTracker.(*itemActionTracker[PersonKey, Person]).items[cur.ID] = cacheItem[PersonKey, Person]{lockRecord: lockRecord{LockID: sop.NewUUID(), Action: updateAction}, item: &cur2, versionInDB: cur.Version}
	// Seed L1 MRU and Handles so NodeRepository.get can fetch the root after caches reset
	rootID := b3.StoreInfo.RootNodeID
	if cn, ok := nrw.nodeRepositoryBackend.localCache[rootID]; ok && cn.node != nil {
		cache.GetGlobalCache().SetNodeToMRU(ctx, rootID, cn.node, ns.CacheConfig.NodeCacheDuration)
		cache.GetGlobalCache().Handles.Set([]sop.KeyValuePair[sop.UUID, sop.Handle]{{Key: rootID, Value: sop.NewHandle(rootID)}})
	} else {
		t.Fatalf("expected root node in local cache for MRU seed")
	}
	// Ensure StoreRepository has up-to-date RootNodeID and Count so refetch works after cache reset
	upd := *ns
	upd.RootNodeID = b3.StoreInfo.RootNodeID
	upd.Count = b3.StoreInfo.Count
	_ = sr.Add(ctx, upd)
	closure := refetchAndMergeClosure(&si, b3, sr)
	if err := closure(ctx); err != nil {
		t.Fatalf("expected refetchAndMerge update(separate) success, got err: %v", err)
	}
}


// errOnAddStoreRepo errors on Add and tracks Remove calls.
type errOnAddStoreRepo struct{ removed []string }

func (e *errOnAddStoreRepo) Add(ctx context.Context, stores ...sop.StoreInfo) error { return errors.New("add failed") }
func (e *errOnAddStoreRepo) Update(ctx context.Context, stores []sop.StoreInfo) ([]sop.StoreInfo, error) {
    return nil, nil
}
func (e *errOnAddStoreRepo) Get(ctx context.Context, names ...string) ([]sop.StoreInfo, error) {
    // Return empty to force NewBtree to try Add
    return []sop.StoreInfo{}, nil
}
func (e *errOnAddStoreRepo) GetAll(ctx context.Context) ([]string, error) { return nil, nil }
func (e *errOnAddStoreRepo) GetWithTTL(ctx context.Context, isTTL bool, d time.Duration, names ...string) ([]sop.StoreInfo, error) {
    return e.Get(ctx, names...)
}
func (e *errOnAddStoreRepo) Remove(ctx context.Context, names ...string) error {
    e.removed = append(e.removed, names...)
    return nil
}
func (e *errOnAddStoreRepo) Replicate(ctx context.Context, storesInfo []sop.StoreInfo) error { return nil }

// errOnGetStoreRepo errors on Get for OpenBtree path.
type errOnGetStoreRepo struct{ err error }

func (e *errOnGetStoreRepo) Add(ctx context.Context, stores ...sop.StoreInfo) error { return nil }
func (e *errOnGetStoreRepo) Update(ctx context.Context, stores []sop.StoreInfo) ([]sop.StoreInfo, error) {
    return nil, nil
}
func (e *errOnGetStoreRepo) Get(ctx context.Context, names ...string) ([]sop.StoreInfo, error) { return nil, e.err }
func (e *errOnGetStoreRepo) GetAll(ctx context.Context) ([]string, error) { return nil, nil }
func (e *errOnGetStoreRepo) GetWithTTL(ctx context.Context, isTTL bool, d time.Duration, names ...string) ([]sop.StoreInfo, error) {
    return nil, e.err
}
func (e *errOnGetStoreRepo) Remove(ctx context.Context, names ...string) error               { return nil }
func (e *errOnGetStoreRepo) Replicate(ctx context.Context, storesInfo []sop.StoreInfo) error { return nil }

// blob store that errors on GetOne to drive itemactiontracker.Get error branch.
type errGetBlobStore struct{}

func (e errGetBlobStore) GetOne(ctx context.Context, blobName string, blobID sop.UUID) ([]byte, error) {
    return nil, errors.New("blob get error")
}
func (e errGetBlobStore) Add(ctx context.Context, storesblobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
    return nil
}
func (e errGetBlobStore) Update(ctx context.Context, storesblobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
    return nil
}
func (e errGetBlobStore) Remove(ctx context.Context, storesBlobsIDs []sop.BlobsPayload[sop.UUID]) error { return nil }

func Test_NewBtree_AddFails_CleansUpAndRollsBack(t *testing.T) {
    ctx := context.Background()
    trans, _ := newMockTransaction(t, sop.ForWriting, -1)
    if err := trans.Begin(); err != nil {
        t.Fatalf("begin err: %v", err)
    }
    // Swap repository with erroring one
    t2 := trans.GetPhasedTransaction().(*Transaction)
    ers := &errOnAddStoreRepo{}
    t2.StoreRepository = ers
    // Attempt to create new store; expect error and rollback (transaction ended)
    cmp := func(a, b int) int { if a < b { return -1 } else if a > b { return 1 } ; return 0 }
    _, err := NewBtree[int, int](ctx, sop.StoreOptions{Name: "add_fail", SlotLength: 2}, trans, cmp)
    if err == nil {
        t.Fatalf("expected error from NewBtree with Add failure")
    }
    if trans.HasBegun() {
        t.Fatalf("expected transaction ended after rollback")
    }
    // Remove should be called for attempted add
    if len(ers.removed) == 0 || ers.removed[0] != "add_fail" {
        t.Fatalf("expected Remove called for add_fail, got %v", ers.removed)
    }
}

func Test_OpenBtree_StoreRepositoryError_RollsBack(t *testing.T) {
    ctx := context.Background()
    trans, _ := newMockTransaction(t, sop.ForWriting, -1)
    _ = trans.Begin()
    t2 := trans.GetPhasedTransaction().(*Transaction)
    t2.StoreRepository = &errOnGetStoreRepo{err: errors.New("get error")}
    cmp := func(a, b int) int { if a < b { return -1 } else if a > b { return 1 } ; return 0 }
    _, err := OpenBtree[int, int](ctx, "any", trans, cmp)
    if err == nil {
        t.Fatalf("expected error from OpenBtree when Get errors")
    }
    if trans.HasBegun() {
        t.Fatalf("expected transaction ended after OpenBtree failure")
    }
}

func Test_ItemActionTracker_Get_TTL_And_BlobError(t *testing.T) {
    ctx := context.Background()
    // TTL=true path with cache miss should fall back to blob store; here blob store errors.
    so := sop.StoreOptions{
        Name:                      "iat_get_ttl",
        SlotLength:                4,
        IsValueDataInNodeSegment:  false,
        IsValueDataGloballyCached: true,
        CacheConfig: &sop.StoreCacheConfig{
            IsValueDataCacheTTL:     true,
            ValueDataCacheDuration:  time.Minute,
        },
    }
    si := sop.NewStoreInfo(so)
    // Use mock transaction log
    tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
    // Construct tracker with blob error store to force error path
    tr := newItemActionTracker[int, int](si, mockRedisCache, errGetBlobStore{}, tl)

    item := &btree.Item[int, int]{ID: sop.NewUUID(), Key: 1, Value: nil, ValueNeedsFetch: true}
    if err := tr.Get(ctx, item); err == nil {
        t.Fatalf("expected error when blob store GetOne fails")
    }
}


func Test_ItemActionTracker_Get_TTL_CacheHit_SetsValue(t *testing.T) {
    ctx := context.Background()
    so := sop.StoreOptions{
        Name:                      "iat_get_ttl_hit",
        SlotLength:                4,
        IsValueDataInNodeSegment:  false,
        IsValueDataGloballyCached: true,
        CacheConfig: &sop.StoreCacheConfig{
            IsValueDataCacheTTL:    true,
            ValueDataCacheDuration: time.Minute,
        },
    }
    si := sop.NewStoreInfo(so)
    tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
    tr := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, tl)

    // Prepare item and seed cache
    pk, pv := newPerson("tt", "l", "m", "e@x", "p")
    id := sop.NewUUID()
    it := &btree.Item[PersonKey, Person]{ID: id, Key: pk, Value: nil, ValueNeedsFetch: true}
    if err := mockRedisCache.SetStruct(ctx, formatItemKey(id.String()), &pv, time.Minute); err != nil {
        t.Fatalf("seed cache err: %v", err)
    }
    if err := tr.Get(ctx, it); err != nil {
        t.Fatalf("Get err: %v", err)
    }
    if it.Value == nil || it.ValueNeedsFetch {
        t.Fatalf("expected value set from cache and NeedsFetch=false")
    }
}

func Test_ItemActionTracker_Get_NonGlobal_FetchesFromBlob(t *testing.T) {
    ctx := context.Background()
    so := sop.StoreOptions{
        Name:                      "iat_get_blob",
        SlotLength:                4,
        IsValueDataInNodeSegment:  false,
        IsValueDataGloballyCached: false,
    }
    si := sop.NewStoreInfo(so)
    tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
    tr := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, tl)

    pk, pv := newPerson("nb", "l", "m", "e@x", "p")
    id := sop.NewUUID()
    it := &btree.Item[PersonKey, Person]{ID: id, Key: pk, Value: nil, ValueNeedsFetch: true}

    // Seed blob store with encoded value via tracker's Add path
    // Alternatively, marshal and call blob store directly
    // Use blob store directly for determinism
    ba, _ := encoding.BlobMarshaler.Marshal(pv)
    _ = mockNodeBlobStore.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: si.BlobTable, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: ba}}}})

    if err := tr.Get(ctx, it); err != nil {
        t.Fatalf("Get err: %v", err)
    }
    if it.Value == nil || it.ValueNeedsFetch {
        t.Fatalf("expected value set from blob and NeedsFetch=false")
    }
}
