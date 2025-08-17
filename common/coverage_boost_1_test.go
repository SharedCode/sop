package common

import (
	"context"
	"errors"
	"time"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/common/mocks"
)

// Covers addAction path in refetchAndMergeClosure when values are stored in a separate segment (AddItem).
func Test_RefetchAndMerge_AddItem_SeparateSegment_Succeeds(t *testing.T) {
	ctx := context.Background()
	so := sop.StoreOptions{Name: "rfm_add_sep", SlotLength: 4, IsValueDataInNodeSegment: false}
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
		t.Fatal(err)
	}

	// Seed a new item in tracker as addAction for AddItem path
	pk, pv := newPerson("ka", "va", "m", "a@b", "p")
	it := btree.Item[PersonKey, Person]{ID: sop.NewUUID(), Key: pk, Value: &pv}
	si.ItemActionTracker.(*itemActionTracker[PersonKey, Person]).items[it.ID] = cacheItem[PersonKey, Person]{
		lockRecord: lockRecord{LockID: sop.NewUUID(), Action: addAction},
		item:       &it,
		persisted:  false,
	}

	// Ensure StoreRepository returns the store on refresh
	_ = sr.Add(ctx, *ns)

	closure := refetchAndMergeClosure(&si, b3, sr)
	if err := closure(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Verify that the item is marked persisted and remains tracked
	ci, ok := si.ItemActionTracker.(*itemActionTracker[PersonKey, Person]).items[it.ID]
	if !ok || !ci.persisted {
		t.Fatalf("expected item to be persisted in tracker, ok=%v persisted=%v", ok, ci.persisted)
	}
}

// registry mock that makes Remove return an error
type failingRemoveRegistry struct{ *mocks.Mock_vid_registry }

func (f *failingRemoveRegistry) Remove(ctx context.Context, storesLids []sop.RegistryPayload[sop.UUID]) error {
	return errors.New("induced remove error")
}

// blob store mock that fails Remove to exercise rollbackNewRootNodes error path
type failingBlobStoreRemove struct{ sop.BlobStore }

func (f failingBlobStoreRemove) Remove(ctx context.Context, storesBlobsIDs []sop.BlobsPayload[sop.UUID]) error {
	return errors.New("induced blob remove error")
}

// Covers commitNewRootNodes early-return false when a non-empty root exists in registry.
func Test_CommitNewRootNodes_ReturnsFalse_OnExistingRoot(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	reg := mocks.NewMockRegistry(false)
	bs := mocks.NewMockBlobStore()
	tx := &Transaction{registry: reg, l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: bs, logger: newTransactionLogger(mocks.NewMockTransactionLog(), true)}
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "root_exists", SlotLength: 4})
	nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache()}

	// Prepare node and pre-populate registry with existing handle for same logical ID
	lid := sop.NewUUID()
	h := sop.NewHandle(lid) // LogicalID is set -> non-empty root
	if err := reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: si.RegistryTable, IDs: []sop.Handle{h}}}); err != nil {
		t.Fatalf("reg add err: %v", err)
	}
	n := &btree.Node[PersonKey, Person]{ID: lid, Version: 0}
	nodes := []sop.Tuple[*sop.StoreInfo, []interface{}]{{First: si, Second: []interface{}{n}}}

	ok, handles, err := nr.commitNewRootNodes(ctx, nodes)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ok || handles != nil {
		t.Fatalf("expected ok=false and nil handles when root exists, got ok=%v handles=%v", ok, handles)
	}
}

// Covers commitRemovedNodes returning false on conflicts (IsDeleted or version mismatch).
func Test_CommitRemovedNodes_Conflict_ReturnsFalse(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	reg := mocks.NewMockRegistry(false)
	bs := mocks.NewMockBlobStore()
	tx := &Transaction{registry: reg, l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: bs, logger: newTransactionLogger(mocks.NewMockTransactionLog(), true)}
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "rm_conflict", SlotLength: 4})
	nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache()}

	// Case 1: IsDeleted=true triggers false
	{
		lid := sop.NewUUID()
		h := sop.NewHandle(lid)
		h.IsDeleted = true
		if err := reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: si.RegistryTable, IDs: []sop.Handle{h}}}); err != nil {
			t.Fatalf("reg add err: %v", err)
		}
		n := &btree.Node[PersonKey, Person]{ID: lid, Version: 1}
		ok, _, err := nr.commitRemovedNodes(ctx, []sop.Tuple[*sop.StoreInfo, []interface{}]{{First: si, Second: []interface{}{n}}})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if ok {
			t.Fatalf("expected ok=false on IsDeleted conflict")
		}
	}
	// Case 2: Version mismatch triggers false
	{
		lid := sop.NewUUID()
		h := sop.NewHandle(lid)
		h.Version = 2 // backend newer than node's
		if err := reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: si.RegistryTable, IDs: []sop.Handle{h}}}); err != nil {
			t.Fatalf("reg add err: %v", err)
		}
		n := &btree.Node[PersonKey, Person]{ID: lid, Version: 1}
		ok, _, err := nr.commitRemovedNodes(ctx, []sop.Tuple[*sop.StoreInfo, []interface{}]{{First: si, Second: []interface{}{n}}})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if ok {
			t.Fatalf("expected ok=false on version mismatch")
		}
	}
}

// Covers rollbackNewRootNodes error flows from blobStore.Remove and registry.Remove
func Test_RollbackNewRootNodes_Error_Paths(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	baseReg := &mocks.Mock_vid_registry{Lookup: make(map[sop.UUID]sop.Handle)}
	reg := &failingRemoveRegistry{Mock_vid_registry: baseReg}
	bs := mocks.NewMockBlobStore()
	txLog := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	tx := &Transaction{registry: reg, l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: failingBlobStoreRemove{BlobStore: bs}, logger: txLog}
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "rb_root_errs", SlotLength: 4})
	nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache()}

	// Build rollback payloads
	lid := sop.NewUUID()
	vids := []sop.RegistryPayload[sop.UUID]{{RegistryTable: si.RegistryTable, IDs: []sop.UUID{lid}}}
	bibs := []sop.BlobsPayload[sop.UUID]{{BlobTable: si.BlobTable, Blobs: []sop.UUID{sop.NewUUID()}}}

	// When committedState > commitNewRootNodes, registry.Remove is invoked; here it will error
	txLog.committedState = deleteObsoleteEntries
	if err := nr.rollbackNewRootNodes(ctx, sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: vids, Second: bibs}); err == nil {
		t.Fatalf("expected rollbackNewRootNodes to return error from blob remove or registry remove")
	}
}

// Covers rollback handling of addActivelyPersistedItem (pre-commit log).
func Test_TransactionLogger_Rollback_PreCommit_AddActivelyPersistedItem(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	trLog := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	reg := mocks.NewMockRegistry(false)
	bs := mocks.NewMockBlobStore()
	sr := mocks.NewMockStoreRepository()
	tnx := &Transaction{l2Cache: l2, l1Cache: cache.GetGlobalCache(), registry: reg, blobStore: bs, logger: trLog, StoreRepository: sr}

	// Build a pre-commit log for addActivelyPersistedItem
	bibs := sop.BlobsPayload[sop.UUID]{BlobTable: "bt_pre", Blobs: []sop.UUID{sop.NewUUID()}}
	logs := []sop.KeyValuePair[int, []byte]{
		{Key: addActivelyPersistedItem, Value: toByteArray(bibs)},
		{Key: commitStoreInfo, Value: nil},
	}
	if err := trLog.rollback(ctx, tnx, trLog.transactionID, logs); err != nil {
		t.Fatalf("rollback err: %v", err)
	}
}

// errOnAddStoreRepo errors on Add and tracks Remove calls.
type errOnAddStoreRepo struct{ removed []string }

func (e *errOnAddStoreRepo) Add(ctx context.Context, stores ...sop.StoreInfo) error {
	return errors.New("add failed")
}
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
func (e *errOnAddStoreRepo) Replicate(ctx context.Context, storesInfo []sop.StoreInfo) error {
	return nil
}

// errOnGetStoreRepo errors on Get for OpenBtree path.
type errOnGetStoreRepo struct{ err error }

func (e *errOnGetStoreRepo) Add(ctx context.Context, stores ...sop.StoreInfo) error { return nil }
func (e *errOnGetStoreRepo) Update(ctx context.Context, stores []sop.StoreInfo) ([]sop.StoreInfo, error) {
	return nil, nil
}
func (e *errOnGetStoreRepo) Get(ctx context.Context, names ...string) ([]sop.StoreInfo, error) {
	return nil, e.err
}
func (e *errOnGetStoreRepo) GetAll(ctx context.Context) ([]string, error) { return nil, nil }
func (e *errOnGetStoreRepo) GetWithTTL(ctx context.Context, isTTL bool, d time.Duration, names ...string) ([]sop.StoreInfo, error) {
	return nil, e.err
}
func (e *errOnGetStoreRepo) Remove(ctx context.Context, names ...string) error { return nil }
func (e *errOnGetStoreRepo) Replicate(ctx context.Context, storesInfo []sop.StoreInfo) error {
	return nil
}

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
func (e errGetBlobStore) Remove(ctx context.Context, storesBlobsIDs []sop.BlobsPayload[sop.UUID]) error {
	return nil
}

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
	cmp := func(a, b int) int {
		if a < b {
			return -1
		} else if a > b {
			return 1
		}
		return 0
	}
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
	cmp := func(a, b int) int {
		if a < b {
			return -1
		} else if a > b {
			return 1
		}
		return 0
	}
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
			IsValueDataCacheTTL:    true,
			ValueDataCacheDuration: time.Minute,
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
