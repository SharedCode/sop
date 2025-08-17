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
)

// Covers refetchAndMergeClosure update path for IsValueDataInNodeSegment=true.
func Test_RefetchAndMerge_Update_InNodeSegment_Succeeds(t *testing.T) {
	ctx := context.Background()
	// Build store with value data in node segment
	so := sop.StoreOptions{Name: "rfm_upd_node", SlotLength: 4, IsValueDataInNodeSegment: true}
	ns := sop.NewStoreInfo(so)
	si := StoreInterface[PersonKey, Person]{}
	sr := mocks.NewMockStoreRepository()
	_ = sr.Add(ctx, *ns)

	// Fresh global cache for determinism
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

	// Seed one item
	pk, pv := newPerson("iu1", "iv1", "m", "u@x", "p")
	if ok, err := b3.Add(ctx, pk, pv); !ok || err != nil {
		t.Fatalf("seed add err: %v", err)
	}
	if ok, _ := b3.Find(ctx, pk, false); !ok {
		t.Fatal("seed find err")
	}
	cur, _ := b3.GetCurrentItem(ctx)

	// Prepare tracker to simulate update with same version
	newV := pv
	newV.Phone = "changed"
	cur2 := cur // copy
	cur2.Value = &newV
	si.ItemActionTracker.(*itemActionTracker[PersonKey, Person]).items[cur.ID] = cacheItem[PersonKey, Person]{lockRecord: lockRecord{LockID: sop.NewUUID(), Action: updateAction}, item: &cur2, versionInDB: cur.Version}

	// Seed MRU and Handle for root so repo.get can fetch after reset
	rootID := b3.StoreInfo.RootNodeID
	if cn, ok := nrw.nodeRepositoryBackend.localCache[rootID]; ok && cn.node != nil {
		cache.GetGlobalCache().SetNodeToMRU(ctx, rootID, cn.node, ns.CacheConfig.NodeCacheDuration)
		cache.GetGlobalCache().Handles.Set([]sop.KeyValuePair[sop.UUID, sop.Handle]{{Key: rootID, Value: sop.NewHandle(rootID)}})
	} else {
		t.Fatalf("expected root node in local cache for MRU seed")
	}
	// Keep StoreRepository in sync so refetch picks current store info
	upd := *ns
	upd.RootNodeID = b3.StoreInfo.RootNodeID
	upd.Count = b3.StoreInfo.Count
	_ = sr.Add(ctx, upd)

	closure := refetchAndMergeClosure(&si, b3, sr)
	if err := closure(ctx); err != nil {
		t.Fatalf("refetchAndMerge update(in-node) err: %v", err)
	}
	if ok, _ := b3.Find(ctx, pk, false); !ok {
		t.Fatal("item missing after update")
	}
	got, _ := b3.GetCurrentItem(ctx)
	if got.Value == nil || got.Value.Phone != "changed" {
		t.Fatalf("expected updated value, got %+v", got.Value)
	}
}

// Covers refetchAndMergeClosure remove path for IsValueDataInNodeSegment=true when backend can refetch root.
func Test_RefetchAndMerge_Remove_InNodeSegment_Succeeds(t *testing.T) {
	ctx := context.Background()
	so := sop.StoreOptions{Name: "rfm_rm_node", SlotLength: 4, IsValueDataInNodeSegment: true}
	ns := sop.NewStoreInfo(so)
	si := StoreInterface[PersonKey, Person]{}
	sr := mocks.NewMockStoreRepository()
	_ = sr.Add(ctx, *ns)

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

	// Seed an item that resides in root
	pk, pv := newPerson("ru1", "rv1", "m", "r@x", "p")
	if ok, err := b3.Add(ctx, pk, pv); !ok || err != nil {
		t.Fatalf("seed add err: %v", err)
	}
	if ok, _ := b3.Find(ctx, pk, false); !ok {
		t.Fatal("seed find err")
	}
	cur, _ := b3.GetCurrentItem(ctx)

	// Track removal
	si.ItemActionTracker.(*itemActionTracker[PersonKey, Person]).items[cur.ID] = cacheItem[PersonKey, Person]{lockRecord: lockRecord{LockID: sop.NewUUID(), Action: removeAction}, item: &cur, versionInDB: cur.Version}

	// Seed MRU for root and handle
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
	if err := closure(ctx); err != nil {
		t.Fatalf("refetchAndMerge remove(in-node) err: %v", err)
	}
	// Expect item removed
	if ok, _ := b3.Find(ctx, pk, false); ok {
		t.Fatal("expected item removed after refetch+remove")
	}
}

// Covers commitUpdatedNodes branch when handle is deleted but expired -> cleared and new inactive allocated.
func Test_CommitUpdatedNodes_DeletedExpired_Allocates(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	reg := mocks.NewMockRegistry(false)
	bs := mocks.NewMockBlobStore()
	sr := mocks.NewMockStoreRepository()
	tx := &Transaction{registry: reg, l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: bs, logger: tl, StoreRepository: sr}
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "upd_expired", SlotLength: 4})
	nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache(), count: si.Count}

	// Prepare a handle marked deleted with expired inactive timestamp and existing active ID A only.
	lid := sop.NewUUID()
	h := sop.NewHandle(lid)
	h.IsDeleted = true
	h.WorkInProgressTimestamp = sop.Now().Add(-2 * time.Hour).UnixMilli()
	h.Version = 1
	_ = reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: si.RegistryTable, IDs: []sop.Handle{h}}})

	// Node to update with matching version
	n := &btree.Node[PersonKey, Person]{ID: lid, Version: 1}
	nodes := []sop.Tuple[*sop.StoreInfo, []interface{}]{
		{First: si, Second: []interface{}{n}},
	}

	ok, handles, err := nr.commitUpdatedNodes(ctx, nodes)
	if err != nil {
		t.Fatalf("commitUpdatedNodes err: %v", err)
	}
	if !ok || len(handles) == 0 || len(handles[0].IDs) != 1 {
		t.Fatalf("expected success and 1 handle, ok=%v handles=%v", ok, handles)
	}
	if handles[0].IDs[0].GetInActiveID().IsNil() {
		t.Fatalf("expected inactive ID allocated after clearing expired deleted state")
	}
}

// Covers Phase2Commit success path where unlockTrackedItems errors are tolerated (warn only).
func Test_Phase2Commit_Success_UnlockTrackedItems_Error_Ignored(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	bs := mocks.NewMockBlobStore()
	sr := mocks.NewMockStoreRepository()
	tx := &Transaction{mode: sop.ForWriting, phaseDone: 1, l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: bs, StoreRepository: sr, registry: mocks.NewMockRegistry(false), logger: newTransactionLogger(mocks.NewMockTransactionLog(), true)}
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "p2_unlock_warn", SlotLength: 4})
	nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache(), count: si.Count}
	tx.btreesBackend = []btreeBackend{{
		nodeRepository:                   nr,
		getStoreInfo:                     func() *sop.StoreInfo { return si },
		hasTrackedItems:                  func() bool { return false },
		checkTrackedItems:                func(context.Context) error { return nil },
		lockTrackedItems:                 func(context.Context, time.Duration) error { return nil },
		unlockTrackedItems:               func(context.Context) error { return errors.New("unlock err") },
		commitTrackedItemsValues:         func(context.Context) error { return nil },
		getForRollbackTrackedItemsValues: func() *sop.BlobsPayload[sop.UUID] { return nil },
		getObsoleteTrackedItemsValues:    func() *sop.BlobsPayload[sop.UUID] { return nil },
		refetchAndMerge:                  func(context.Context) error { return nil },
	}}
	// No updated/removed handles -> UpdateNoLocks block skipped; just ensure commit completes
	if err := tx.Phase2Commit(ctx); err != nil {
		t.Fatalf("Phase2Commit should ignore unlockTrackedItems error, got: %v", err)
	}
}

// Minimal priority log that records Remove calls.
type recPrioLog2 struct{ removed []string }

func (r *recPrioLog2) IsEnabled() bool                             { return true }
func (r *recPrioLog2) Add(context.Context, sop.UUID, []byte) error { return nil }
func (r *recPrioLog2) Remove(ctx context.Context, tid sop.UUID) error {
	r.removed = append(r.removed, tid.String())
	return nil
}
func (r *recPrioLog2) Get(context.Context, sop.UUID) ([]sop.RegistryPayload[sop.Handle], error) {
	// Return a non-empty payload so priorityRollback attempts UpdateNoLocks (which we induce to fail)
	// and does not remove the priority log.
	return []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rt", IDs: []sop.Handle{sop.NewHandle(sop.NewUUID())}}}, nil
}
func (r *recPrioLog2) GetBatch(context.Context, int) ([]sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]], error) {
	return nil, nil
}
func (r *recPrioLog2) LogCommitChanges(context.Context, []sop.StoreInfo, []sop.RegistryPayload[sop.Handle], []sop.RegistryPayload[sop.Handle], []sop.RegistryPayload[sop.Handle], []sop.RegistryPayload[sop.Handle]) error {
	return nil
}
func (r *recPrioLog2) WriteBackup(context.Context, sop.UUID, []byte) error { return nil }
func (r *recPrioLog2) RemoveBackup(context.Context, sop.UUID) error        { return nil }

// TransactionLog wrapper that returns our recPrioLog2.
type tlWithPL2 struct{ pl sop.TransactionPriorityLog }

func (l tlWithPL2) PriorityLog() sop.TransactionPriorityLog          { return l.pl }
func (l tlWithPL2) Add(context.Context, sop.UUID, int, []byte) error { return nil }
func (l tlWithPL2) Remove(context.Context, sop.UUID) error           { return nil }
func (l tlWithPL2) GetOne(context.Context) (sop.UUID, string, []sop.KeyValuePair[int, []byte], error) {
	return sop.NilUUID, "", nil, nil
}
func (l tlWithPL2) GetOneOfHour(context.Context, string) (sop.UUID, []sop.KeyValuePair[int, []byte], error) {
	return sop.NilUUID, nil, nil
}
func (l tlWithPL2) NewUUID() sop.UUID { return sop.NewUUID() }

// Registry stub that fails UpdateNoLocks to force phase2 error and priority rollback path.
type failingRegistry struct{}

func (f failingRegistry) Add(context.Context, []sop.RegistryPayload[sop.Handle]) error    { return nil }
func (f failingRegistry) Update(context.Context, []sop.RegistryPayload[sop.Handle]) error { return nil }
func (f failingRegistry) UpdateNoLocks(context.Context, bool, []sop.RegistryPayload[sop.Handle]) error {
	return fmt.Errorf("induced UpdateNoLocks error")
}
func (f failingRegistry) Get(context.Context, []sop.RegistryPayload[sop.UUID]) ([]sop.RegistryPayload[sop.Handle], error) {
	return nil, nil
}
func (f failingRegistry) Remove(context.Context, []sop.RegistryPayload[sop.UUID]) error { return nil }
func (f failingRegistry) Replicate(context.Context, []sop.RegistryPayload[sop.Handle], []sop.RegistryPayload[sop.Handle], []sop.RegistryPayload[sop.Handle], []sop.RegistryPayload[sop.Handle]) error {
	return nil
}

// Covers Phase2Commit error branch when nodes are locked: priorityRollback is invoked and nodesKeys get cleared.
func Test_Phase2Commit_Error_WithLockedNodes_PriorityRollbackPath(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	pr := &recPrioLog2{}
	tx := &Transaction{
		id:              sop.NewUUID(),
		mode:            sop.ForWriting,
		phaseDone:       1,
		l2Cache:         l2,
		l1Cache:         cache.GetGlobalCache(),
		blobStore:       mocks.NewMockBlobStore(),
		registry:        failingRegistry{},
		StoreRepository: mocks.NewMockStoreRepository(),
		logger:          newTransactionLogger(tlWithPL2{pl: pr}, true),
	}
	// Minimal backend to avoid rollback panics on btreesBackend[0]
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "p2_err_locked", SlotLength: 4})
	nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache(), count: si.Count}
	tx.btreesBackend = []btreeBackend{{
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
	// Mark nodes as "locked"
	tx.nodesKeys = tx.l2Cache.CreateLockKeys([]string{"n1"})
	// Set updated handles so phase2Commit reaches UpdateNoLocks and errors
	tx.updatedNodeHandles = []sop.RegistryPayload[sop.Handle]{{IDs: []sop.Handle{sop.NewHandle(sop.NewUUID())}}}

	if err := tx.Phase2Commit(ctx); err == nil {
		t.Fatalf("expected Phase2Commit error")
	}
	if tx.nodesKeys != nil {
		t.Fatalf("expected nodesKeys cleared by unlockNodesKeys")
	}
	// Rollback removes priority log even on locked path; expect at least one removal.
	if len(pr.removed) == 0 {
		t.Fatalf("expected priority log removed during rollback on locked path")
	}
}

// Covers Phase2Commit error branch when nodes are not locked: priority log Remove is called.
func Test_Phase2Commit_Error_WithoutLockedNodes_RemovesPriorityLog(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	pr := &recPrioLog2{}
	tx := &Transaction{
		id:              sop.NewUUID(),
		mode:            sop.ForWriting,
		phaseDone:       1,
		l2Cache:         l2,
		l1Cache:         cache.GetGlobalCache(),
		blobStore:       mocks.NewMockBlobStore(),
		registry:        failingRegistry{},
		StoreRepository: mocks.NewMockStoreRepository(),
		logger:          newTransactionLogger(tlWithPL2{pl: pr}, true),
	}
	// Minimal backend to avoid rollback panics on btreesBackend[0]
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "p2_err_unlocked", SlotLength: 4})
	nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache(), count: si.Count}
	tx.btreesBackend = []btreeBackend{{
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
	// nodesKeys is nil to simulate not locked
	tx.updatedNodeHandles = []sop.RegistryPayload[sop.Handle]{{IDs: []sop.Handle{sop.NewHandle(sop.NewUUID())}}}

	if err := tx.Phase2Commit(ctx); err == nil {
		t.Fatalf("expected Phase2Commit error")
	}
	if len(pr.removed) < 1 {
		t.Fatalf("expected priority log removed at least once, got %d", len(pr.removed))
	}
}

// Covers refetchAndMergeClosure getAction path.
func Test_RefetchAndMerge_GetAction_PassThrough(t *testing.T) {
	ctx := context.Background()
	so := sop.StoreOptions{Name: "rfm_get_node", SlotLength: 4, IsValueDataInNodeSegment: true}
	ns := sop.NewStoreInfo(so)
	si := StoreInterface[PersonKey, Person]{}
	sr := mocks.NewMockStoreRepository()
	_ = sr.Add(ctx, *ns)

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

	// Seed an item and then mark as getAction
	pk, pv := newPerson("gg1", "gv1", "m", "g@x", "p")
	if ok, err := b3.Add(ctx, pk, pv); !ok || err != nil {
		t.Fatalf("seed add err: %v", err)
	}
	if ok, _ := b3.Find(ctx, pk, false); !ok {
		t.Fatal("seed find err")
	}
	cur, _ := b3.GetCurrentItem(ctx)
	si.ItemActionTracker.(*itemActionTracker[PersonKey, Person]).items[cur.ID] = cacheItem[PersonKey, Person]{lockRecord: lockRecord{LockID: sop.NewUUID(), Action: getAction}, item: &cur, versionInDB: cur.Version}

	// Seed MRU and handle for root so get/find can refetch after reset
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
	if err := closure(ctx); err != nil {
		t.Fatalf("refetchAndMerge getAction err: %v", err)
	}
}
