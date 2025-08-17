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

func Test_TransactionLogger_Rollback_CommitAddedRemovedUpdated_Paths(t *testing.T) {
	ctx := context.Background()
	// Wire a minimal transaction with nodeRepository backend needed by rollback methods.
	redis := mocks.NewMockClient()
	cache.NewGlobalCache(redis, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	bs := mocks.NewMockBlobStore()
	rg := mocks.NewMockRegistry(false)
	sr := mocks.NewMockStoreRepository()
	tx := &Transaction{l2Cache: redis, l1Cache: cache.GetGlobalCache(), blobStore: bs, registry: rg, StoreRepository: sr}
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "rb_paths", SlotLength: 4})
	nr := &nodeRepositoryBackend{
		transaction:    tx,
		storeInfo:      si,
		readNodesCache: cache.NewCache[sop.UUID, any](8, 12),
		localCache:     make(map[sop.UUID]cachedNode),
		l2Cache:        redis,
		l1Cache:        cache.GetGlobalCache(),
		count:          si.Count,
	}
	tx.btreesBackend = []btreeBackend{{nodeRepository: nr, getStoreInfo: func() *sop.StoreInfo { return si }}}

	// Prepare payloads for logs.
	// commitAddedNodes -> vids and bibs
	addLID := sop.NewUUID()
	vidsAdded := []sop.RegistryPayload[sop.UUID]{{RegistryTable: si.RegistryTable, BlobTable: si.BlobTable, IDs: []sop.UUID{addLID}}}
	bibsAdded := []sop.BlobsPayload[sop.UUID]{{BlobTable: si.BlobTable, Blobs: []sop.UUID{sop.NewUUID()}}}
	payloadAdded := toByteArray(sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: vidsAdded, Second: bibsAdded})

	// commitRemovedNodes -> vids; seed registry handles marked deleted to exercise rollback logic
	remLID := sop.NewUUID()
	vidsRemoved := []sop.RegistryPayload[sop.UUID]{{RegistryTable: si.RegistryTable, IDs: []sop.UUID{remLID}}}
	mr := rg.(*mocks.Mock_vid_registry)
	hRem := sop.NewHandle(remLID)
	hRem.IsDeleted = true
	mr.Lookup[remLID] = hRem
	payloadRemoved := toByteArray(vidsRemoved)

	// commitUpdatedNodes -> blobsIDs for removeNodes
	updBlobID := sop.NewUUID()
	blobsUpd := []sop.BlobsPayload[sop.UUID]{{BlobTable: si.BlobTable, Blobs: []sop.UUID{updBlobID}}}
	payloadUpdated := toByteArray(blobsUpd)

	// Build logs to trigger the three rollback branches; ensure lastCommittedFunctionLog is high.
	tid := sop.NewUUID()
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	logs := []sop.KeyValuePair[int, []byte]{
		{Key: commitAddedNodes, Value: payloadAdded},
		{Key: commitRemovedNodes, Value: payloadRemoved},
		{Key: commitUpdatedNodes, Value: payloadUpdated},
		{Key: commitStoreInfo, Value: toByteArray([]sop.StoreInfo{{Name: si.Name}})},
	}
	if err := tl.rollback(ctx, tx, tid, logs); err != nil {
		t.Fatalf("rollback err: %v", err)
	}
}

// --- Additional coverage for Phase2/rollback/priority paths ---

func Test_Phase2Commit_LogError_WithNodeLocks_PriorityRollbackPath(t *testing.T) {
	ctx := context.Background()
	pr := &recPrioLog{}
	tl := newTransactionLogger(errAddTL{pr: pr}, true)

	// Use mock caches and repos
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	bs := mocks.NewMockBlobStore()
	sr := mocks.NewMockStoreRepository()
	rg := mocks.NewMockRegistry(false)

	tx := &Transaction{mode: sop.ForWriting, logger: tl, l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: bs, StoreRepository: sr, registry: rg, id: sop.NewUUID()}
	// Simulate Phase1 done and pretend we hold node locks so error path uses priorityRollback
	tx.phaseDone = 1
	tx.nodesKeys = l2.CreateLockKeys([]string{"X", "Y"})

	// Provide minimal backend so rollback won't panic
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "p2_err_locks", SlotLength: 4})
	nr := &nodeRepositoryBackend{
		transaction:    tx,
		storeInfo:      si,
		readNodesCache: cache.NewCache[sop.UUID, any](8, 12),
		localCache:     make(map[sop.UUID]cachedNode),
		l2Cache:        l2,
		l1Cache:        cache.GetGlobalCache(),
		count:          si.Count,
	}
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
	}}

	if err := tx.Phase2Commit(ctx); err == nil {
		t.Fatalf("expected Phase2Commit error due to logger.Add failure")
	}
	// nodesKeys should be cleared by unlockNodesKeys on error path
	if tx.nodesKeys != nil {
		t.Fatalf("expected nodesKeys to be nil after error handling")
	}
	if pr.removed == 0 {
		t.Fatalf("expected priority rollback to remove log entry")
	}
}

func Test_TransactionLogger_PriorityRollback_NoLogs_RemovesTid(t *testing.T) {
	ctx := context.Background()
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	// With dummy priority log returning nil and no registry work, expect nil
	if err := tl.priorityRollback(ctx, &Transaction{registry: mocks.NewMockRegistry(false)}, sop.NewUUID()); err != nil {
		t.Fatalf("priorityRollback with no logs expected nil, got: %v", err)
	}
}

func Test_TransactionLogger_DoPriorityRollbacks_IsLockedFalse(t *testing.T) {
	ctx := context.Background()
	// Wrap cache so IsLocked on coordinator key returns false even after lock
	base := mocks.NewMockClient()
	c := isLockedFalseCache{Cache: base}
	tnx := &Transaction{l2Cache: c}
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	ok, err := tl.doPriorityRollbacks(ctx, tnx)
	if err != nil || ok {
		t.Fatalf("expected ok=false and err=nil when IsLocked=false; got ok=%v err=%v", ok, err)
	}
}

func Test_HandleRegistrySectorLockTimeout_UserDataNotLockKey_ReturnsOriginalErr(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	tx := &Transaction{l2Cache: l2, logger: newTransactionLogger(mocks.NewMockTransactionLog(), true)}
	se := sop.Error{Err: fmt.Errorf("sector lock timeout"), UserData: "not-a-lock"}
	if err := tx.handleRegistrySectorLockTimeout(ctx, se); err == nil || err.Error() != se.Error() {
		t.Fatalf("expected original error to be returned when UserData not lock key, got: %v", err)
	}
}

func Test_HandleRegistrySectorLockTimeout_LockNotAcquired_ReturnsOriginalErr(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	// Pre-acquire the coordination lock so Lock returns false
	_ = l2.Set(ctx, l2.FormatLockKey("DTrollbk"), sop.NewUUID().String(), time.Minute)
	tx := &Transaction{l2Cache: l2, logger: newTransactionLogger(mocks.NewMockTransactionLog(), true)}
	se := sop.Error{Err: fmt.Errorf("sector lock timeout"), UserData: &sop.LockKey{Key: l2.FormatLockKey("X")}}
	if err := tx.handleRegistrySectorLockTimeout(ctx, se); err == nil || err.Error() != se.Error() {
		t.Fatalf("expected original error to be returned when coordinator lock not acquired, got: %v", err)
	}
}

func Test_HandleRegistrySectorLockTimeout_IsLockedFalse_ReturnsOriginalErr(t *testing.T) {
	ctx := context.Background()
	// IsLocked always false
	base := mocks.NewMockClient()
	c := isLockedFalseCache{Cache: base}
	tx := &Transaction{l2Cache: c, logger: newTransactionLogger(mocks.NewMockTransactionLog(), true)}
	se := sop.Error{Err: fmt.Errorf("sector lock timeout"), UserData: &sop.LockKey{Key: c.FormatLockKey("X")}}
	if err := tx.handleRegistrySectorLockTimeout(ctx, se); err == nil || err.Error() != se.Error() {
		t.Fatalf("expected original error to be returned when IsLocked=false, got: %v", err)
	}
}

func Test_TransactionLogger_Rollback_CommitNewRootNodes_Path(t *testing.T) {
	ctx := context.Background()
	// Wire minimal tx/nr to satisfy rollback calls
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	bs := mocks.NewMockBlobStore()
	rg := mocks.NewMockRegistry(false)
	sr := mocks.NewMockStoreRepository()
	tx := &Transaction{l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: bs, registry: rg, StoreRepository: sr}
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "rb_newroot_branch", SlotLength: 4})
	nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache(), count: si.Count}
	tx.btreesBackend = []btreeBackend{{nodeRepository: nr, getStoreInfo: func() *sop.StoreInfo { return si }}}

	// Payload for commitNewRootNodes
	vids := []sop.RegistryPayload[sop.UUID]{{RegistryTable: si.RegistryTable, BlobTable: si.BlobTable, IDs: []sop.UUID{sop.NewUUID()}}}
	bibs := []sop.BlobsPayload[sop.UUID]{{BlobTable: si.BlobTable, Blobs: []sop.UUID{sop.NewUUID()}}}
	payload := toByteArray(sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: vids, Second: bibs})

	tid := sop.NewUUID()
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	tx.logger = tl
	logs := []sop.KeyValuePair[int, []byte]{
		{Key: commitNewRootNodes, Value: payload},
		{Key: commitStoreInfo, Value: toByteArray([]sop.StoreInfo{{Name: si.Name}})},
	}
	if err := tl.rollback(ctx, tx, tid, logs); err != nil {
		t.Fatalf("rollback(commitNewRootNodes) err: %v", err)
	}
}

func Test_Transaction_Cleanup_LogError_Propagated(t *testing.T) {
	ctx := context.Background()
	// Use a TL that fails on Add so cleanup's first log call fails
	tl := newTransactionLogger(errAddTL{pr: &recPrioLog{}}, true)
	l2 := mocks.NewMockClient()
	l1 := cache.NewL1Cache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	tx := &Transaction{l1Cache: l1, l2Cache: l2, registry: mocks.NewMockRegistry(false), blobStore: mocks.NewMockBlobStore(), logger: tl}
	if err := tx.cleanup(ctx); err == nil {
		t.Fatalf("expected cleanup to return error when logger.Add fails")
	}
}

// --- More phase1Commit and support branches ---

// prioLogRemoveFail simulates remove error so backup removal path is exercised in doPriorityRollbacks.
type prioLogRemoveFail struct {
	prioLogBatch
}

func (p *prioLogRemoveFail) Remove(ctx context.Context, tid sop.UUID) error {
	return fmt.Errorf("remove fail")
}

func Test_TransactionLogger_DoPriorityRollbacks_RemoveError_BackupRemoved(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	tid := sop.NewUUID()
	// Prepare batch with one entry; Remove will fail, so RemoveBackup should still be called
	base := &prioLogRemoveFail{prioLogBatch{tid: tid, batch: [][]sop.RegistryPayload[sop.Handle]{
		{{RegistryTable: "rt", IDs: []sop.Handle{sop.NewHandle(sop.NewUUID())}}},
	}}}
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	// Wrap TransactionLog to return our prio log behavior
	tl.TransactionLog = tlWithPL{inner: tl.TransactionLog.(*mocks.MockTransactionLog), pl: base}
	// Registry not used due to early remove error; just provide transaction with cache
	txn := &Transaction{l2Cache: l2, registry: mocks.NewMockRegistry(false)}
	ok, err := tl.doPriorityRollbacks(ctx, txn)
	if err != nil || !ok {
		t.Fatalf("expected ok=true err=nil; got ok=%v err=%v", ok, err)
	}
	if base.wrote == 0 || base.removedBk == 0 {
		t.Fatalf("expected backup write and backup remove on remove error")
	}
}

// Remove behavior coverage: when action is addAction, entry is deleted; when in readNodesCache, becomes removeAction
func Test_NodeRepository_Remove_ActionPaths(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	tx := &Transaction{l2Cache: l2, l1Cache: cache.GetGlobalCache()}
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "nr_remove", SlotLength: 4})
	nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache()}

	// Case 1: addAction -> delete from localCache
	idAdd := sop.NewUUID()
	nr.localCache[idAdd] = cachedNode{action: addAction, node: &btree.Node[PersonKey, Person]{ID: idAdd}}
	nr.remove(idAdd)
	if _, ok := nr.localCache[idAdd]; ok {
		t.Fatalf("expected localCache entry to be removed when action was addAction")
	}

	// Case 2: present in readNodesCache -> ultimately marked removeAction in localCache
	idR := sop.NewUUID()
	n := &btree.Node[PersonKey, Person]{ID: idR}
	nr.readNodesCache.Set([]sop.KeyValuePair[sop.UUID, any]{{Key: idR, Value: n}})
	nr.remove(idR)
	if v, ok := nr.localCache[idR]; !ok || v.action != removeAction {
		t.Fatalf("expected localCache action removeAction, got: %#v", v)
	}
	_ = ctx
}

// errOnceIsLockedCache returns error on first IsLocked then delegates to base.
type errOnceIsLockedCache struct {
	sop.Cache
	tripped bool
}

func (e *errOnceIsLockedCache) IsLocked(ctx context.Context, lockKeys []*sop.LockKey) (bool, error) {
	if !e.tripped {
		e.tripped = true
		return false, fmt.Errorf("islocked once err")
	}
	return e.Cache.IsLocked(ctx, lockKeys)
}

func Test_Phase1Commit_IsLockedError_Then_Succeeds(t *testing.T) {
	ctx := context.Background()
	base := mocks.NewMockClient()
	c := &errOnceIsLockedCache{Cache: base}
	cache.NewGlobalCache(c, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	rg := mocks.NewMockRegistry(false).(*mocks.Mock_vid_registry)
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	tx := &Transaction{mode: sop.ForWriting, maxTime: time.Minute, StoreRepository: mocks.NewMockStoreRepository(), registry: rg, l2Cache: c, l1Cache: cache.GetGlobalCache(), blobStore: mocks.NewMockBlobStore(), logger: tl, phaseDone: 0, id: sop.NewUUID()}

	si := sop.NewStoreInfo(sop.StoreOptions{Name: "p1_islocked_err", SlotLength: 4, IsValueDataInNodeSegment: true})
	nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: c, l1Cache: cache.GetGlobalCache(), count: si.Count}
	uid := sop.NewUUID()
	node := &btree.Node[PersonKey, Person]{ID: uid, Version: 1}
	nr.localCache[uid] = cachedNode{action: updateAction, node: node}
	// Registry with matching version so commitUpdatedNodes will succeed
	h := sop.NewHandle(uid)
	h.Version = 1
	rg.Lookup[uid] = h

	tx.btreesBackend = []btreeBackend{{
		nodeRepository:                   nr,
		getStoreInfo:                     func() *sop.StoreInfo { return si },
		hasTrackedItems:                  func() bool { return true },
		checkTrackedItems:                func(context.Context) error { return nil },
		lockTrackedItems:                 func(context.Context, time.Duration) error { return nil },
		unlockTrackedItems:               func(context.Context) error { return nil },
		commitTrackedItemsValues:         func(context.Context) error { return nil },
		getForRollbackTrackedItemsValues: func() *sop.BlobsPayload[sop.UUID] { return nil },
		getObsoleteTrackedItemsValues:    func() *sop.BlobsPayload[sop.UUID] { return nil },
	}}
	if err := tx.phase1Commit(ctx); err != nil {
		t.Fatalf("phase1Commit unexpected err after IsLocked error retry: %v", err)
	}
}

func Test_Phase1Commit_AreFetchedItemsIntact_False_TriggersRefetch(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	rg := mocks.NewMockRegistry(false).(*mocks.Mock_vid_registry)
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	tx := &Transaction{mode: sop.ForWriting, maxTime: time.Minute, StoreRepository: mocks.NewMockStoreRepository(), registry: rg, l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: mocks.NewMockBlobStore(), logger: tl, phaseDone: 0, id: sop.NewUUID()}

	si := sop.NewStoreInfo(sop.StoreOptions{Name: "p1_fetched_conflict", SlotLength: 4})
	nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache(), count: si.Count}
	// Updated node that will succeed
	uid := sop.NewUUID()
	un := &btree.Node[PersonKey, Person]{ID: uid, Version: 1}
	nr.localCache[uid] = cachedNode{action: updateAction, node: un}
	hh := sop.NewHandle(uid)
	hh.Version = 1
	rg.Lookup[uid] = hh
	// Fetched node with mismatched version to force areFetchedItemsIntact=false
	fid := sop.NewUUID()
	fn := &btree.Node[PersonKey, Person]{ID: fid, Version: 1}
	nr.localCache[fid] = cachedNode{action: getAction, node: fn}
	// Registry has different version
	fh := sop.NewHandle(fid)
	fh.Version = 2
	rg.Lookup[fid] = fh

	retried := false
	tx.btreesBackend = []btreeBackend{{
		nodeRepository:                   nr,
		getStoreInfo:                     func() *sop.StoreInfo { return si },
		hasTrackedItems:                  func() bool { return true },
		checkTrackedItems:                func(context.Context) error { return nil },
		lockTrackedItems:                 func(context.Context, time.Duration) error { return nil },
		unlockTrackedItems:               func(context.Context) error { return nil },
		commitTrackedItemsValues:         func(context.Context) error { return nil },
		getForRollbackTrackedItemsValues: func() *sop.BlobsPayload[sop.UUID] { return nil },
		getObsoleteTrackedItemsValues:    func() *sop.BlobsPayload[sop.UUID] { return nil },
		refetchAndMerge: func(ctx context.Context) error {
			if !retried { // align fetched version then
				fh := rg.Lookup[fid]
				fh.Version = 1
				rg.Lookup[fid] = fh
				retried = true
			}
			return nil
		},
	}}
	if err := tx.phase1Commit(ctx); err != nil {
		t.Fatalf("phase1Commit expected to succeed after fetched-items retry, err: %v", err)
	}
	if !retried {
		t.Fatalf("expected refetch to be invoked after areFetchedItemsIntact=false")
	}
}

// registryGetErrorOnce returns sop.Error on the first Get to trigger handleRegistrySectorLockTimeout,
// then delegates to the wrapped registry so the retry can succeed.
type registryGetErrorOnce struct {
	sop.Registry
	called bool
}

func (r *registryGetErrorOnce) Get(ctx context.Context, storesLids []sop.RegistryPayload[sop.UUID]) ([]sop.RegistryPayload[sop.Handle], error) {
	if !r.called {
		r.called = true
		return nil, sop.Error{Err: fmt.Errorf("get err"), UserData: &sop.LockKey{Key: "Lx"}}
	}
	return r.Registry.Get(ctx, storesLids)
}

func Test_Phase1Commit_RegistryGetError_HandledBySectorTimeout(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	// Compose registry that returns sop.Error once on Get then succeeds
	rg := &registryGetErrorOnce{Registry: mocks.NewMockRegistry(false)}
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	tx := &Transaction{mode: sop.ForWriting, maxTime: time.Minute, StoreRepository: mocks.NewMockStoreRepository(), registry: rg, l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: mocks.NewMockBlobStore(), logger: tl, phaseDone: 0, id: sop.NewUUID()}

	si := sop.NewStoreInfo(sop.StoreOptions{Name: "p1_regerr", SlotLength: 4})
	nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache(), count: si.Count}
	// Add a root node so commitNewRootNodes triggers a Get which returns sop.Error
	rid := sop.NewUUID()
	rn := &btree.Node[PersonKey, Person]{ID: rid, Version: 0}
	// Make it a root by matching store root ID
	si.RootNodeID = rid
	nr.localCache[rid] = cachedNode{action: addAction, node: rn}

	retried := false
	tx.btreesBackend = []btreeBackend{{
		nodeRepository:                   nr,
		getStoreInfo:                     func() *sop.StoreInfo { return si },
		hasTrackedItems:                  func() bool { return true },
		checkTrackedItems:                func(context.Context) error { return nil },
		lockTrackedItems:                 func(context.Context, time.Duration) error { return nil },
		unlockTrackedItems:               func(context.Context) error { return nil },
		commitTrackedItemsValues:         func(context.Context) error { return nil },
		getForRollbackTrackedItemsValues: func() *sop.BlobsPayload[sop.UUID] { return nil },
		getObsoleteTrackedItemsValues:    func() *sop.BlobsPayload[sop.UUID] { return nil },
		refetchAndMerge:                  func(ctx context.Context) error { retried = true; return nil },
	}}
	if err := tx.phase1Commit(ctx); err != nil {
		t.Fatalf("expected phase1Commit to succeed after sector-timeout handling and refetch, err: %v", err)
	}
	if !retried {
		t.Fatalf("expected refetch to run after sector-timeout handling")
	}
}

func Test_Phase1Commit_PreCommitLogs_CleanedUp(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	rg := mocks.NewMockRegistry(false).(*mocks.Mock_vid_registry)
	mtl := mocks.NewMockTransactionLog().(*mocks.MockTransactionLog)
	tl := newTransactionLogger(mtl, true)
	// Force pre-commit state
	tl.committedState = addActivelyPersistedItem
	preTid := tl.transactionID
	// Seed a pre-commit log under preTid so we can verify it's removed
	_ = mtl.Add(ctx, preTid, int(addActivelyPersistedItem), []byte("x"))
	tx := &Transaction{mode: sop.ForWriting, maxTime: time.Minute, StoreRepository: mocks.NewMockStoreRepository(), registry: rg, l2Cache: l2, l1Cache: cache.GetGlobalCache(), blobStore: mocks.NewMockBlobStore(), logger: tl, phaseDone: 0, id: sop.NewUUID()}

	si := sop.NewStoreInfo(sop.StoreOptions{Name: "p1_precommit_cleanup", SlotLength: 4})
	nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: l2, l1Cache: cache.GetGlobalCache(), count: si.Count}
	// Drive phase1 through normal path (with no actual node changes) so the pre-commit logs are cleaned up as implemented.
	tx.btreesBackend = []btreeBackend{{
		nodeRepository:                   nr,
		getStoreInfo:                     func() *sop.StoreInfo { return si },
		hasTrackedItems:                  func() bool { return true },
		checkTrackedItems:                func(context.Context) error { return nil },
		lockTrackedItems:                 func(context.Context, time.Duration) error { return nil },
		unlockTrackedItems:               func(context.Context) error { return nil },
		commitTrackedItemsValues:         func(context.Context) error { return nil },
		getForRollbackTrackedItemsValues: func() *sop.BlobsPayload[sop.UUID] { return nil },
		getObsoleteTrackedItemsValues:    func() *sop.BlobsPayload[sop.UUID] { return nil },
		refetchAndMerge:                  func(ctx context.Context) error { return nil },
	}}
	if err := tx.phase1Commit(ctx); err != nil {
		t.Fatalf("phase1Commit err: %v", err)
	}
	// Logs for preTid should be gone
	if logs := mtl.GetTIDLogs(preTid); len(logs) != 0 {
		t.Fatalf("expected pre-commit logs to be removed; still present: %v", logs)
	}
}
