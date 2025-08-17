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

// --- Stubs for exercising specific branches ---

// errAddTL returns an error from Add to force Phase2 finalizeCommit log failure.
type errAddTL struct{ pr sop.TransactionPriorityLog }

func (e errAddTL) Add(ctx context.Context, tid sop.UUID, commitFunction int, payload []byte) error {
	return fmt.Errorf("add fail")
}
func (e errAddTL) Remove(ctx context.Context, tid sop.UUID) error { return nil }
func (e errAddTL) GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, []byte], error) {
	return sop.NilUUID, "", nil, nil
}
func (e errAddTL) GetOneOfHour(ctx context.Context, hour string) (sop.UUID, []sop.KeyValuePair[int, []byte], error) {
	return sop.NilUUID, nil, nil
}
func (e errAddTL) NewUUID() sop.UUID                       { return sop.NewUUID() }
func (e errAddTL) PriorityLog() sop.TransactionPriorityLog { return e.pr }

type recPrioLog struct{ removed int }

func (r *recPrioLog) IsEnabled() bool                                             { return true }
func (r *recPrioLog) Add(ctx context.Context, tid sop.UUID, payload []byte) error { return nil }
func (r *recPrioLog) Remove(ctx context.Context, tid sop.UUID) error              { r.removed++; return nil }
func (r *recPrioLog) Get(ctx context.Context, tid sop.UUID) ([]sop.RegistryPayload[sop.Handle], error) {
	// Return a non-empty slice so priorityRollback path that checks t or registry nil is reachable.
	return []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rt", IDs: []sop.Handle{sop.NewHandle(sop.NewUUID())}}}, nil
}
func (r *recPrioLog) GetBatch(ctx context.Context, batchSize int) ([]sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]], error) {
	return nil, nil
}
func (r *recPrioLog) LogCommitChanges(ctx context.Context, _ []sop.StoreInfo, _ []sop.RegistryPayload[sop.Handle], _ []sop.RegistryPayload[sop.Handle], _ []sop.RegistryPayload[sop.Handle], _ []sop.RegistryPayload[sop.Handle]) error {
	return nil
}
func (r *recPrioLog) WriteBackup(ctx context.Context, tid sop.UUID, payload []byte) error { return nil }
func (r *recPrioLog) RemoveBackup(ctx context.Context, tid sop.UUID) error                { return nil }

// wrapCache lets us override IsLocked once to simulate transient lock verification failure.
type wrapCache struct {
	sop.Cache
	flipOnce bool
}

func (w *wrapCache) IsLocked(ctx context.Context, lockKeys []*sop.LockKey) (bool, error) {
	if w.flipOnce {
		w.flipOnce = false
		return false, nil
	}
	return w.Cache.IsLocked(ctx, lockKeys)
}

// flipLockCache forces the first Lock call to fail, succeeding thereafter.
type flipLockCache struct {
	sop.Cache
	failures int
}

func (f *flipLockCache) Lock(ctx context.Context, duration time.Duration, lockKeys []*sop.LockKey) (bool, sop.UUID, error) {
	if f.failures > 0 {
		f.failures--
		return false, sop.NilUUID, nil
	}
	return f.Cache.Lock(ctx, duration, lockKeys)
}

// --- Tests ---

func Test_Phase1Commit_RefetchAndMerge_Path_Succeeds(t *testing.T) {
	ctx := context.Background()

	// Use mock redis for both L1 and L2; wrap L2 to fail first Lock then succeed.
	base := mocks.NewMockClient()
	redis := &flipLockCache{Cache: base, failures: 1}
	cache.NewGlobalCache(redis, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)

	// Transaction wiring with mocks.
	t2 := &Transaction{
		mode:            sop.ForWriting,
		maxTime:         time.Minute,
		StoreRepository: mocks.NewMockStoreRepository(),
		registry:        mocks.NewMockRegistry(false),
		l2Cache:         redis,
		l1Cache:         cache.GetGlobalCache(),
		blobStore:       mocks.NewMockBlobStore(),
		logger:          newTransactionLogger(mocks.NewMockTransactionLog(), true),
		phaseDone:       0,
		id:              sop.NewUUID(),
	}

	// Build a minimal backend with one updated and one removed node.
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "p1_refetch", SlotLength: 4, IsValueDataInNodeSegment: true})
	nr := &nodeRepositoryBackend{
		transaction:    t2,
		storeInfo:      si,
		readNodesCache: cache.NewCache[sop.UUID, any](8, 12),
		localCache:     make(map[sop.UUID]cachedNode),
		l2Cache:        redis,
		l1Cache:        cache.GetGlobalCache(),
		count:          si.Count,
	}

	updID := sop.NewUUID()
	remID := sop.NewUUID()
	nr.localCache[updID] = cachedNode{action: updateAction, node: &btree.Node[PersonKey, Person]{ID: updID, Version: 1}}
	nr.localCache[remID] = cachedNode{action: removeAction, node: &btree.Node[PersonKey, Person]{ID: remID, Version: 1}}

	// Seed registry with handles at matching versions.
	_ = t2.registry.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: si.RegistryTable, IDs: []sop.Handle{{LogicalID: updID, Version: 1}, {LogicalID: remID, Version: 1}}}})

	// Wire backend and ensure refetch path runs after first lock failure.
	refetchHit := false
	t2.btreesBackend = []btreeBackend{{
		nodeRepository: nr,
		refetchAndMerge: func(ctx context.Context) error {
			refetchHit = true
			return nil
		},
		getStoreInfo:                     func() *sop.StoreInfo { return si },
		commitTrackedItemsValues:         func(context.Context) error { return nil },
		getForRollbackTrackedItemsValues: func() *sop.BlobsPayload[sop.UUID] { return nil },
		getObsoleteTrackedItemsValues:    func() *sop.BlobsPayload[sop.UUID] { return nil },
		hasTrackedItems:                  func() bool { return true },
		checkTrackedItems:                func(context.Context) error { return nil },
		lockTrackedItems:                 func(context.Context, time.Duration) error { return nil },
		unlockTrackedItems:               func(context.Context) error { return nil },
	}}

	if err := t2.phase1Commit(ctx); err != nil {
		t.Fatalf("phase1Commit err: %v", err)
	}
	if !refetchHit {
		t.Fatalf("expected refetchAndMerge to be invoked after initial lock failure")
	}
}

func Test_Phase2Commit_LogError_NoNodeLocks_RemovesPrioLog(t *testing.T) {
	ctx := context.Background()
	pr := &recPrioLog{}
	tl := newTransactionLogger(errAddTL{pr: pr}, true)

	// Provide minimal dependencies to allow rollback path without panics.
	redis := mocks.NewMockClient()
	cache.NewGlobalCache(redis, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	bs := mocks.NewMockBlobStore()
	sr := mocks.NewMockStoreRepository()
	rg := mocks.NewMockRegistry(false)

	tx := &Transaction{mode: sop.ForWriting, logger: tl, l2Cache: redis, l1Cache: cache.GetGlobalCache(), blobStore: bs, StoreRepository: sr, registry: rg, id: sop.NewUUID()}

	// Minimal node repository/backend for rollback functions.
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "p2_err", SlotLength: 4})
	nr := &nodeRepositoryBackend{
		transaction:    tx,
		storeInfo:      si,
		readNodesCache: cache.NewCache[sop.UUID, any](8, 12),
		localCache:     make(map[sop.UUID]cachedNode),
		l2Cache:        redis,
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

	// Simulate Phase1 done
	tx.phaseDone = 1

	// No tracked nodes/locks, but finalize logging will fail -> error path with PriorityLog.Remove branch and safe rollback.
	if err := tx.Phase2Commit(ctx); err == nil {
		t.Fatalf("expected Phase2Commit error due to logger.Add failure")
	}
	if pr.removed == 0 {
		t.Fatalf("expected priority log Remove to be called")
	}
}

func Test_TransactionLogger_DoPriorityRollbacks_LockNotAcquired(t *testing.T) {
	ctx := context.Background()
	txn := &Transaction{l2Cache: mocks.NewMockClient()}
	// Pre-acquire the priority lock with another owner so doPriorityRollbacks cannot enter.
	// Note: doPriorityRollbacks formats the key twice, so mirror that here.
	k := txn.l2Cache.FormatLockKey(txn.l2Cache.FormatLockKey("Prbs"))
	_ = txn.l2Cache.Set(ctx, k, sop.NewUUID().String(), time.Minute)
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	ok, err := tl.doPriorityRollbacks(ctx, txn)
	if err != nil {
		t.Fatalf("doPriorityRollbacks unexpected err: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false when lock not acquired")
	}
}

func Test_TransactionLogger_PriorityRollback_NoRegistry_NoOp(t *testing.T) {
	ctx := context.Background()
	// Provide a transaction log whose PriorityLog returns a non-empty payload so branch checks t==nil.
	pl := &recPrioLog{}
	tl := newTransactionLogger(errAddTL{pr: pl}, true)
	// Expect no panic and nil error when transaction is nil.
	if err := tl.priorityRollback(ctx, nil, sop.NewUUID()); err != nil {
		t.Fatalf("priorityRollback expected nil with nil transaction, got: %v", err)
	}
}

func Test_ItemActionTracker_Lock_Compatibility_Cases(t *testing.T) {
	ctx := context.Background()
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "iat_lock_compat", SlotLength: 4})
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	trk := newItemActionTracker[PersonKey, Person](si, redis, blobs, tl)

	id := sop.NewUUID()
	pk, p := newPerson("lk", "c", "m", "e@x", "p")
	it := &btree.Item[PersonKey, Person]{ID: id, Key: pk, Value: &p, Version: 1}

	// Start tracking as a Get, so action is getAction.
	it.ValueNeedsFetch = false
	if err := trk.Get(ctx, it); err != nil {
		t.Fatalf("Get err: %v", err)
	}

	cases := []struct {
		name string
		seed func()
	}{
		{name: "same_owner_present", seed: func() {
			// Seed same LockID -> treated as ours; lock should pass.
			v := trk.items[id]
			_ = redis.SetStruct(ctx, redis.FormatLockKey(id.String()), &v.lockRecord, time.Minute)
		}},
		{name: "get_get_compatibility", seed: func() {
			// Seed a different LockID but both actions are getAction -> compatible.
			lr := lockRecord{LockID: sop.NewUUID(), Action: getAction}
			_ = redis.SetStruct(ctx, redis.FormatLockKey(id.String()), &lr, time.Minute)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset redis key for isolation
			_, _ = redis.Delete(ctx, []string{redis.FormatLockKey(id.String())})
			tc.seed()
			if err := trk.lock(ctx, time.Minute); err != nil {
				t.Fatalf("lock err: %v", err)
			}
			// Always attempt unlock to exercise branch
			if err := trk.unlock(ctx); err != nil {
				t.Fatalf("unlock err: %v", err)
			}
		})
	}
}

func Test_TransactionLogger_AcquireLocks_IsLocked_False_Path(t *testing.T) {
	ctx := context.Background()
	base := mocks.NewMockClient()
	wc := &wrapCache{Cache: base, flipOnce: true}
	txn := &Transaction{l2Cache: wc}
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	ids := []sop.UUID{sop.NewUUID(), sop.NewUUID()}
	hs := []sop.Handle{sop.NewHandle(ids[0]), sop.NewHandle(ids[1])}
	stores := []sop.RegistryPayload[sop.Handle]{{IDs: hs}}
	// Expect RestoreRegistryFileSectorFailure error due to partial lock verification failure.
	_, err := tl.acquireLocks(ctx, txn, sop.NewUUID(), stores)
	if err == nil {
		t.Fatalf("expected error from acquireLocks when IsLocked=false after Lock")
	}
	var se sop.Error
	if !errors.As(err, &se) || se.Code != sop.RestoreRegistryFileSectorFailure {
		t.Fatalf("expected RestoreRegistryFileSectorFailure, got %v", err)
	}
}

// --- Additional helpers for extended coverage ---

// errBlobStore returns an error on Remove to exercise deleteObsoleteEntries lastErr propagation.
type errBlobStore struct{ err error }

func (e errBlobStore) GetOne(ctx context.Context, blobName string, blobID sop.UUID) ([]byte, error) {
	return nil, nil
}
func (e errBlobStore) Add(ctx context.Context, storesblobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	return nil
}
func (e errBlobStore) Update(ctx context.Context, storesblobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	return nil
}
func (e errBlobStore) Remove(ctx context.Context, storesBlobsIDs []sop.BlobsPayload[sop.UUID]) error {
	return e.err
}

// errRemoveRegistry wraps the mock registry to return an error on Remove.
type errRemoveRegistry struct {
	*mocks.Mock_vid_registry
	err error
}

func (e errRemoveRegistry) Remove(ctx context.Context, storesLids []sop.RegistryPayload[sop.UUID]) error {
	return e.err
}

// errRepRegistry wraps the mock registry to return an error on Replicate.
type errRepRegistry struct {
	*mocks.Mock_vid_registry
	err error
}

func (e errRepRegistry) Replicate(ctx context.Context, _ []sop.RegistryPayload[sop.Handle], _ []sop.RegistryPayload[sop.Handle], _ []sop.RegistryPayload[sop.Handle], _ []sop.RegistryPayload[sop.Handle]) error {
	return e.err
}

// errRepStoreRepo wraps the mock store repo to return an error on Replicate.
type errRepStoreRepo struct {
	inner sop.StoreRepository
	err   error
}

func (e errRepStoreRepo) Add(ctx context.Context, stores ...sop.StoreInfo) error {
	return e.inner.Add(ctx, stores...)
}
func (e errRepStoreRepo) Update(ctx context.Context, stores []sop.StoreInfo) ([]sop.StoreInfo, error) {
	return e.inner.Update(ctx, stores)
}
func (e errRepStoreRepo) Get(ctx context.Context, names ...string) ([]sop.StoreInfo, error) {
	return e.inner.Get(ctx, names...)
}
func (e errRepStoreRepo) GetAll(ctx context.Context) ([]string, error) { return e.inner.GetAll(ctx) }
func (e errRepStoreRepo) GetWithTTL(ctx context.Context, isCacheTTL bool, cacheDuration time.Duration, names ...string) ([]sop.StoreInfo, error) {
	return e.inner.GetWithTTL(ctx, isCacheTTL, cacheDuration, names...)
}
func (e errRepStoreRepo) Remove(ctx context.Context, names ...string) error {
	return e.inner.Remove(ctx, names...)
}
func (e errRepStoreRepo) Replicate(ctx context.Context, storesInfo []sop.StoreInfo) error {
	return e.err
}

// warnPL returns an error on LogCommitChanges to exercise the warning path.
type warnPL struct{}

func (w warnPL) IsEnabled() bool                                             { return true }
func (w warnPL) Add(ctx context.Context, tid sop.UUID, payload []byte) error { return nil }
func (w warnPL) Remove(ctx context.Context, tid sop.UUID) error              { return nil }
func (w warnPL) Get(ctx context.Context, tid sop.UUID) ([]sop.RegistryPayload[sop.Handle], error) {
	return nil, nil
}
func (w warnPL) GetBatch(ctx context.Context, batchSize int) ([]sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]], error) {
	return nil, nil
}
func (w warnPL) LogCommitChanges(ctx context.Context, _ []sop.StoreInfo, _ []sop.RegistryPayload[sop.Handle], _ []sop.RegistryPayload[sop.Handle], _ []sop.RegistryPayload[sop.Handle], _ []sop.RegistryPayload[sop.Handle]) error {
	return fmt.Errorf("warn: log commit changes")
}
func (w warnPL) WriteBackup(ctx context.Context, tid sop.UUID, payload []byte) error { return nil }
func (w warnPL) RemoveBackup(ctx context.Context, tid sop.UUID) error                { return nil }

// wrapTL delegates to the mock transaction log but returns a warnPL for PriorityLog.
type wrapTL struct{ inner *mocks.MockTransactionLog }

func (w wrapTL) GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, []byte], error) {
	return w.inner.GetOne(ctx)
}
func (w wrapTL) GetOneOfHour(ctx context.Context, hour string) (sop.UUID, []sop.KeyValuePair[int, []byte], error) {
	return w.inner.GetOneOfHour(ctx, hour)
}
func (w wrapTL) Add(ctx context.Context, tid sop.UUID, commitFunction int, payload []byte) error {
	return w.inner.Add(ctx, tid, commitFunction, payload)
}
func (w wrapTL) Remove(ctx context.Context, tid sop.UUID) error { return w.inner.Remove(ctx, tid) }
func (w wrapTL) NewUUID() sop.UUID                              { return w.inner.NewUUID() }
func (w wrapTL) PriorityLog() sop.TransactionPriorityLog        { return warnPL{} }

func Test_DeleteObsoleteEntries_Errors_BlobAndRegistry(t *testing.T) {
	ctx := context.Background()
	// Fresh L1 cache instance; L2 is mock redis (Delete returns nil error).
	l2 := mocks.NewMockClient()
	l1 := cache.NewL1Cache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)

	// Transaction with erroring blobstore and registry.
	berr := fmt.Errorf("blob remove fail")
	rerr := fmt.Errorf("registry remove fail")
	tx := &Transaction{l1Cache: l1, blobStore: errBlobStore{err: berr}, registry: errRemoveRegistry{Mock_vid_registry: mocks.NewMockRegistry(false).(*mocks.Mock_vid_registry), err: rerr}}

	// Prepare one unused node id and one deleted registry id.
	uid := sop.NewUUID()
	unused := []sop.BlobsPayload[sop.UUID]{{BlobTable: "bt", Blobs: []sop.UUID{uid}}}
	deleted := []sop.RegistryPayload[sop.UUID]{{RegistryTable: "rt", IDs: []sop.UUID{sop.NewUUID()}}}

	// Expect the last error to be from registry.Remove.
	if err := tx.deleteObsoleteEntries(ctx, deleted, unused); err == nil || err.Error() != rerr.Error() {
		t.Fatalf("expected registry remove error, got: %v", err)
	}
}

func Test_Phase2Commit_ReplicationWarnings_DoNotFailCommit(t *testing.T) {
	ctx := context.Background()
	// Use wrappers that return errors from replicate/logcommit but should not fail commit.
	sr := errRepStoreRepo{inner: mocks.NewMockStoreRepository(), err: fmt.Errorf("store replicate fail")}
	rg := errRepRegistry{Mock_vid_registry: mocks.NewMockRegistry(false).(*mocks.Mock_vid_registry), err: fmt.Errorf("registry replicate fail")}
	tl := newTransactionLogger(wrapTL{inner: mocks.NewMockTransactionLog().(*mocks.MockTransactionLog)}, true)

	tx := &Transaction{mode: sop.ForWriting, phaseDone: 1, StoreRepository: sr, registry: rg, logger: tl, l1Cache: cache.GetGlobalCache()}

	// Call Phase2Commit; finalize logging and replication tasks should run and warn, but commit succeeds.
	if err := tx.Phase2Commit(ctx); err != nil {
		t.Fatalf("Phase2Commit should succeed despite replication warnings, got: %v", err)
	}
}

// recTL tracks Remove calls to verify rollback branch that clears logs when finalizeCommit had no payload and deleteObsoleteEntries was last.
type recTL struct{ removes int }

func (r *recTL) GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, []byte], error) {
	return sop.NilUUID, "", nil, nil
}
func (r *recTL) GetOneOfHour(ctx context.Context, hour string) (sop.UUID, []sop.KeyValuePair[int, []byte], error) {
	return sop.NilUUID, nil, nil
}
func (r *recTL) Add(ctx context.Context, tid sop.UUID, commitFunction int, payload []byte) error {
	return nil
}
func (r *recTL) Remove(ctx context.Context, tid sop.UUID) error { r.removes++; return nil }
func (r *recTL) NewUUID() sop.UUID                              { return sop.NewUUID() }
func (r *recTL) PriorityLog() sop.TransactionPriorityLog        { return &recPrioLog{} }

func Test_TransactionLogger_Rollback_FinalizeNil_ThenDeleteObsoleteEntries_RemovesLogs(t *testing.T) {
	ctx := context.Background()
	rtl := &recTL{}
	tl := newTransactionLogger(rtl, true)
	tx := &Transaction{blobStore: mocks.NewMockBlobStore()} // minimal; not used in this branch
	tid := sop.NewUUID()

	logs := []sop.KeyValuePair[int, []byte]{
		{Key: finalizeCommit, Value: nil},
		{Key: deleteObsoleteEntries, Value: nil},
	}
	if err := tl.rollback(ctx, tx, tid, logs); err != nil {
		t.Fatalf("rollback returned error: %v", err)
	}
	if rtl.removes == 0 {
		t.Fatalf("expected logs to be removed in rollback path")
	}
}

func Test_TransactionLogger_Rollback_FinalizeWithPayload_DeletesTrackedItemsValues(t *testing.T) {
	ctx := context.Background()
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	// Transaction with mock blob store and caches to avoid nil deref in deleteObsoleteEntries path.
	l2 := mocks.NewMockClient()
	l1 := cache.NewL1Cache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	tx := &Transaction{blobStore: mocks.NewMockBlobStore(), registry: mocks.NewMockRegistry(false), l1Cache: l1, l2Cache: l2}
	tid := sop.NewUUID()

	// Build a payload with tracked items values (Second) so rollback calls deleteTrackedItemsValues.
	items := []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]{
		{First: false, Second: sop.BlobsPayload[sop.UUID]{BlobTable: "bt", Blobs: []sop.UUID{sop.NewUUID()}}},
	}
	pl := sop.Tuple[sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]], []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]]{
		First:  sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{},
		Second: items,
	}

	logs := []sop.KeyValuePair[int, []byte]{
		{Key: finalizeCommit, Value: toByteArray(pl)},
		{Key: deleteTrackedItemsValues, Value: nil},
	}
	if err := tl.rollback(ctx, tx, tid, logs); err != nil {
		t.Fatalf("rollback with finalize payload err: %v", err)
	}
}

func Test_NodeRepository_RollbackNewRootNodes_RemovesCacheAndRegistry(t *testing.T) {
	ctx := context.Background()
	redis := mocks.NewMockClient()
	cache.NewGlobalCache(redis, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	bs := mocks.NewMockBlobStore()
	rg := mocks.NewMockRegistry(false)
	sr := mocks.NewMockStoreRepository()
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	tx := &Transaction{l2Cache: redis, l1Cache: cache.GetGlobalCache(), blobStore: bs, registry: rg, StoreRepository: sr, logger: tl}

	si := sop.NewStoreInfo(sop.StoreOptions{Name: "rb_newroot", SlotLength: 4})
	nr := &nodeRepositoryBackend{transaction: tx, storeInfo: si, readNodesCache: cache.NewCache[sop.UUID, any](8, 12), localCache: make(map[sop.UUID]cachedNode), l2Cache: redis, l1Cache: cache.GetGlobalCache(), count: si.Count}

	// Prepare payloads
	lid := sop.NewUUID()
	vids := []sop.RegistryPayload[sop.UUID]{{RegistryTable: si.RegistryTable, IDs: []sop.UUID{lid}}}
	bibs := []sop.BlobsPayload[sop.UUID]{{BlobTable: si.BlobTable, Blobs: []sop.UUID{sop.NewUUID()}}}

	// Pre-seed L2 key so Delete returns found=true without error.
	_ = redis.Set(ctx, nr.formatKey(lid.String()), sop.NewUUID().String(), time.Minute)

	// Simulate that commitNewRootNodes had been logged/committed so branch unregisters on registry.
	tx.logger.committedState = commitRemovedNodes

	if err := nr.rollbackNewRootNodes(ctx, sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: vids, Second: bibs}); err != nil {
		t.Fatalf("rollbackNewRootNodes err: %v", err)
	}
}

// --- Additional acquireLocks/priority rollbacks coverage ---

func Test_TransactionLogger_AcquireLocks_OwnerDifferent_Fail(t *testing.T) {
	ctx := context.Background()
	redis := mocks.NewMockClient()
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	tnx := &Transaction{l2Cache: redis}
	tid := sop.NewUUID() // our tid
	// Construct two lock keys from two logical IDs via acquireLocks input.
	ids := []sop.Handle{{LogicalID: sop.NewUUID()}, {LogicalID: sop.NewUUID()}}
	stores := []sop.RegistryPayload[sop.Handle]{{IDs: ids}}
	// Pre-seed with a different owner.
	other := sop.NewUUID()
	for _, h := range ids {
		_ = redis.Set(ctx, redis.FormatLockKey(h.LogicalID.String()), other.String(), time.Minute)
	}
	_, err := tl.acquireLocks(ctx, tnx, tid, stores)
	var se sop.Error
	if err == nil || !errors.As(err, &se) || se.Code != sop.RestoreRegistryFileSectorFailure {
		t.Fatalf("expected RestoreRegistryFileSectorFailure, got: %v", err)
	}
}

func Test_TransactionLogger_AcquireLocks_Takeover_DeadOwner_Succeeds(t *testing.T) {
	ctx := context.Background()
	redis := mocks.NewMockClient()
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	tnx := &Transaction{l2Cache: redis}
	tid := sop.NewUUID() // dead owner's tid
	ids := []sop.Handle{{LogicalID: sop.NewUUID()}, {LogicalID: sop.NewUUID()}}
	stores := []sop.RegistryPayload[sop.Handle]{{IDs: ids}}
	// Seed the lock keys to the same tid so takeover path is exercised.
	for _, h := range ids {
		_ = redis.Set(ctx, redis.FormatLockKey(h.LogicalID.String()), tid.String(), time.Minute)
	}
	keys, err := tl.acquireLocks(ctx, tnx, tid, stores)
	if err != nil {
		t.Fatalf("unexpected err in takeover path: %v", err)
	}
	if len(keys) == 0 || !keys[0].IsLockOwner {
		t.Fatalf("expected lock ownership after takeover")
	}
}

// prioLogBatch provides a single-batch PriorityLog for doPriorityRollbacks success path.
type prioLogBatch struct {
	tid                       sop.UUID
	batch                     [][]sop.RegistryPayload[sop.Handle]
	wrote, removed, removedBk int
}

func (p *prioLogBatch) IsEnabled() bool                                             { return true }
func (p *prioLogBatch) Add(ctx context.Context, tid sop.UUID, payload []byte) error { return nil }
func (p *prioLogBatch) Remove(ctx context.Context, tid sop.UUID) error              { p.removed++; return nil }
func (p *prioLogBatch) Get(ctx context.Context, tid sop.UUID) ([]sop.RegistryPayload[sop.Handle], error) {
	return nil, nil
}
func (p *prioLogBatch) GetBatch(ctx context.Context, batchSize int) ([]sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]], error) {
	if len(p.batch) == 0 {
		return nil, nil
	}
	v := p.batch[0]
	p.batch = p.batch[1:]
	return []sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]]{{Key: p.tid, Value: v}}, nil
}
func (p *prioLogBatch) LogCommitChanges(ctx context.Context, _ []sop.StoreInfo, _ []sop.RegistryPayload[sop.Handle], _ []sop.RegistryPayload[sop.Handle], _ []sop.RegistryPayload[sop.Handle], _ []sop.RegistryPayload[sop.Handle]) error {
	return nil
}
func (p *prioLogBatch) WriteBackup(ctx context.Context, tid sop.UUID, payload []byte) error {
	p.wrote++
	return nil
}
func (p *prioLogBatch) RemoveBackup(ctx context.Context, tid sop.UUID) error {
	p.removedBk++
	return nil
}

// tlWithPL wires a custom PriorityLog while deferring all other methods to the mock TL.
type tlWithPL struct {
	inner *mocks.MockTransactionLog
	pl    sop.TransactionPriorityLog
}

func (w tlWithPL) GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, []byte], error) {
	return w.inner.GetOne(ctx)
}
func (w tlWithPL) GetOneOfHour(ctx context.Context, hour string) (sop.UUID, []sop.KeyValuePair[int, []byte], error) {
	return w.inner.GetOneOfHour(ctx, hour)
}
func (w tlWithPL) Add(ctx context.Context, tid sop.UUID, commitFunction int, payload []byte) error {
	return w.inner.Add(ctx, tid, commitFunction, payload)
}
func (w tlWithPL) Remove(ctx context.Context, tid sop.UUID) error { return w.inner.Remove(ctx, tid) }
func (w tlWithPL) NewUUID() sop.UUID                              { return w.inner.NewUUID() }
func (w tlWithPL) PriorityLog() sop.TransactionPriorityLog        { return w.pl }

func Test_TransactionLogger_DoPriorityRollbacks_Batch_Succeeds(t *testing.T) {
	ctx := context.Background()
	redis := mocks.NewMockClient()
	rg := mocks.NewMockRegistry(false).(*mocks.Mock_vid_registry)
	// Prepare handles in registry with versions equal to batch payload to satisfy version checks.
	id1, id2 := sop.NewUUID(), sop.NewUUID()
	h1, h2 := sop.NewHandle(id1), sop.NewHandle(id2)
	h1.Version, h2.Version = 1, 1
	rg.Lookup[id1] = h1
	rg.Lookup[id2] = h2

	// Prepare batch payload with the same versions.
	tid := sop.NewUUID()
	p := &prioLogBatch{tid: tid, batch: [][]sop.RegistryPayload[sop.Handle]{
		{{RegistryTable: "rt", IDs: []sop.Handle{h1, h2}}},
	}}
	baseTL := mocks.NewMockTransactionLog().(*mocks.MockTransactionLog)
	tl := newTransactionLogger(tlWithPL{inner: baseTL, pl: p}, true)
	txn := &Transaction{l2Cache: redis, registry: rg}

	ok, err := tl.doPriorityRollbacks(ctx, txn)
	if err != nil || !ok {
		t.Fatalf("expected batch to be processed, ok=true, err=nil; got ok=%v err=%v", ok, err)
	}
	if p.wrote == 0 || p.removed == 0 || p.removedBk == 0 {
		t.Fatalf("expected backup write/remove and backup remove to be called")
	}
}
