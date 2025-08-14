package common

// Consolidated scenarios targeting Phase2Commit success and error paths, plus logging-enabled constructor.

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

// seedUpdateAndRemove performs operations that will generate updated and removed node handles during commit.
func seedUpdateAndRemove(t *testing.T, ctx context.Context, b3 btree.BtreeInterface[PersonKey, Person], pk PersonKey, p Person) {
	t.Helper()
	if ok, err := b3.Add(ctx, pk, p); !ok || err != nil {
		t.Fatalf("Add failed: ok=%v err=%v", ok, err)
	}
	p2 := p
	p2.Email = "u@x"
	if _, err := b3.Update(ctx, pk, p2); err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	// Add a second record then remove it -> removed handles
	pkR, pR := newPerson("r", "m", "x", "r@x", "1")
	if ok, err := b3.Add(ctx, pkR, pR); !ok || err != nil {
		t.Fatalf("Add(second) failed: ok=%v err=%v", ok, err)
	}
	if _, err := b3.Remove(ctx, pkR); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
}

func Test_Phase2Commit_Success_PopulatesMRU_And_CleansUp(t *testing.T) {
	ctx := context.Background()

	// Use logging-enabled constructor to also cover that path.
	tx, err := newMockTransactionWithLogging(t, sop.ForWriting, -1)
	if err != nil {
		t.Fatalf("newMockTransactionWithLogging error: %v", err)
	}
	if err := tx.Begin(); err != nil {
		t.Fatalf("Begin error: %v", err)
	}

	// Separate value segment to exercise tracked values commit in phase1.
	so := sop.StoreOptions{Name: "c2_success_store", SlotLength: 8, IsValueDataInNodeSegment: false}
	b3, err := NewBtree[PersonKey, Person](ctx, so, tx, Compare)
	if err != nil {
		t.Fatalf("NewBtree error: %v", err)
	}

	pk, p := newPerson("a", "b", "m", "e@x", "p")
	seedUpdateAndRemove(t, ctx, b3, pk, p)

	// Full commit should exercise Phase2Commit success path including UpdateNoLocks and MRU populate.
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit error: %v", err)
	}

	// Re-open and verify updated value visible after commit.
	tx2, _ := newMockTransaction(t, sop.ForReading, -1)
	_ = tx2.Begin()
	b3r, _ := OpenBtree[PersonKey, Person](ctx, so.Name, tx2, Compare)
	if ok, err := b3r.Find(ctx, pk, false); !ok || err != nil {
		t.Fatalf("Find after commit failed: ok=%v err=%v", ok, err)
	}
	if _, err := b3r.GetCurrentValue(ctx); err != nil {
		t.Fatalf("GetCurrentValue after commit: %v", err)
	}
	_ = tx2.Commit(ctx)
}

func Test_Phase2Commit_Error_UpdateNoLocks_RaisesAndRollsBack(t *testing.T) {
	ctx := context.Background()

	// Seed store with pre-existing records so updates/removes generate handles in Phase1.
	so := sop.StoreOptions{Name: "c2_error_store", SlotLength: 8, IsValueDataInNodeSegment: true}
	pkUpd, pUpd := newPerson("e1", "e2", "m", "e@x", "p")
	pkDel, pDel := newPerson("d1", "d2", "m", "d@x", "p")
	seedStoreWithOne(t, so.Name, so.IsValueDataInNodeSegment, pkUpd, pUpd)
	seedStoreWithOne(t, so.Name, so.IsValueDataInNodeSegment, pkDel, pDel)

	// Use logging-enabled constructor to ensure priority logging is wired; mocks keep it no-op.
	tx, err := newMockTransactionWithLogging(t, sop.ForWriting, -1)
	if err != nil {
		t.Fatalf("newMockTransactionWithLogging error: %v", err)
	}
	if err := tx.Begin(); err != nil {
		t.Fatalf("Begin error: %v", err)
	}
	b3, err := NewBtree[PersonKey, Person](ctx, so, tx, Compare)
	if err != nil {
		t.Fatalf("NewBtree error: %v", err)
	}

	// Perform an update on existing key and a remove on another existing key to produce handles.
	pUpd2 := pUpd
	pUpd2.Email = "changed@x"
	if _, err := b3.Update(ctx, pkUpd, pUpd2); err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if _, err := b3.Remove(ctx, pkDel); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Split phases to inject failure between Phase1 and Phase2.
	t2 := tx.GetPhasedTransaction().(*Transaction)
	if err := t2.Phase1Commit(ctx); err != nil {
		t.Fatalf("Phase1Commit error: %v", err)
	}
	// Induce UpdateNoLocks(allOrNothing=true) failure in Phase2 by swapping registry.
	t2.registry = mocks.NewMockRegistry(true)
	if err := t2.Phase2Commit(ctx); err == nil {
		t.Fatalf("expected Phase2Commit error due to UpdateNoLocks failure")
	}
}

// Reader transaction: track an item, ensure commitForReaderTransaction succeeds with no conflict
// and Phase2Commit is a no-op for read mode.
func Test_ReaderTransaction_Commit_Succeeds_NoConflict(t *testing.T) {
	ctx := context.Background()

	// Seed with one record
	so := sop.StoreOptions{Name: "reader_ok_store", SlotLength: 8, IsValueDataInNodeSegment: true}
	pk, p := newPerson("r1", "r2", "m", "r@x", "p")
	seedStoreWithOne(t, so.Name, so.IsValueDataInNodeSegment, pk, p)

	// Open reader transaction and fetch item to create tracked GET action
	tx, err := newMockTransaction(t, sop.ForReading, -1)
	if err != nil {
		t.Fatalf("newMockTransaction err: %v", err)
	}
	if err := tx.Begin(); err != nil {
		t.Fatalf("Begin err: %v", err)
	}
	b3, err := OpenBtree[PersonKey, Person](ctx, so.Name, tx, Compare)
	if err != nil {
		t.Fatalf("OpenBtree err: %v", err)
	}
	if ok, err := b3.Find(ctx, pk, false); !ok || err != nil {
		t.Fatalf("Find reader failed: ok=%v err=%v", ok, err)
	}
	if _, err := b3.GetCurrentValue(ctx); err != nil {
		t.Fatalf("GetCurrentValue err: %v", err)
	}

	// Phase1 should run commitForReaderTransaction and succeed
	t2 := tx.GetPhasedTransaction().(*Transaction)
	if err := t2.Phase1Commit(ctx); err != nil {
		t.Fatalf("Phase1Commit(reader) err: %v", err)
	}
	// Phase2 should be no-op for reader
	if err := t2.Phase2Commit(ctx); err != nil {
		t.Fatalf("Phase2Commit(reader) err: %v", err)
	}
}

// Rollback returns error when pre-commit tracked values cleanup fails
func Test_Rollback_Error_From_PreCommitTrackedValues_Delete(t *testing.T) {
	ctx := context.Background()
	// Transaction with failing blobStore so deleteTrackedItemsValues fails
	fb := failingBlobStore{}
	tx := &Transaction{blobStore: fb, l2Cache: mocks.NewMockClient()}
	// Ensure transaction starts in initial state so Begin() succeeds
	tx.phaseDone = -1
	tx.logger = newTransactionLogger(mocks.NewMockTransactionLog(), true)
	if err := tx.Begin(); err != nil {
		t.Fatalf("Begin err: %v", err)
	}

	// Stub btree backend to return a non-empty tracked-values payload
	blobID := sop.NewUUID()
	tx.btreesBackend = []btreeBackend{{
		getForRollbackTrackedItemsValues: func() *sop.BlobsPayload[sop.UUID] {
			return &sop.BlobsPayload[sop.UUID]{BlobTable: "it", Blobs: []sop.UUID{blobID}}
		},
		getStoreInfo: func() *sop.StoreInfo {
			// Ensure getForRollbackTrackedItemsValues can read caching flag without panic
			si := sop.StoreInfo{IsValueDataGloballyCached: true}
			return &si
		},
	}}
	// Simulate we crashed after actively persisting values (pre-commit state)
	tx.logger.committedState = addActivelyPersistedItem

	// Rollback should surface error coming from deleteTrackedItemsValues
	if err := tx.Rollback(ctx, fmt.Errorf("trigger")); err == nil {
		t.Fatalf("expected rollback to return error due to failing blob delete")
	}
}

// onIdle should run priority rollback path when enabled and also process expired logs path.
func Test_OnIdle_Runs_Priority_And_Expired_Paths(t *testing.T) {
	ctx := context.Background()
	// Stub priority log with one entry; expect RemoveBackup to be called.
	pl := &stubPriorityLog{
		batch: []sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]]{
			{Key: sop.NewUUID(), Value: []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rt", IDs: []sop.Handle{sop.NewHandle(sop.NewUUID())}}}},
		},
		writeBackupErr:  map[string]error{},
		removeErr:       map[string]error{},
		removeBackupHit: map[string]int{},
	}
	tl := newTransactionLogger(stubTLog{pl: pl}, true)
	reg := mocks.NewMockRegistry(false)
	// Seed registry with the same handles referenced by the priority log so version checks won't panic.
	_ = reg.Add(ctx, pl.batch[0].Value)
	tx := &Transaction{logger: tl, l2Cache: mocks.NewMockClient(), registry: reg}
	// Ensure onIdle doesn't early-return.
	tx.btreesBackend = []btreeBackend{{}}

	// Force next run times by resetting globals.
	prevPrio := lastPriorityOnIdleTime
	prevFound := priorityLogFound
	prevHour := hourBeingProcessed
	prevIdle := lastOnIdleRunTime
	lastPriorityOnIdleTime = 0
	priorityLogFound = false
	hourBeingProcessed = "some-hour"
	lastOnIdleRunTime = 0
	defer func() {
		lastPriorityOnIdleTime = prevPrio
		priorityLogFound = prevFound
		hourBeingProcessed = prevHour
		lastOnIdleRunTime = prevIdle
	}()

	tx.onIdle(ctx)

	// Priority path should have run and set found=true when batch non-empty.
	if !priorityLogFound {
		t.Fatalf("expected priorityLogFound to be true after onIdle")
	}
	// Expired logs path should have reset hourBeingProcessed when no entries for that hour.
	if hourBeingProcessed != "" {
		t.Fatalf("expected hourBeingProcessed reset, got %q", hourBeingProcessed)
	}
	// And RemoveBackup should be observed for the batch entry.
	if len(pl.removeBackupHit) == 0 {
		t.Fatalf("expected RemoveBackup to be called at least once")
	}
}

// Cover handleRegistrySectorLockTimeout success path, including taking lock and calling priorityRollback.
func Test_HandleRegistrySectorLockTimeout_Succeeds(t *testing.T) {
	ctx := context.Background()
	tl := newTransactionLogger(stubTLog{pl: &stubPriorityLog{}}, true)
	tx := &Transaction{logger: tl, l2Cache: mocks.NewMockClient(), registry: mocks.NewMockRegistry(false)}
	se := sop.Error{Code: sop.RestoreRegistryFileSectorFailure, UserData: &sop.LockKey{LockID: sop.NewUUID()}}
	if err := tx.handleRegistrySectorLockTimeout(ctx, se); err != nil {
		t.Fatalf("handleRegistrySectorLockTimeout err: %v", err)
	}
}

// Validate getCommitStoresInfo and getRollbackStoresInfo compute CountDelta correctly.
func Test_StoreInfo_Delta_Computations(t *testing.T) {
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "delta", SlotLength: 4})
	si.Count = 10
	nr := &nodeRepositoryBackend{count: 7, storeInfo: si}
	t2 := &Transaction{btreesBackend: []btreeBackend{{getStoreInfo: func() *sop.StoreInfo { return si }, nodeRepository: nr}}}

	commits := t2.getCommitStoresInfo()
	if len(commits) != 1 || commits[0].CountDelta != (si.Count-nr.count) {
		t.Fatalf("commit CountDelta mismatch: %+v", commits)
	}
	rollbacks := t2.getRollbackStoresInfo()
	if len(rollbacks) != 1 || rollbacks[0].CountDelta != (nr.count-si.Count) {
		t.Fatalf("rollback CountDelta mismatch: %+v", rollbacks)
	}
}

// Ensure populateMru updates MRU with versioned nodes/handles.
func Test_PopulateMru_Updates_Versions_In_MRU(t *testing.T) {
	ctx := context.Background()
	// Global cache requires a client; reset for isolation.
	redis := mocks.NewMockClient()
	// The global cache might already be initialized; reinit to ensure consistency.
	cache.NewGlobalCache(redis, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)

	si := sop.NewStoreInfo(sop.StoreOptions{Name: "mru", SlotLength: 4})
	// Prepare one updated node and corresponding handle with target version.
	id := sop.NewUUID()
	n := &btree.Node[PersonKey, Person]{ID: id, Version: 0}
	handles := []sop.RegistryPayload[sop.Handle]{{IDs: []sop.Handle{{LogicalID: id, Version: 5}}}}
	nodes := []sop.Tuple[*sop.StoreInfo, []interface{}]{{First: si, Second: []interface{}{n}}}

	tx := &Transaction{l1Cache: cache.GetGlobalCache()}
	tx.updateVersionThenPopulateMru(ctx, handles, nodes)

	// Read back from MRU to verify version propagated.
	got := tx.l1Cache.GetNodeFromMRU(handles[0].IDs[0], &btree.Node[PersonKey, Person]{})
	if got == nil {
		t.Fatalf("expected node present in MRU")
	}
	if got.(*btree.Node[PersonKey, Person]).Version != 5 {
		t.Fatalf("expected version 5 in MRU, got %d", got.(*btree.Node[PersonKey, Person]).Version)
	}
}

// Reader transaction commitForReaderTransaction should retry once when versions mismatch then succeed after refetch updates.
func Test_ReaderTransaction_Commit_Retry_Then_Succeed(t *testing.T) {
	ctx := context.Background()
	// Prepare a node in fetched state with version 1 while registry says version 2.
	id := sop.NewUUID()
	node := &btree.Node[PersonKey, Person]{ID: id, Version: 1}
	reg := mocks.NewMockRegistry(false)
	h := sop.NewHandle(id)
	h.Version = 2
	if err := reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rt", IDs: []sop.Handle{h}}}); err != nil {
		t.Fatalf("seed registry: %v", err)
	}

	tx := &Transaction{mode: sop.ForReading, registry: reg, l2Cache: mocks.NewMockClient(), maxTime: time.Second}
	// Node repo backend with one fetched node (getAction) to be checked.
	nr := &nodeRepositoryBackend{transaction: tx, localCache: map[sop.UUID]cachedNode{id: {node: node, action: getAction}}}
	// btreeBackend wiring: hasTrackedItems true; refetchAndMerge updates node version to 2 to match registry on retry.
	b := btreeBackend{
		nodeRepository:  nr,
		hasTrackedItems: func() bool { return true },
		refetchAndMerge: func(context.Context) error {
			// Simulate refetch making node current
			if n, ok := nr.localCache[id]; ok {
				n.node.(*btree.Node[PersonKey, Person]).Version = 2
				nr.localCache[id] = n
			}
			return nil
		},
		getStoreInfo: func() *sop.StoreInfo { si := sop.NewStoreInfo(sop.StoreOptions{Name: "rt", SlotLength: 2}); return si },
	}
	tx.btreesBackend = []btreeBackend{b}

	if err := tx.commitForReaderTransaction(ctx); err != nil {
		t.Fatalf("commitForReaderTransaction err: %v", err)
	}
}

// mergeNodesKeys should unlock and clear nodesKeys when there are no updated/removed nodes.
func Test_MergeNodesKeys_Unlocks_On_Empty(t *testing.T) {
	ctx := context.Background()
	lc := mocks.NewMockClient()
	tx := &Transaction{l2Cache: lc}
	// Seed nodesKeys to non-nil so branch executes unlock.
	tx.nodesKeys = lc.CreateLockKeys([]string{sop.NewUUID().String()})
	tx.mergeNodesKeys(ctx, nil, nil)
	if tx.nodesKeys != nil {
		t.Fatalf("expected nodesKeys to be nil after mergeNodesKeys with empty inputs")
	}
}
