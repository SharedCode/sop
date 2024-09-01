package in_red_ck

import (
	"context"
	"fmt"
	log "log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
	"github.com/SharedCode/sop/redis"
)

type btreeBackend struct {
	nodeRepository *nodeRepository
	// Following are function pointers because BTree is generic typed for Key & Value,
	// and these functions being pointers allow the backend to deal without requiring knowing data types.
	refetchAndMerge    func(ctx context.Context) error
	getStoreInfo       func() *sop.StoreInfo
	hasTrackedItems    func() bool
	checkTrackedItems  func(ctx context.Context) error
	lockTrackedItems   func(ctx context.Context, duration time.Duration) error
	unlockTrackedItems func(ctx context.Context) error

	// Manage tracked items' values in separate segments.
	commitTrackedItemsValues         func(ctx context.Context) error
	getForRollbackTrackedItemsValues func() *sop.BlobsPayload[sop.UUID]
	getObsoleteTrackedItemsValues    func() *sop.BlobsPayload[sop.UUID]
}

type transaction struct {
	// B-Tree instances, & their backend bits, managed within the transaction session.
	btreesBackend []btreeBackend
	// Needed by NodeRepository & ValueDataRepository for Node/Value data merging to the backend storage systems.
	blobStore       sop.BlobStore
	redisCache      redis.Cache
	storeRepository sop.StoreRepository
	// VirtualIDRegistry manages the virtual IDs, a.k.a. "handle".
	registry sop.Registry
	// true if transaction allows upserts & deletes, false(read-only mode) otherwise.
	mode sop.TransactionMode
	// -1 = intial state, 0 = began, 1 = phase 1 commit done, 2 = phase 2 commit or rollback done.
	phaseDone int
	maxTime   time.Duration
	logger    *transactionLog
	// Phase 1 commit generated objects required for phase 2 commit.
	updatedNodeHandles []sop.RegistryPayload[sop.Handle]
	removedNodeHandles []sop.RegistryPayload[sop.Handle]
}

// Use lambda for time.Now so automated test can replace with replayable time if needed.
var Now = time.Now

// NewTransaction is a convenience function to create an enduser facing transaction object that wraps the two phase commit transaction.
func NewTransaction(mode sop.TransactionMode, maxTime time.Duration, logging bool) (sop.Transaction, error) {
	twoPT, err := NewTwoPhaseCommitTransaction(mode, maxTime, logging, cas.NewBlobStore(), cas.NewStoreRepository())
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(mode, twoPT, maxTime, logging)
}

// NewTwoPhaseCommitTransaction will instantiate a transaction object for writing(forWriting=true)
// or for reading(forWriting=false). Pass in -1 on maxTime to default to 15 minutes of max "commit" duration.
// If logging is on, 'will log changes so it can get rolledback if transaction got left unfinished, e.g. crash or power reboot.
// However, without logging, the transaction commit can execute faster because there is no data getting logged.
func NewTwoPhaseCommitTransaction(mode sop.TransactionMode, maxTime time.Duration, logging bool, blobStore sop.BlobStore, storeRepository sop.StoreRepository) (sop.TwoPhaseCommitTransaction, error) {
	// Transaction commit time defaults to 15 mins if negative or 0.
	if maxTime <= 0 {
		maxTime = time.Duration(15 * time.Minute)
	}
	// Maximum transaction commit time is 1 hour.
	if maxTime > time.Duration(1*time.Hour) {
		maxTime = time.Duration(1 * time.Hour)
	}
	if !IsInitialized() {
		return nil, fmt.Errorf("Redis and/or Cassandra bits were not initialized")
	}
	return &transaction{
		mode:            mode,
		maxTime:         maxTime,
		storeRepository: storeRepository,
		registry:        cas.NewRegistry(),
		redisCache:      redis.NewClient(),
		blobStore:       blobStore,
		logger:          newTransactionLogger(nil, logging),
		phaseDone:       -1,
	}, nil
}

func (t *transaction) Begin() error {
	if t.HasBegun() {
		return fmt.Errorf("transaction is ongoing, 'can't begin again")
	}
	if t.phaseDone == 2 {
		return fmt.Errorf("transaction is done, 'create a new one")
	}
	t.phaseDone = 0
	return nil
}

var lastOnIdleRunTime int64
var locker = sync.Mutex{}

func (t *transaction) onIdle(ctx context.Context) {
	// Required to have a backend btree to do cleanup.
	if len(t.btreesBackend) == 0 {
		return
	}
	// If it is known that there is nothing to clean up then do 4hr interval polling,
	// otherwise do shorter interval of 5 minutes, to allow faster cleanup.
	// Having "abandoned" commit is a very rare occurrence.
	interval := 4 * 60
	if hourBeingProcessed != "" {
		interval = 5
	}
	nextRunTime := Now().Add(time.Duration(-interval) * time.Minute).UnixMilli()
	if lastOnIdleRunTime < nextRunTime {
		runTime := false
		locker.Lock()
		if lastOnIdleRunTime < nextRunTime {
			lastOnIdleRunTime = Now().UnixMilli()
			runTime = true
		}
		locker.Unlock()
		if runTime {
			t.logger.processExpiredTransactionLogs(ctx, t)
		}
	}
}
func (t *transaction) Phase1Commit(ctx context.Context) error {
	// Service the cleanup of left hanging transactions.
	t.onIdle(ctx)

	if !t.HasBegun() {
		return fmt.Errorf("no transaction to commit, call Begin to start a transaction")
	}
	if t.phaseDone == 2 {
		return fmt.Errorf("transaction is done, 'create a new one")
	}
	t.phaseDone = 1
	if t.mode == sop.NoCheck {
		return nil
	}
	if t.mode == sop.ForReading {
		return t.commitForReaderTransaction(ctx)
	}
	if err := t.phase1Commit(ctx); err != nil {
		t.phaseDone = 2
		if rerr := t.rollback(ctx, true); rerr != nil {
			return fmt.Errorf("phase 1 commit failed, details: %v, rollback error: %v", err, rerr)
		}
		return fmt.Errorf("phase 1 commit failed, details: %v", err)
	}
	return nil
}

func (t *transaction) Phase2Commit(ctx context.Context) error {
	if !t.HasBegun() {
		return fmt.Errorf("no transaction to commit, call Begin to start a transaction")
	}
	if t.phaseDone == 0 {
		return fmt.Errorf("phase 1 commit has not been invoke yet")
	}
	if t.phaseDone == 2 {
		return fmt.Errorf("transaction is done, 'create a new one")
	}
	t.phaseDone = 2
	if t.mode != sop.ForWriting {
		return nil
	}
	if err := t.phase2Commit(ctx); err != nil {
		if _, ok := err.(*cas.UpdateAllOrNothingError); ok {
			startTime := Now()
			// Retry if "update all or nothing" failed due to conflict. Retry will refetch & merge changes in
			// until it succeeds or timeout.
			for {
				if err := t.timedOut(ctx, startTime); err != nil {
					break
				}
				if rerr := t.rollback(ctx, false); rerr != nil {
					return fmt.Errorf("phase 2 commit failed, details: %v, rollback error: %v", err, rerr)
				}
				log.Warn(err.Error() + ", will retry")

				randomSleep(ctx)
				if err = t.phase1Commit(ctx); err != nil {
					break
				}
				if err = t.phase2Commit(ctx); err == nil {
					return nil
				} else if _, ok := err.(*cas.UpdateAllOrNothingError); !ok {
					break
				}
			}
		}
		if rerr := t.rollback(ctx, true); rerr != nil {
			return fmt.Errorf("phase 2 commit failed, details: %v, rollback error: %v", err, rerr)
		}
		return fmt.Errorf("phase 2 commit failed, details: %v", err)
	}
	return nil
}

func (t *transaction) Rollback(ctx context.Context) error {
	if t.phaseDone == 2 {
		return fmt.Errorf("transaction is done, 'create a new one")
	}
	if !t.HasBegun() {
		return fmt.Errorf("no transaction to rollback, call Begin to start a transaction")
	}
	// Reset transaction status and mark done to end it without persisting any change.
	t.phaseDone = 2
	if err := t.rollback(ctx, true); err != nil {
		return fmt.Errorf("rollback failed, details: %v", err)
	}
	return nil
}

// Returns the transaction's mode.
func (t *transaction) GetMode() sop.TransactionMode {
	return t.mode
}

// Transaction has begun if it is has begun & not yet committed/rolled back.
func (t *transaction) HasBegun() bool {
	return t.phaseDone >= 0 && t.phaseDone < 2
}

func (t *transaction) timedOut(ctx context.Context, startTime time.Time) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if Now().Sub(startTime).Minutes() > float64(t.maxTime) {
		return fmt.Errorf("transaction timed out(maxTime=%v)", t.maxTime)
	}
	return nil
}

// Sleep in random milli-seconds to allow different conflicting (Node modifying) transactions
// to retry on different times, thus, increasing chance to succeed one after the other.
func randomSleep(ctx context.Context) {
	sleepTime := (1 + rand.Intn(6)) * 100
	sleep(ctx, time.Duration(sleepTime)*time.Millisecond)
}

// sleep with context.
func sleep(ctx context.Context, sleepTime time.Duration) {
	sleep, cancel := context.WithTimeout(ctx, sleepTime)
	defer cancel()
	<-sleep.Done()
}

// phase1Commit does the phase 1 commit steps.
func (t *transaction) phase1Commit(ctx context.Context) error {
	if !t.hasTrackedItems() {
		return nil
	}

	var preCommitTID sop.UUID
	if t.logger.committedState == addActivelyPersistedItem {
		preCommitTID = t.logger.transactionID
		// Assign new TID to the transaction as pre-commit logs need to be cleaned up seperately.
		t.logger.setNewTID()
	}

	if err := t.logger.log(ctx, lockTrackedItems, nil); err != nil {
		return err
	}
	// Mark session modified items as locked in Redis. If lock or there is conflict, return it as error.
	if err := t.lockTrackedItems(ctx); err != nil {
		return err
	}

	var updatedNodes, removedNodes, addedNodes, fetchedNodes, rootNodes []sop.Tuple[*sop.StoreInfo, []interface{}]
	var updatedNodesHandles, removedNodesHandles []sop.RegistryPayload[sop.Handle]

	startTime := Now()
	successful := false
	for !successful {
		var err error
		if err = t.timedOut(ctx, startTime); err != nil {
			return err
		}

		if err := t.logger.log(ctx, commitTrackedItemsValues, toByteArray(t.getForRollbackTrackedItemsValues())); err != nil {
			return err
		}
		if err := t.commitTrackedItemsValues(ctx); err != nil {
			return err
		}

		// Remove the pre commit logs as not needed anymore from this point.
		// TODO: finalize the logic here and the commit call above.
		if preCommitTID != sop.NilUUID {
			t.logger.logger.Remove(ctx, preCommitTID)
			preCommitTID = sop.NilUUID
		}

		successful = true

		// Classify modified Nodes into update, remove and add. Updated & removed nodes are processed differently,
		// has to do merging & conflict resolution. Add is simple upsert.
		updatedNodes, removedNodes, addedNodes, fetchedNodes, rootNodes = t.classifyModifiedNodes()

		// Commit new root nodes.
		bibs := convertToBlobRequestPayload(rootNodes)
		vids := convertToRegistryRequestPayload(rootNodes)
		if err := t.logger.log(ctx, commitNewRootNodes, toByteArray(sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{
			First: vids, Second: bibs,
		})); err != nil {
			return err
		}
		if successful, err = t.btreesBackend[0].nodeRepository.commitNewRootNodes(ctx, rootNodes); err != nil {
			return err
		}

		if successful {
			// Check for conflict on fetched items.
			if err := t.logger.log(ctx, areFetchedItemsIntact, nil); err != nil {
				return err
			}
			if successful, err = t.btreesBackend[0].nodeRepository.areFetchedItemsIntact(ctx, fetchedNodes); err != nil {
				return err
			}
		}
		if successful {
			// Commit updated nodes.
			if err := t.logger.log(ctx, commitUpdatedNodes, toByteArray(convertToRegistryRequestPayload(updatedNodes))); err != nil {
				return err
			}
			if successful, updatedNodesHandles, err = t.btreesBackend[0].nodeRepository.commitUpdatedNodes(ctx, updatedNodes); err != nil {
				return err
			}
		}
		// Only do commit removed nodes if successful so far.
		if successful {
			// Commit removed nodes.
			if err := t.logger.log(ctx, commitRemovedNodes, toByteArray(convertToRegistryRequestPayload(removedNodes))); err != nil {
				return err
			}
			if successful, removedNodesHandles, err = t.btreesBackend[0].nodeRepository.commitRemovedNodes(ctx, removedNodes); err != nil {
				return err
			}
		}
		if !successful {
			// Rollback partial changes.
			t.rollback(ctx, false)
			// Clear logs as we rolled back.
			t.logger.removeLogs(ctx)

			randomSleep(ctx)

			if err = t.refetchAndMergeModifications(ctx); err != nil {
				return err
			}
			if err := t.logger.log(ctx, lockTrackedItems, nil); err != nil {
				return err
			}
			if err = t.lockTrackedItems(ctx); err != nil {
				return err
			}
		}
	}

	// Commit added nodes.
	if err := t.logger.log(ctx, commitAddedNodes, toByteArray(sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{
		First:  convertToRegistryRequestPayload(addedNodes),
		Second: convertToBlobRequestPayload(addedNodes),
	})); err != nil {
		return err
	}
	if err := t.btreesBackend[0].nodeRepository.commitAddedNodes(ctx, addedNodes); err != nil {
		return err
	}

	// Commit stores update(CountDelta apply).
	if err := t.logger.log(ctx, commitStoreInfo, toByteArray(t.getRollbackStoresInfo())); err != nil {
		return err
	}
	if err := t.commitStores(ctx); err != nil {
		return err
	}

	// Mark that store info commit succeeded, so it can get rolled back if rollback occurs.
	if err := t.logger.log(ctx, beforeFinalize, nil); err != nil {
		return err
	}

	// Prepare to switch to active "state" the (inactive) updated Nodes, in phase2Commit.
	uh, err := t.btreesBackend[0].nodeRepository.activateInactiveNodes(updatedNodesHandles)
	if err != nil {
		return err
	}

	// Prepare to update upsert time of removed nodes to signal that they are finalized, in phase2Commit.
	rh, err := t.btreesBackend[0].nodeRepository.touchNodes(removedNodesHandles)
	if err != nil {
		return err
	}

	// In case race condition exists, we remove it here by checking our tracked items' lock integrity.
	if err := t.checkTrackedItems(ctx); err != nil {
		return err
	}

	// Populate the phase 2 commit required objects.
	t.updatedNodeHandles = uh
	t.removedNodeHandles = rh

	return nil
}

// phase2Commit finalizes the commit process and does cleanup afterwards.
func (t *transaction) phase2Commit(ctx context.Context) error {

	// The last step to consider a completed commit. It is the only "all or nothing" action in the commit.
	f := t.getToBeObsoleteEntries()
	s := t.getObsoleteTrackedItemsValues()
	var pl sop.Tuple[sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]], []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]]
	if len(f.First) > 0 || len(f.Second) > 0 || len(s) > 0 {
		pl = sop.Tuple[sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]], []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]]{
			First:  f,
			Second: s,
		}
	}
	if err := t.logger.log(ctx, finalizeCommit, toByteArray(pl)); err != nil {
		return err
	}
	if err := t.registry.Update(ctx, true, append(t.updatedNodeHandles, t.removedNodeHandles...)...); err != nil {
		return err
	}

	// Unlock the items in Redis since technically "commit" is done.
	if err := t.unlockTrackedItems(ctx); err != nil {
		// Just log as warning any error as at this point, commit is already finalized.
		// Any partial changes before failure in unlock tracked items will just expire in Redis.
		log.Warn(err.Error())
	}

	// Cleanup transaction logs & obsolete entries.
	t.cleanup(ctx)
	return nil
}

func (t *transaction) cleanup(ctx context.Context) error {
	// Cleanup resources not needed anymore.
	if err := t.logger.log(ctx, deleteObsoleteEntries, nil); err != nil {
		return err
	}
	obsoleteEntries := t.getToBeObsoleteEntries()
	t.deleteObsoleteEntries(ctx, obsoleteEntries.First, obsoleteEntries.Second)

	if err := t.logger.log(ctx, deleteTrackedItemsValues, nil); err != nil {
		return err
	}
	t.deleteTrackedItemsValues(ctx, t.getObsoleteTrackedItemsValues())

	// Remove unneeded transaction logs since commit is done.
	t.logger.removeLogs(ctx)
	return nil
}

func (t *transaction) getToBeObsoleteEntries() sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]] {
	// Cleanup resources not needed anymore.
	unusedNodeIDs := make([]sop.BlobsPayload[sop.UUID], 0, len(t.updatedNodeHandles)+len(t.removedNodeHandles))
	for i := range t.updatedNodeHandles {
		blobsIDs := sop.BlobsPayload[sop.UUID]{
			BlobTable: t.updatedNodeHandles[i].BlobTable,
			Blobs:     make([]sop.UUID, len(t.updatedNodeHandles[i].IDs)),
		}
		for ii := range t.updatedNodeHandles[i].IDs {
			// Since we've flipped the inactive to active, the new inactive ID is to be flushed out of Redis cache.
			blobsIDs.Blobs[ii] = t.updatedNodeHandles[i].IDs[ii].GetInActiveID()
		}
		unusedNodeIDs = append(unusedNodeIDs, blobsIDs)
	}

	// Package the logically deleted IDs for actual physical deletes.
	deletedIDs := make([]sop.RegistryPayload[sop.UUID], len(t.removedNodeHandles))
	for i := range t.removedNodeHandles {
		deletedIDs[i].RegistryTable = t.removedNodeHandles[i].RegistryTable
		deletedIDs[i].IDs = make([]sop.UUID, len(t.removedNodeHandles[i].IDs))
		blobsIDs := sop.BlobsPayload[sop.UUID]{
			BlobTable: t.removedNodeHandles[i].BlobTable,
			Blobs:     make([]sop.UUID, len(t.removedNodeHandles[i].IDs)),
		}
		for ii := range t.removedNodeHandles[i].IDs {
			// Removed nodes are marked deleted, thus, its active node ID can be safely removed.
			deletedIDs[i].IDs[ii] = t.removedNodeHandles[i].IDs[ii].LogicalID
			blobsIDs.Blobs[ii] = t.removedNodeHandles[i].IDs[ii].GetActiveID()
		}
		unusedNodeIDs = append(unusedNodeIDs, blobsIDs)
	}

	return sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{
		First:  deletedIDs,
		Second: unusedNodeIDs,
	}
}

func (t *transaction) rollback(ctx context.Context, rollbackTrackedItemsValues bool) error {
	var lastErr error

	// Rollback pre commit logged items.
	if t.logger.committedState == addActivelyPersistedItem {
		itemsForDelete := t.getForRollbackTrackedItemsValues()
		if err := t.deleteTrackedItemsValues(ctx, itemsForDelete); err != nil {
			lastErr = err
		}
		// Transaction got rolled back, no need for the logs.
		t.logger.removeLogs(ctx)
		// Rewind the transaction log in case retry will check it.
		t.logger.log(ctx, unknown, nil)
		return lastErr
	}

	// Rollback on commit logged items.
	if t.logger.committedState > finalizeCommit {
		// This state should not be reached and rollback invoked, but return an error about it, in case.
		return fmt.Errorf("transaction got committed, 'can't rollback it")
	}

	updatedNodes, removedNodes, addedNodes, _, rootNodes := t.classifyModifiedNodes()

	if t.logger.committedState > commitStoreInfo {
		rollbackStoresInfo := t.getRollbackStoresInfo()
		if err := t.storeRepository.Update(ctx, rollbackStoresInfo...); err != nil {
			lastErr = err
		}
	}
	if t.logger.committedState > commitAddedNodes {
		bibs := convertToBlobRequestPayload(addedNodes)
		vids := convertToRegistryRequestPayload(addedNodes)
		bv := sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: vids, Second: bibs}
		if err := t.btreesBackend[0].nodeRepository.rollbackAddedNodes(ctx, bv); err != nil {
			lastErr = err
		}
	}
	if t.logger.committedState > commitRemovedNodes {
		vids := convertToRegistryRequestPayload(removedNodes)
		if err := t.btreesBackend[0].nodeRepository.rollbackRemovedNodes(ctx, vids); err != nil {
			lastErr = err
		}
	}
	if t.logger.committedState > commitUpdatedNodes {
		vids := convertToRegistryRequestPayload(updatedNodes)
		if err := t.btreesBackend[0].nodeRepository.rollbackUpdatedNodes(ctx, vids); err != nil {
			lastErr = err
		}
	}
	if t.logger.committedState > commitNewRootNodes {
		bibs := convertToBlobRequestPayload(rootNodes)
		vids := convertToRegistryRequestPayload(rootNodes)
		bv := sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: vids, Second: bibs}
		if err := t.btreesBackend[0].nodeRepository.rollbackNewRootNodes(ctx, bv); err != nil {
			lastErr = err
		}
	}
	if rollbackTrackedItemsValues && t.logger.committedState >= commitTrackedItemsValues {
		itemsForDelete := t.getForRollbackTrackedItemsValues()
		if err := t.deleteTrackedItemsValues(ctx, itemsForDelete); err != nil {
			lastErr = err
		}
	}
	if t.logger.committedState >= lockTrackedItems {
		if err := t.unlockTrackedItems(ctx); err != nil {
			lastErr = err
		}
	}
	// Transaction got rolled back, no need for the logs.
	t.logger.removeLogs(ctx)
	// Rewind the transaction log in case retry will check it.
	t.logger.log(ctx, unknown, nil)

	return lastErr
}

func (t *transaction) commitTrackedItemsValues(ctx context.Context) error {
	for i := range t.btreesBackend {
		if err := t.btreesBackend[i].commitTrackedItemsValues(ctx); err != nil {
			return err
		}
	}
	return nil
}
func (t *transaction) getForRollbackTrackedItemsValues() []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]] {
	r := make([]sop.Tuple[bool, sop.BlobsPayload[sop.UUID]], 0, 5)
	for i := range t.btreesBackend {
		itemsForDelete := t.btreesBackend[i].getForRollbackTrackedItemsValues()
		if itemsForDelete != nil && len(itemsForDelete.Blobs) > 0 {
			r = append(r, sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]{
				First:  t.btreesBackend[i].getStoreInfo().IsValueDataGloballyCached,
				Second: *itemsForDelete,
			})
		}
	}
	return r
}
func (t *transaction) getObsoleteTrackedItemsValues() []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]] {
	r := make([]sop.Tuple[bool, sop.BlobsPayload[sop.UUID]], 0, 5)
	for i := range t.btreesBackend {
		itemsForDelete := t.btreesBackend[i].getObsoleteTrackedItemsValues()
		if itemsForDelete != nil && len(itemsForDelete.Blobs) > 0 {
			r = append(r, sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]{
				First:  t.btreesBackend[i].getStoreInfo().IsValueDataGloballyCached,
				Second: *itemsForDelete,
			})
		}
	}
	return r
}

func (t *transaction) deleteTrackedItemsValues(ctx context.Context, itemsForDelete []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]) error {
	var lastErr error
	for i := range itemsForDelete {
		// First field of the Tuple specifies whether we need to delete from Redis cache the blob IDs specified in Second.
		if itemsForDelete[i].First {
			for ii := range itemsForDelete[i].Second.Blobs {
				if err := t.redisCache.Delete(ctx, formatItemKey(itemsForDelete[i].Second.Blobs[ii].String())); err != nil {
					lastErr = err
				}
			}
		}
		if err := t.blobStore.Remove(ctx, itemsForDelete[i].Second); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Checks if fetched items are intact.
func (t *transaction) commitForReaderTransaction(ctx context.Context) error {
	if t.mode == sop.ForWriting {
		return nil
	}
	if !t.hasTrackedItems() {
		return nil
	}
	// For a reader transaction, conflict check is enough.
	startTime := Now()
	for {
		if err := t.timedOut(ctx, startTime); err != nil {
			return err
		}
		// Check items if have not changed since fetching.
		_, _, _, fetchedNodes, _ := t.classifyModifiedNodes()
		if ok, err := t.btreesBackend[0].nodeRepository.areFetchedItemsIntact(ctx, fetchedNodes); err != nil {
			return err
		} else if ok {
			return nil
		}

		randomSleep(ctx)
		// Recreate the fetches on latest committed nodes & check if fetched Items are unchanged.
		if err := t.refetchAndMergeModifications(ctx); err != nil {
			return err
		}
	}
}

// Use tracked Items to refetch their Nodes(using B-Tree) and merge the changes in, if there is no conflict.
func (t *transaction) refetchAndMergeModifications(ctx context.Context) error {
	log.Debug("same node(s) are being modified elsewhere, 'will refetch and re-merge changes in...")
	for i := range t.btreesBackend {
		if err := t.btreesBackend[i].refetchAndMerge(ctx); err != nil {
			return err
		}
	}
	return nil
}

// classifyModifiedNodes will classify modified Nodes into 3 tables & return them:
// a. updated Nodes, b. removed Nodes, c. added Nodes, d. fetched Nodes.
func (t *transaction) classifyModifiedNodes() ([]sop.Tuple[*sop.StoreInfo, []interface{}],
	[]sop.Tuple[*sop.StoreInfo, []interface{}],
	[]sop.Tuple[*sop.StoreInfo, []interface{}],
	[]sop.Tuple[*sop.StoreInfo, []interface{}],
	[]sop.Tuple[*sop.StoreInfo, []interface{}]) {
	var storesUpdatedNodes, storesRemovedNodes, storesAddedNodes, storesFetchedNodes, storesRootNodes []sop.Tuple[*sop.StoreInfo, []interface{}]
	for i, s := range t.btreesBackend {
		var updatedNodes, removedNodes, addedNodes, fetchedNodes, rootNodes []interface{}
		for _, cacheNode := range s.nodeRepository.nodeLocalCache {
			// Allow newly created root nodes to get merged between transactions.
			if s.nodeRepository.count == 0 &&
				cacheNode.action == addAction && t.btreesBackend[i].getStoreInfo().RootNodeID == cacheNode.node.(btree.MetaDataType).GetID() {
				rootNodes = append(rootNodes, cacheNode.node)
				continue
			}
			switch cacheNode.action {
			case updateAction:
				updatedNodes = append(updatedNodes, cacheNode.node)
			case removeAction:
				removedNodes = append(removedNodes, cacheNode.node)
			case addAction:
				addedNodes = append(addedNodes, cacheNode.node)
			case getAction:
				fetchedNodes = append(fetchedNodes, cacheNode.node)
			}
		}
		if len(updatedNodes) > 0 {
			storesUpdatedNodes = append(storesUpdatedNodes, sop.Tuple[*sop.StoreInfo, []interface{}]{
				First:  s.getStoreInfo(),
				Second: updatedNodes,
			})
		}
		if len(removedNodes) > 0 {
			storesRemovedNodes = append(storesRemovedNodes, sop.Tuple[*sop.StoreInfo, []interface{}]{
				First:  s.getStoreInfo(),
				Second: removedNodes,
			})
		}
		if len(addedNodes) > 0 {
			storesAddedNodes = append(storesAddedNodes, sop.Tuple[*sop.StoreInfo, []interface{}]{
				First:  s.getStoreInfo(),
				Second: addedNodes,
			})
		}
		if len(fetchedNodes) > 0 {
			storesFetchedNodes = append(storesFetchedNodes, sop.Tuple[*sop.StoreInfo, []interface{}]{
				First:  s.getStoreInfo(),
				Second: fetchedNodes,
			})
		}
		if len(rootNodes) > 0 {
			storesRootNodes = append(storesRootNodes, sop.Tuple[*sop.StoreInfo, []interface{}]{
				First:  s.getStoreInfo(),
				Second: rootNodes,
			})
		}
	}
	return storesUpdatedNodes, storesRemovedNodes, storesAddedNodes, storesFetchedNodes, storesRootNodes
}

func (t *transaction) commitStores(ctx context.Context) error {
	stores := make([]sop.StoreInfo, len(t.btreesBackend))
	for i := range t.btreesBackend {
		store := t.btreesBackend[i].getStoreInfo()
		s2 := *store
		// Compute the count delta so Store Repository can reconcile for commit.
		s2.CountDelta = s2.Count - t.btreesBackend[i].nodeRepository.count
		s2.Timestamp = Now().UnixMilli()
		stores[i] = s2
	}
	return t.storeRepository.Update(ctx, stores...)
}
func (t *transaction) getRollbackStoresInfo() []sop.StoreInfo {
	stores := make([]sop.StoreInfo, len(t.btreesBackend))
	for i := range t.btreesBackend {
		store := t.btreesBackend[i].getStoreInfo()
		s2 := *store
		// Compute the count delta so Store Repository can reconcile for rollback.
		s2.CountDelta = t.btreesBackend[i].nodeRepository.count - s2.Count
		stores[i] = s2
	}
	return stores
}

func (t *transaction) hasTrackedItems() bool {
	for _, s := range t.btreesBackend {
		if s.hasTrackedItems() {
			return true
		}
	}
	return false
}

// Check Tracked items for conflict, this pass is to remove any race condition.
func (t *transaction) checkTrackedItems(ctx context.Context) error {
	for _, s := range t.btreesBackend {
		if err := s.checkTrackedItems(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (t *transaction) lockTrackedItems(ctx context.Context) error {
	for _, s := range t.btreesBackend {
		if err := s.lockTrackedItems(ctx, t.maxTime); err != nil {
			return err
		}
	}
	return nil
}

func (t *transaction) unlockTrackedItems(ctx context.Context) error {
	var lastErr error
	for _, s := range t.btreesBackend {
		if err := s.unlockTrackedItems(ctx); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Delete the registry entries and unused node blobs.
func (t *transaction) deleteObsoleteEntries(ctx context.Context,
	deletedRegistryIDs []sop.RegistryPayload[sop.UUID], unusedNodeIDs []sop.BlobsPayload[sop.UUID]) error {
	var lastErr error
	if len(unusedNodeIDs) > 0 {
		// Delete from Redis & BlobStore the unused/inactive nodes.
		deletedKeys := make([]string, sop.GetBlobPayloadCount(unusedNodeIDs))
		ik := 0
		for i := range unusedNodeIDs {
			for ii := range unusedNodeIDs[i].Blobs {
				deletedKeys[ik] = t.btreesBackend[0].nodeRepository.formatKey(unusedNodeIDs[i].Blobs[ii].String())
				ik++
			}
		}
		if err := t.redisCache.Delete(ctx, deletedKeys...); err != nil && !redis.KeyNotFound(err) {
			lastErr = err
			log.Error(fmt.Sprintf("Redis delete failed, details: %v", err))
		}
		if err := t.blobStore.Remove(ctx, unusedNodeIDs...); err != nil {
			lastErr = err
		}
		// End of block.
	}
	// Delete from registry the deleted Registry IDs (it manages redis cache internally).
	if err := t.registry.Remove(ctx, deletedRegistryIDs...); err != nil {
		lastErr = err
	}
	return lastErr
}
