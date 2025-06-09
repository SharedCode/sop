package common

import (
	"context"
	"fmt"
	"io"
	log "log/slog"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/cache"
)

type btreeBackend struct {
	nodeRepository *nodeRepositoryBackend
	// Following are function references because BTree is generic typed for Key & Value,
	// and these functions being references allow the backend to deal without requiring knowing data types.
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

// Transaction implements the sop's TwoPhaseTransaction interface.
type Transaction struct {
	id sop.UUID
	// B-Tree instances, & their backend bits, managed within the transaction session.
	btreesBackend []btreeBackend
	// Needed by NodeRepository & ValueDataRepository for Node/Value data merging to the backend storage systems.
	blobStore       sop.BlobStore
	l1Cache         *cache.L1Cache
	l2Cache         sop.Cache
	storeRepository sop.StoreRepository
	// VirtualIDRegistry manages the virtual IDs, a.k.a. "handle".
	registry sop.Registry
	// true if transaction allows upserts & deletes, false(read-only mode) otherwise.
	mode sop.TransactionMode
	// -1 = intial state, 0 = began, 1 = phase 1 commit started, 2 = phase 2 commit or rollback done.
	phaseDone int
	maxTime   time.Duration
	logger    *transactionLog

	// Handle replication related error.
	HandleReplicationRelatedError func(ctx context.Context, ioError error, rollbackSucceeded bool)

	// Phase 1 commit generated objects required for phase 2 commit.
	updatedNodeHandles []sop.RegistryPayload[sop.Handle]
	removedNodeHandles []sop.RegistryPayload[sop.Handle]

	// Phase 1 commit generated objects required for "replication" in phase 2 commit.
	addedNodeHandles   []sop.RegistryPayload[sop.Handle]
	newRootNodeHandles []sop.RegistryPayload[sop.Handle]
	updatedStoresInfo  []sop.StoreInfo

	// Needed for Phase 2 commit for populating MRU cache.
	updatedNodes []sop.Tuple[*sop.StoreInfo, []interface{}]
	addedNodes   []sop.Tuple[*sop.StoreInfo, []interface{}]
	rootNodes    []sop.Tuple[*sop.StoreInfo, []interface{}]

	// Used for transaction level locking.
	nodesKeys []*sop.LockKey
}

// NewTwoPhaseCommitTransaction will instantiate a transaction object for writing(forWriting=true)
// or for reading(forWriting=false). Pass in -1 on maxTime to default to 15 minutes of max "commit" duration.
// If logging is on, 'will log changes so it can get rolledback if transaction got left unfinished, e.g. crash or power reboot.
// However, without logging, the transaction commit can execute faster because there is no data getting logged.
func NewTwoPhaseCommitTransaction(mode sop.TransactionMode, maxTime time.Duration, logging bool,
	blobStore sop.BlobStore, storeRepository sop.StoreRepository, registry sop.Registry, l2Cache sop.Cache, transactionLog sop.TransactionLog) (*Transaction, error) {
	// Transaction commit time defaults to 15 mins if negative or 0.
	if maxTime <= 0 {
		maxTime = time.Duration(15 * time.Minute)
	}
	// Maximum transaction commit time is 1 hour.
	if maxTime > time.Duration(1*time.Hour) {
		maxTime = time.Duration(1 * time.Hour)
	}
	return &Transaction{
		mode:            mode,
		maxTime:         maxTime,
		storeRepository: storeRepository,
		registry:        registry,
		l2Cache:         l2Cache,
		l1Cache:         cache.GetGlobalCache(),
		blobStore:       blobStore,
		logger:          newTransactionLogger(transactionLog, logging),
		phaseDone:       -1,
		id:              sop.NewUUID(),
	}, nil
}

func (t *Transaction) Begin() error {
	if t.HasBegun() {
		return fmt.Errorf("transaction is ongoing, 'can't begin again")
	}
	if t.phaseDone == 2 {
		return fmt.Errorf("transaction is done, 'create a new one")
	}
	t.phaseDone = 0
	return nil
}

// Close will do cleanup.
func (t *Transaction) Close() error {
	// Do registry cleanup, e.g. - close all opened files.
	if closeable, ok := t.registry.(io.Closer); ok {
		return closeable.Close()
	}
	return nil
}

func (t *Transaction) Phase1Commit(ctx context.Context) error {
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
		rerr := t.rollback(ctx, true)

		// Allow replication handler to handle error related to replication, e.g. IO error.
		if t.HandleReplicationRelatedError != nil {
			t.HandleReplicationRelatedError(ctx, err, rerr == nil)
		}

		if rerr != nil {
			return fmt.Errorf("phase 1 commit failed, details: %w, rollback error: %v", err, rerr)
		}
		return fmt.Errorf("phase 1 commit failed, details: %w", err)
	}
	log.Debug("after phase1Commit call")
	return nil
}

func (t *Transaction) Phase2Commit(ctx context.Context) error {
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

	// Ensure resources are cleaned up or released.
	defer t.Close()
	if t.mode != sop.ForWriting {
		return nil
	}
	if err := t.phase2Commit(ctx); err != nil {
		rerr := t.rollback(ctx, true)

		// Allow replication handler to handle error related to replication, e.g. IO error.
		if t.HandleReplicationRelatedError != nil {
			t.HandleReplicationRelatedError(ctx, err, rerr == nil)
		}

		if rerr != nil {
			return fmt.Errorf("phase 2 commit failed, details: %w, rollback error: %v", err, rerr)
		}
		return fmt.Errorf("phase 2 commit failed, details: %w", err)
	}
	return nil
}

// Rollback the transaction. err param allows code to flow the error that caused rollback.
// An IO error as originally detected by this transaction can cause failover to the passive targets.
func (t *Transaction) Rollback(ctx context.Context, err error) error {
	if t.phaseDone == 2 {
		return fmt.Errorf("transaction is done, 'create a new one")
	}
	if !t.HasBegun() {
		return fmt.Errorf("no transaction to rollback, call Begin to start a transaction")
	}
	// Reset transaction status and mark done to end it without persisting any change.
	t.phaseDone = 2
	if rerr := t.rollback(ctx, true); rerr != nil {
		t.Close()

		// Allow replication handler to handle error related to replication, e.g. IO error.
		if t.HandleReplicationRelatedError != nil {
			t.HandleReplicationRelatedError(ctx, err, false)
		}

		return fmt.Errorf("rollback failed, details: %w", rerr)
	}
	t.Close()

	// Allow replication handler to handle error related to replication, e.g. IO error.
	if t.HandleReplicationRelatedError != nil {
		t.HandleReplicationRelatedError(ctx, err, true)
	}

	return nil
}

// Returns the transaction's mode.
func (t *Transaction) GetMode() sop.TransactionMode {
	return t.mode
}

// Transaction has begun if it is has begun & not yet committed/rolled back.
func (t *Transaction) HasBegun() bool {
	return t.phaseDone >= 0 && t.phaseDone < 2
}

func (t *Transaction) GetStores(ctx context.Context) ([]string, error) {
	return t.storeRepository.GetAll(ctx)
}

// Returns this transaction's StoreRepository.
func (t *Transaction) GetStoreRepository() sop.StoreRepository {
	return t.storeRepository
}

func (t *Transaction) GetID() sop.UUID {
	return t.id
}

// phase1Commit does the phase 1 commit steps.
func (t *Transaction) phase1Commit(ctx context.Context) error {
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

	// Classify modified Nodes into update, remove and add. Updated & removed nodes are processed differently,
	// has to do merging & conflict resolution. Add is simple upsert.
	updatedNodes, removedNodes, addedNodes, fetchedNodes, rootNodes = t.classifyModifiedNodes()

	// Extract lock keys from updated & removed nodes.
	t.mergeNodesKeys(ctx, updatedNodes, removedNodes)

	startTime := sop.Now()
	successful := false
	needsRefetchAndMerge := false

	for !successful {

		log.Debug(fmt.Sprintf("inside phase1Commit forloop, tid: %v", t.GetID()))

		var err error
		if err = t.timedOut(ctx, startTime); err != nil {
			return err
		}

		//* Start: Try to lock all updated & removed nodes before moving forward.
		if ok, _ := t.l2Cache.Lock(ctx, t.maxTime, t.nodesKeys); !ok {
			log.Debug(fmt.Sprintf("cache.Lock can't lock all nodesKeys, tid: %v", t.GetID()))
			// Unlock in case there are those that got locked.
			t.l2Cache.Unlock(ctx, t.nodesKeys)
			sop.RandomSleep(ctx)
			needsRefetchAndMerge = true
			continue
		}

		if ok, err := t.l2Cache.IsLocked(ctx, t.nodesKeys); !ok || err != nil {
			log.Debug(fmt.Sprintf("cache.IsLocked didn't confirm nodesKeys are locked, tid: %v", t.GetID()))
			sop.RandomSleep(ctx)
			continue
		}

		if needsRefetchAndMerge {
			log.Debug(fmt.Sprintf("before refetchAndMergeModifications, tid: %v", t.GetID()))
			if err := t.refetchAndMergeModifications(ctx); err != nil {
				log.Info(fmt.Sprintf("after refetchAndMergeModifications, tid: %v, error: %v", t.GetID(), err))
				return err
			}

			if err := t.logger.log(ctx, lockTrackedItems, nil); err != nil {
				return err
			}
			if err = t.lockTrackedItems(ctx); err != nil {
				log.Info(fmt.Sprintf("failed to lock tracked items, details: %v", err))
				return err
			}

			updatedNodes, removedNodes, addedNodes, fetchedNodes, rootNodes = t.classifyModifiedNodes()
			t.mergeNodesKeys(ctx, updatedNodes, removedNodes)
			needsRefetchAndMerge = false
			continue
		}
		//* End: Try to lock all updated & removed nodes before moving forward.

		if err := t.logger.log(ctx, commitTrackedItemsValues, toByteArray(t.getForRollbackTrackedItemsValues())); err != nil {
			return err
		}
		if err := t.commitTrackedItemsValues(ctx); err != nil {
			return err
		}

		// Remove the pre commit logs as not needed anymore from this point.
		if preCommitTID != sop.NilUUID {
			t.logger.logger.Remove(ctx, preCommitTID)
			preCommitTID = sop.NilUUID
		}

		successful = true

		// Commit new root nodes.
		bibs := convertToBlobRequestPayload(rootNodes)
		vids := convertToRegistryRequestPayload(rootNodes)

		if err := t.logger.log(ctx, commitNewRootNodes, toByteArray(sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{
			First: vids, Second: bibs,
		})); err != nil {
			return err
		}
		if successful, t.newRootNodeHandles, err = t.btreesBackend[0].nodeRepository.commitNewRootNodes(ctx, rootNodes); err != nil {
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
			if successful, updatedNodesHandles, err = t.btreesBackend[0].nodeRepository.commitUpdatedNodes(ctx, updatedNodes); err != nil {
				return err
			}
			// Log the inactive Blobs' IDs of newly written so we can just easily remove them when cleaning up "dead" transaction logs.
			if err := t.logger.log(ctx, commitUpdatedNodes, toByteArray(extractInactiveBlobsIDs(updatedNodesHandles))); err != nil {
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
			if rerr := t.rollback(ctx, false); rerr != nil {
				return fmt.Errorf("phase 1 commit failed, then rollback errored with: %w", rerr)
			}

			log.Debug("commit failed, refetch, remerge & another commit try will occur after randomSleep")

			needsRefetchAndMerge = true
			sop.RandomSleep(ctx)
		}
	}

	log.Debug(fmt.Sprintf("phase 1 commit loop done, tid: %v", t.GetID()))

	// Commit added nodes.
	if err := t.logger.log(ctx, commitAddedNodes, toByteArray(sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{
		First:  convertToRegistryRequestPayload(addedNodes),
		Second: convertToBlobRequestPayload(addedNodes),
	})); err != nil {
		return err
	}
	var err error
	if t.addedNodeHandles, err = t.btreesBackend[0].nodeRepository.commitAddedNodes(ctx, addedNodes); err != nil {
		return err
	}

	// Commit stores update(CountDelta apply).
	if err := t.logger.log(ctx, commitStoreInfo, toByteArray(t.getRollbackStoresInfo())); err != nil {
		return err
	}
	if t.updatedStoresInfo, err = t.commitStores(ctx); err != nil {
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

	log.Debug(fmt.Sprintf("phase 1 commit ends, tid: %v", t.GetID()))

	// Populate the phase 2 commit required objects.
	t.updatedNodeHandles = uh
	t.removedNodeHandles = rh

	t.addedNodes = addedNodes
	t.rootNodes = rootNodes
	t.updatedNodes = updatedNodes

	return nil
}

// phase2Commit finalizes the commit process and does cleanup afterwards.
func (t *Transaction) phase2Commit(ctx context.Context) error {

	f := t.getToBeObsoleteEntries()
	s := t.getObsoleteTrackedItemsValues()
	var pl sop.Tuple[sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]], []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]]
	if len(f.First) > 0 || len(f.Second) > 0 || len(s) > 0 {
		pl = sop.Tuple[sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]], []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]]{
			First:  f,
			Second: s,
		}
	}
	// Log the "finalizeCommit" step & parameters, useful for rollback.
	if err := t.logger.log(ctx, finalizeCommit, toByteArray(pl)); err != nil {
		t.unlockNodesKeys(ctx)
		return err
	}

	// The last step to consider a completed commit.
	if err := t.registry.UpdateNoLocks(ctx, append(t.updatedNodeHandles, t.removedNodeHandles...)); err != nil {
		t.unlockNodesKeys(ctx)
		return err
	}

	// Replicate to passive target paths.
	tr := sop.NewTaskRunner(ctx, -1)
	tr.Go(func() error {
		t.registry.Replicate(tr.GetContext(), t.newRootNodeHandles, t.addedNodeHandles, t.updatedNodeHandles, t.removedNodeHandles)
		return nil
	})
	tr.Go(func() error {
		t.storeRepository.Replicate(tr.GetContext(), t.updatedStoresInfo)
		return nil
	})
	tr.Go(func() error {
		t.logger.logCommitChanges(tr.GetContext(), t.updatedStoresInfo, t.newRootNodeHandles, t.addedNodeHandles, t.updatedNodeHandles, t.removedNodeHandles)
		return nil
	})
	t.populateMru(ctx)

	// Wait before proceeding to let replication to complete.
	tr.Wait()

	// Let other transactions get a lock on these updated & removed nodes' keys we've locked.
	t.unlockNodesKeys(ctx)

	// Unlock the items in Redis since technically, "commit" is done.
	if err := t.unlockTrackedItems(ctx); err != nil {
		// Just log as warning any error as at this point, commit is already finalized.
		// Any partial changes before failure in unlock tracked items will just expire in Redis.
		log.Warn(err.Error())
	}

	// Cleanup transaction logs & obsolete entries.
	t.cleanup(ctx)

	log.Debug(fmt.Sprintf("phase 2 commit ends, tid: %v", t.GetID()))

	return nil
}

func (t *Transaction) populateMru(ctx context.Context) {
	// Sync up the cache layers.
	t.updateVersionThenPopulateMru(ctx, t.addedNodeHandles, t.addedNodes)
	t.updateVersionThenPopulateMru(ctx, t.updatedNodeHandles, t.updatedNodes)
	t.updateVersionThenPopulateMru(ctx, t.newRootNodeHandles, t.rootNodes)
}

func (t *Transaction) updateVersionThenPopulateMru(ctx context.Context, handles []sop.RegistryPayload[sop.Handle], nodes []sop.Tuple[*sop.StoreInfo, []interface{}]) {
	for i := range nodes {
		for ii := range nodes[i].Second {
			target := nodes[i].Second[ii]
			target.(btree.MetaDataType).SetVersion(handles[i].IDs[ii].Version)
			t.l1Cache.SetNodeToMRU(ctx, handles[i].IDs[ii].GetActiveID(), target, nodes[i].First.CacheConfig.NodeCacheDuration)
		}
	}
}
