package in_red_ck

import (
	"context"
	"fmt"
	log "log/slog"
	"math/rand"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
	"github.com/SharedCode/sop/in_red_ck/kafka"
	"github.com/SharedCode/sop/in_red_ck/redis"
)

// TwoPhaseCommitTransaction interface defines the "infrastructure facing" transaction methods.
type TwoPhaseCommitTransaction interface {
	// Begin the transaction.
	Begin() error
	// Phase1Commit of the transaction.
	Phase1Commit(ctx context.Context) error
	// Phase2Commit of the transaction.
	Phase2Commit(ctx context.Context) error
	// Rollback the transaction.
	Rollback(ctx context.Context) error
	// Returns true if transaction has begun, false otherwise.
	HasBegun() bool
}

type btreeBackend struct {
	nodeRepository *nodeRepository
	// Following are function pointers because BTree is generic typed for Key & Value,
	// and these functions being pointers allow the backend to deal without requiring knowing data types.
	refetchAndMerge    func(ctx context.Context) error
	getStoreInfo       func() *btree.StoreInfo
	hasTrackedItems    func() bool
	checkTrackedItems  func(ctx context.Context) error
	lockTrackedItems   func(ctx context.Context, duration time.Duration) error
	unlockTrackedItems func(ctx context.Context) error

	// Manage tracked items' values in separate segments.
	commitTrackedItemsValues         func(ctx context.Context) error
	rollbackTrackedItemsValues       func(ctx context.Context) error
	deleteObsoleteTrackedItemsValues func(ctx context.Context) error
}

type transaction struct {
	// B-Tree instances, & their backend bits, managed within the transaction session.
	btreesBackend []btreeBackend
	// Needed by NodeRepository & ValueDataRepository for Node/Value data merging to the backend storage systems.
	blobStore       cas.BlobStore
	redisCache      redis.Cache
	storeRepository cas.StoreRepository
	// VirtualIDRegistry manages the virtual IDs, a.k.a. "handle".
	registry cas.Registry
	// true if transaction allows upserts & deletes, false(read-only mode) otherwise.
	forWriting bool
	// -1 = intial state, 0 = began, 1 = phase 1 commit done, 2 = phase 2 commit or rollback done.
	phaseDone int
	maxTime   time.Duration
	logger    *transactionLog
	// Phase 1 commit generated objects required for phase 2 commit.
	updatedNodeHandles []cas.RegistryPayload[sop.Handle]
	removedNodeHandles []cas.RegistryPayload[sop.Handle]
}

// Use lambda for time.Now so automated test can replace with replayable time if needed.
var now = time.Now

// NewTwoPhaseCommitTransaction will instantiate a transaction object for writing(forWriting=true)
// or for reading(forWriting=false). Pass in -1 on maxTime to default to 15 minutes of max "commit" duration.
func NewTwoPhaseCommitTransaction(forWriting bool, maxTime time.Duration) (TwoPhaseCommitTransaction, error) {
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
		forWriting:      forWriting,
		maxTime:         maxTime,
		storeRepository: cas.NewStoreRepository(),
		registry:        cas.NewRegistry(),
		redisCache:      redis.NewClient(),
		blobStore:       cas.NewBlobStore(),
		logger:          newTransactionLogger(nil),
		phaseDone:       -1,
	}, nil
}

func (t *transaction) Begin() error {
	if t.HasBegun() {
		return fmt.Errorf("Transaction is ongoing, 'can't begin again")
	}
	if t.phaseDone == 2 {
		return fmt.Errorf("Transaction is done, 'create a new one")
	}
	t.phaseDone = 0
	return nil
}

func (t *transaction) Phase1Commit(ctx context.Context) error {
	if !t.HasBegun() {
		return fmt.Errorf("No transaction to commit, call Begin to start a transaction")
	}
	if t.phaseDone == 2 {
		return fmt.Errorf("Transaction is done, 'create a new one")
	}
	t.phaseDone = 1
	if !t.forWriting {
		return t.commitForReaderTransaction(ctx)
	}
	if err := t.phase1Commit(ctx); err != nil {
		t.phaseDone = 2
		if rerr := t.rollback(ctx); rerr != nil {
			return fmt.Errorf("Phase 1 commit failed, details: %v, rollback error: %v", err, rerr)
		}
		return fmt.Errorf("Phase 1 commit failed, details: %v", err)
	}
	return nil
}

func (t *transaction) Phase2Commit(ctx context.Context) error {
	if !t.HasBegun() {
		return fmt.Errorf("No transaction to commit, call Begin to start a transaction")
	}
	if t.phaseDone == 0 {
		return fmt.Errorf("Phase 1 commit has not been invoke yet")
	}
	if t.phaseDone == 2 {
		return fmt.Errorf("Transaction is done, 'create a new one")
	}
	t.phaseDone = 2
	if !t.forWriting {
		return nil
	}
	if err := t.phase2Commit(ctx); err != nil {
		if _, ok := err.(*cas.UpdateAllOrNothingError); ok {
			startTime := now()
			// Retry if "update all or nothing" failed due to conflict. Retry will refetch & merge changes in
			// until it succeeds or timeout.
			for {
				if err := t.timedOut(ctx, startTime); err != nil {
					break
				}
				if rerr := t.rollback(ctx); rerr != nil {
					return fmt.Errorf("Phase 2 commit failed, details: %v, rollback error: %v", err, rerr)
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
		if rerr := t.rollback(ctx); rerr != nil {
			return fmt.Errorf("Phase 2 commit failed, details: %v, rollback error: %v", err, rerr)
		}
		return fmt.Errorf("Phase 2 commit failed, details: %v", err)
	}
	return nil
}

func (t *transaction) Rollback(ctx context.Context) error {
	if t.phaseDone == 2 {
		return fmt.Errorf("Transaction is done, 'create a new one")
	}
	if !t.HasBegun() {
		return fmt.Errorf("No transaction to rollback, call Begin to start a transaction")
	}
	// Reset transaction status and mark done to end it without persisting any change.
	t.phaseDone = 2
	if err := t.rollback(ctx); err != nil {
		return fmt.Errorf("Rollback failed, details: %v", err)
	}
	return nil
}

// Transaction has begun if it is has begun & not yet committed/rolled back.
func (t *transaction) HasBegun() bool {
	return t.phaseDone >= 0 && t.phaseDone < 2
}

func (t *transaction) timedOut(ctx context.Context, startTime time.Time) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if now().Sub(startTime).Minutes() > float64(t.maxTime) {
		return fmt.Errorf("Transaction timed out(maxTime=%v)", t.maxTime)
	}
	return nil
}

// Sleep in random milli-seconds to allow different conflicting (Node modifying) transactions
// to retry on different times, thus, increasing chance to succeed one after the other.
func randomSleep(ctx context.Context) {
	sleepTime := sleepBeforeRefetchBase + (1+rand.Intn(5))*100
	sleep(ctx, time.Duration(sleepTime)*time.Millisecond)
}

// sleep with context.
func sleep(ctx context.Context, sleepTime time.Duration) {
	sleep, cancel := context.WithTimeout(ctx, sleepTime)
	defer cancel()
	<-sleep.Done()
}

const sleepBeforeRefetchBase = 100

func (t *transaction) phase1Commit(ctx context.Context) error {
	if !t.hasTrackedItems() {
		return nil
	}
	// Mark session modified items as locked in Redis. If lock or there is conflict, return it as error.
	t.logger.log(ctx, lockTrackedItems, nil)
	if err := t.lockTrackedItems(ctx); err != nil {
		return err
	}

	var updatedNodes, removedNodes, addedNodes, fetchedNodes, rootNodes []sop.KeyValuePair[*btree.StoreInfo, []interface{}]
	startTime := now()
	var updatedNodesHandles, removedNodesHandles []cas.RegistryPayload[sop.Handle]

	// For writer transaction. Save the managed Node(s) as inactive:
	// NOTE: a transaction Commit can timeout and thus, rollback if it exceeds the maximum time(defaults to 30 mins).
	// Return error to trigger rollback for any operation that fails.
	//
	// - Create a lookup table of added/updated/removed items together with their Nodes
	//   Specify whether Node is updated, added or removed
	// * Repeat until timeout, for updated Nodes:
	// - Upsert each Node from the lookup to blobStore(Add only if blobStore is S3)
	// - Log UUID in transaction rollback log categorized as updated Node
	// - Compare each updated Node to Redis copy if identical(active UUID is same)
	//   NOTE: added Node(s) don't need this logic.
	//   For identical Node(s), update the "inactive UUID" with the Node's UUID(in redis).
	//   Collect each Node that are different in Redis(as updated by other transaction(s))
	//   Gather all the items of these Nodes(using the lookup table)
	//   Break if there are no more items different.
	// - Re-fetch the Nodes of these items, re-create the lookup table consisting only of these items
	//   & their re-fetched Nodes
	// Repeat end.
	// - Return error if loop timed out to trigger rollback.
	successful := false
	for !successful {
		var err error
		if err = t.timedOut(ctx, startTime); err != nil {
			return err
		}

		t.logger.enqueue(commitTrackedItemsValues, nil)
		if err := t.commitTrackedItemsValues(ctx); err != nil {
			return err
		}

		successful = true

		// Classify modified Nodes into update, remove and add. Updated & removed nodes are processed differently,
		// has to do merging & conflict resolution. Add is simple upsert.
		updatedNodes, removedNodes, addedNodes, fetchedNodes, rootNodes = t.classifyModifiedNodes()

		// TODO: parallelize these tasks.
		// Commit new root nodes.
		t.logger.enqueue(commitNewRootNodes, nil)
		if successful, err = t.btreesBackend[0].nodeRepository.commitNewRootNodes(ctx, rootNodes); err != nil {
			return err
		}

		if successful {
			// Check for conflict on fetched items.
			t.logger.enqueue(areFetchedItemsIntact, nil)
			if successful, err = t.btreesBackend[0].nodeRepository.areFetchedItemsIntact(ctx, fetchedNodes); err != nil {
				return err
			}
		}
		if successful {
			// Commit updated nodes.
			t.logger.enqueue(commitUpdatedNodes, nil)
			if successful, updatedNodesHandles, err = t.btreesBackend[0].nodeRepository.commitUpdatedNodes(ctx, updatedNodes); err != nil {
				return err
			}
		}
		// Only do commit removed nodes if successful so far.
		if successful {
			// Commit removed nodes.
			t.logger.enqueue(commitRemovedNodes, nil)
			if successful, removedNodesHandles, err = t.btreesBackend[0].nodeRepository.commitRemovedNodes(ctx, removedNodes); err != nil {
				return err
			}
		}
		if !successful {
			// Rollback partial changes.
			t.rollback(ctx)
			// Clear enqueued logs as we rolled back.
			t.logger.clearQueue()

			randomSleep(ctx)

			if err = t.refetchAndMergeModifications(ctx); err != nil {
				return err
			}
			if err = t.lockTrackedItems(ctx); err != nil {
				return err
			}
		}
	}

	// Persist logs in the queue.
	t.logger.saveQueue(ctx)

	// TODO: parallelize these tasks as well.
	// Commit added nodes.
	t.logger.log(ctx, commitAddedNodes, nil)
	if err := t.btreesBackend[0].nodeRepository.commitAddedNodes(ctx, addedNodes); err != nil {
		return err
	}

	// Commit stores update(CountDelta apply).
	t.logger.log(ctx, commitStoreInfo, nil)
	if err := t.commitStores(ctx); err != nil {
		return err
	}

	// Mark that store info commit succeeded, so it can get rolled back if rollback occurs.
	t.logger.log(ctx, beforeFinalize, nil)

	// Prepare to switch to active "state" the (inactive) updated Nodes. See phase2Commit for actual change.
	uh, err := t.btreesBackend[0].nodeRepository.activateInactiveNodes(ctx, updatedNodesHandles)
	if err != nil {
		return err
	}
	// Prepare to update upsert time of removed nodes to signal that they are finalized.
	// See phase2Commit for actual change.
	rh, err := t.btreesBackend[0].nodeRepository.touchNodes(ctx, removedNodesHandles)
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

func (t *transaction) phase2Commit(ctx context.Context) error {
	// Finalize the commit, it is the only all or nothing action in the commit,
	// and on registry (very small) records only.
	if err := t.logger.log(ctx, finalizeCommit, nil); err != nil {
		return err
	}
	if err := t.registry.Update(ctx, true, append(t.updatedNodeHandles, t.removedNodeHandles...)...); err != nil {
		return err
	}

	unusedNodeIDs := make([]cas.BlobsPayload[sop.UUID], 0, len(t.updatedNodeHandles)+len(t.removedNodeHandles))
	for i := range t.updatedNodeHandles {
		blobsIDs := cas.BlobsPayload[sop.UUID]{
			BlobTable: btree.ConvertToBlobTableName(t.updatedNodeHandles[i].RegistryTable),
			Blobs:     make([]sop.UUID, len(t.updatedNodeHandles[i].IDs)),
		}
		for ii := range t.updatedNodeHandles[i].IDs {
			// Since we've flipped the inactive to active, the new inactive ID is to be flushed out of Redis cache.
			blobsIDs.Blobs[ii] = t.updatedNodeHandles[i].IDs[ii].GetInActiveID()
		}
		unusedNodeIDs = append(unusedNodeIDs, blobsIDs)
	}

	// Package the logically deleted IDs for actual physical deletes.
	deletedIDs := make([]cas.RegistryPayload[sop.UUID], len(t.removedNodeHandles))
	for i := range t.removedNodeHandles {
		deletedIDs[i].RegistryTable = t.removedNodeHandles[i].RegistryTable
		deletedIDs[i].IDs = make([]sop.UUID, len(t.removedNodeHandles[i].IDs))
		blobsIDs := cas.BlobsPayload[sop.UUID]{
			BlobTable: btree.ConvertToBlobTableName(t.removedNodeHandles[i].RegistryTable),
			Blobs:     make([]sop.UUID, len(t.removedNodeHandles[i].IDs)),
		}
		for ii := range t.removedNodeHandles[i].IDs {
			// Removed nodes are marked deleted, thus, its active node ID can be safely removed.
			deletedIDs[i].IDs[ii] = t.removedNodeHandles[i].IDs[ii].LogicalID
			blobsIDs.Blobs[ii] = t.removedNodeHandles[i].IDs[ii].GetActiveID()
		}
		unusedNodeIDs = append(unusedNodeIDs, blobsIDs)
	}

	// TODO: finalize error handling, e.g. - if err occurred, don't remove logs.

	t.logger.log(ctx, deleteObsoleteEntries, []interface{}{deletedIDs, unusedNodeIDs})
	t.deleteObsoleteEntries(ctx, deletedIDs, unusedNodeIDs)

	t.logger.log(ctx, deleteObsoleteTrackedItemsValues, nil)
	t.deleteObsoleteTrackedItemsValues(ctx)

	// Commit is considered completed and the logs are not needed anymore.
	t.logger.removeLogs(ctx)
	// Unlock the items in Redis.
	if err := t.unlockTrackedItems(ctx); err != nil {
		// Just log as warning any error as at this point, commit is already finalized.
		// Any partial changes before failure in unlock tracked items will just expire in Redis.
		log.Warn(err.Error())
	}
	return nil
}

func (t *transaction) rollback(ctx context.Context) error {
	if t.logger.committedState == unlockTrackedItems {
		// This state should not be reached and rollback invoked, but return an error about it, in case.
		return fmt.Errorf("Transaction got committed, 'can't rollback it")
	}

	updatedNodes, removedNodes, addedNodes, _, rootNodes := t.classifyModifiedNodes()

	var lastErr error
	if t.logger.committedState == finalizeCommit {
		// do nothing as the function failed, nothing to undo.
	}
	if t.logger.committedState > commitStoreInfo {
		if err := t.rollbackStores(ctx); err != nil {
			lastErr = err
		}
	}
	if t.logger.committedState > commitAddedNodes {
		if err := t.btreesBackend[0].nodeRepository.rollbackAddedNodes(ctx, addedNodes); err != nil {
			lastErr = err
		}
	}
	if t.logger.committedState > commitRemovedNodes {
		if err := t.btreesBackend[0].nodeRepository.rollbackRemovedNodes(ctx, removedNodes); err != nil {
			lastErr = err
		}
	}
	if t.logger.committedState > commitUpdatedNodes {
		if err := t.btreesBackend[0].nodeRepository.rollbackUpdatedNodes(ctx, updatedNodes); err != nil {
			lastErr = err
		}
	}
	if t.logger.committedState > commitNewRootNodes {
		if err := t.btreesBackend[0].nodeRepository.rollbackNewRootNodes(ctx, rootNodes); err != nil {
			lastErr = err
		}
	}
	if t.logger.committedState >= commitTrackedItemsValues {
		if err := t.btreesBackend[0].rollbackTrackedItemsValues(ctx); err != nil {
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
func (t *transaction) rollbackTrackedItemsValues(ctx context.Context) error {
	for i := range t.btreesBackend {
		if err := t.btreesBackend[i].rollbackTrackedItemsValues(ctx); err != nil {
			return err
		}
	}
	return nil
}
func (t *transaction) deleteObsoleteTrackedItemsValues(ctx context.Context) error {
	for i := range t.btreesBackend {
		if err := t.btreesBackend[i].deleteObsoleteTrackedItemsValues(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Checks if fetched items are intact.
func (t *transaction) commitForReaderTransaction(ctx context.Context) error {
	if t.forWriting {
		return nil
	}
	if !t.hasTrackedItems() {
		return nil
	}
	// For a reader transaction, conflict check is enough.
	startTime := now()
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
	log.Debug("Same Node(s) are being modified elsewhere, 'will refetch and re-merge changes in...")
	for i := range t.btreesBackend {
		if err := t.btreesBackend[i].refetchAndMerge(ctx); err != nil {
			return err
		}
	}
	return nil
}

// classifyModifiedNodes will classify modified Nodes into 3 tables & return them:
// a. updated Nodes, b. removed Nodes, c. added Nodes, d. fetched Nodes.
func (t *transaction) classifyModifiedNodes() ([]sop.KeyValuePair[*btree.StoreInfo, []interface{}],
	[]sop.KeyValuePair[*btree.StoreInfo, []interface{}],
	[]sop.KeyValuePair[*btree.StoreInfo, []interface{}],
	[]sop.KeyValuePair[*btree.StoreInfo, []interface{}],
	[]sop.KeyValuePair[*btree.StoreInfo, []interface{}]) {
	var storesUpdatedNodes, storesRemovedNodes, storesAddedNodes, storesFetchedNodes, storesRootNodes []sop.KeyValuePair[*btree.StoreInfo, []interface{}]
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
			storesUpdatedNodes = append(storesUpdatedNodes, sop.KeyValuePair[*btree.StoreInfo, []interface{}]{
				Key:   s.getStoreInfo(),
				Value: updatedNodes,
			})
		}
		if len(removedNodes) > 0 {
			storesRemovedNodes = append(storesRemovedNodes, sop.KeyValuePair[*btree.StoreInfo, []interface{}]{
				Key:   s.getStoreInfo(),
				Value: removedNodes,
			})
		}
		if len(addedNodes) > 0 {
			storesAddedNodes = append(storesAddedNodes, sop.KeyValuePair[*btree.StoreInfo, []interface{}]{
				Key:   s.getStoreInfo(),
				Value: addedNodes,
			})
		}
		if len(fetchedNodes) > 0 {
			storesFetchedNodes = append(storesFetchedNodes, sop.KeyValuePair[*btree.StoreInfo, []interface{}]{
				Key:   s.getStoreInfo(),
				Value: fetchedNodes,
			})
		}
		if len(rootNodes) > 0 {
			storesRootNodes = append(storesRootNodes, sop.KeyValuePair[*btree.StoreInfo, []interface{}]{
				Key:   s.getStoreInfo(),
				Value: rootNodes,
			})
		}
	}
	return storesUpdatedNodes, storesRemovedNodes, storesAddedNodes, storesFetchedNodes, storesRootNodes
}

func (t *transaction) commitStores(ctx context.Context) error {
	stores := make([]btree.StoreInfo, len(t.btreesBackend))
	for i := range t.btreesBackend {
		store := t.btreesBackend[i].getStoreInfo()
		s2 := *store
		// Compute the count delta so Store Repository can reconcile for commit.
		s2.CountDelta = s2.Count - t.btreesBackend[i].nodeRepository.count
		s2.Timestamp = nowUnixMilli()
		stores[i] = s2
	}
	return t.storeRepository.Update(ctx, stores...)
}
func (t *transaction) rollbackStores(ctx context.Context) error {
	stores := make([]btree.StoreInfo, len(t.btreesBackend))
	for i := range t.btreesBackend {
		store := t.btreesBackend[i].getStoreInfo()
		s2 := *store
		// Compute the count delta so Store Repository can reconcile for rollback.
		s2.CountDelta = t.btreesBackend[i].nodeRepository.count - s2.Count
		stores[i] = s2
	}
	return t.storeRepository.Update(ctx, stores...)
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

var warnDeleteServiceMissing bool = true

// Delete the registry entries and unused node blobs.
func (t *transaction) deleteObsoleteEntries(ctx context.Context,
	deletedRegistryIDs []cas.RegistryPayload[sop.UUID], unusedNodeIDs []cas.BlobsPayload[sop.UUID]) {
	if len(unusedNodeIDs) > 0 {
		// Delete from Redis the inactive nodes.
		// Leave the registry keys as there may be other in-flight transactions that need them
		// for conflict resolution, to rollback or to fail their "reader" transaction.
		deletedKeys := make([]string, cas.GetBlobPayloadCount[sop.UUID](unusedNodeIDs))
		ik := 0
		for i := range unusedNodeIDs {
			for ii := range unusedNodeIDs[i].Blobs {
				deletedKeys[ik] = t.btreesBackend[0].nodeRepository.formatKey(unusedNodeIDs[i].Blobs[ii].String())
				ik++
			}
		}
		if err := t.redisCache.Delete(ctx, deletedKeys...); err != nil && !redis.KeyNotFound(err) {
			log.Error("Redis Delete failed, details: %v", err)
		}
		// Only attempt to send the delete message to Kafka if the delete service is enabled.
		if IsDeleteServiceEnabled {
			if ok, err := kafka.Enqueue[[]cas.BlobsPayload[sop.UUID]](ctx, unusedNodeIDs); !ok || err != nil {
				if err != nil {
					log.Error("Kafka Enqueue failed, details: %v, deleting the leftover unused nodes.", err)
				}
				if !ok {
					log.Info("Kafka Enqueue is still being sampled, deleting the leftover unused nodes.")
				}
				t.blobStore.Remove(ctx, unusedNodeIDs...)
			} else {
				log.Info(fmt.Sprintf("Kafka Enqueue passed sampling, expecting consumer(@topic:%s) to delete the leftover unused nodes.", kafka.GetConfig().Topic))
			}
		} else {
			if warnDeleteServiceMissing {
				// Warn only once per instance lifetime.
				log.Warn("DeleteService is not enabled, deleting the leftover unused nodes.")
				warnDeleteServiceMissing = false
			}
			t.blobStore.Remove(ctx, unusedNodeIDs...)
		}
	}
	// Delete from registry the requested entries.
	t.registry.Remove(ctx, deletedRegistryIDs...)
}
