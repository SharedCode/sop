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
	nodeRepository     *nodeRepository
	refetchAndMerge    func(ctx context.Context) error
	getStoreInfo       func() *btree.StoreInfo
	lockTrackedItems   func(ctx context.Context, itemRedisCache redis.Cache, duration time.Duration) error
	unlockTrackedItems func(ctx context.Context, itemRedisCache redis.Cache) error
}

type transaction struct {
	// B-Tree instances, & their backend bits, managed within the transaction session.
	btreesBackend []btreeBackend
	// Needed by NodeRepository for Node data merging to the backend storage systems.
	nodeBlobStore   cas.BlobStore
	redisCache      redis.Cache
	storeRepository cas.StoreRepository
	// VirtualIdRegistry manages the virtual Ids, a.k.a. "handle".
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
var getCurrentTime = time.Now

// NewTwoPhaseCommitTransaction will instantiate a transaction object for writing(forWriting=true)
// or for reading(forWriting=false). Pass in -1 on maxTime to default to 15 minutes
// of session duration.
func NewTwoPhaseCommitTransaction(forWriting bool, maxTime time.Duration) (TwoPhaseCommitTransaction, error) {
	if maxTime <= 0 {
		m := 15
		maxTime = time.Duration(m * int(time.Minute))
	}
	if !IsInitialized() {
		return nil, fmt.Errorf("Redis and/or Cassandra bits were not initialized")
	}
	return &transaction{
		forWriting: forWriting,
		maxTime:    maxTime,
		// TODO: Allow caller to supply Redis & blob store settings.
		storeRepository: cas.NewStoreRepository(),
		registry:        cas.NewRegistry(),
		redisCache:      redis.NewClient(),
		nodeBlobStore:   cas.NewBlobStore(),
		logger:          newTransactionLogger(),
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
		if rerr := t.rollback(ctx, false); rerr != nil {
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
		if rerr := t.rollback(ctx, false); rerr != nil {
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
	return nil
}

func (t *transaction) HasBegun() bool {
	return t.phaseDone >= 0
}

func (t *transaction) timedOut(ctx context.Context, startTime time.Time) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if getCurrentTime().Sub(startTime).Minutes() > float64(t.maxTime) {
		return fmt.Errorf("Transaction timed out(maxTime=%v)", t.maxTime)
	}
	return nil
}

// sleep with context.
func sleep(ctx context.Context, sleepTime int) {
	sleep, cancel := context.WithTimeout(ctx, time.Second*time.Duration(sleepTime))
	defer cancel()
	<-sleep.Done()
}

func (t *transaction) phase1Commit(ctx context.Context) error {
	// Mark session modified items as locked in Redis. If lock or there is conflict, return it as error.
	t.logger.log(lockTrackedItems)
	if err := t.lockTrackedItems(ctx); err != nil {
		return err
	}

	var updatedNodes, removedNodes, addedNodes, fetchedNodes, rootNodes []sop.KeyValuePair[*btree.StoreInfo, []interface{}]
	startTime := getCurrentTime()

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

		successful = true
		// Classify modified Nodes into update, remove and add. Updated & removed nodes are processed differently,
		// has to do merging & conflict resolution. Add is simple upsert.
		updatedNodes, removedNodes, addedNodes, fetchedNodes, rootNodes = t.classifyModifiedNodes()

		// Commit new root nodes.
		t.logger.log(commitNewRootNodes)
		if successful, err = t.btreesBackend[0].nodeRepository.commitNewRootNodes(ctx, rootNodes); err != nil {
			return err
		}

		if successful {
			// Check for conflict on fetched nodes.
			if successful, err = t.btreesBackend[0].nodeRepository.areFetchedNodesIntact(ctx, fetchedNodes); err != nil {
				return err
			}
		}
		if successful {
			// Commit updated nodes.
			t.logger.log(commitUpdatedNodes)
			if successful, err = t.btreesBackend[0].nodeRepository.commitUpdatedNodes(ctx, updatedNodes); err != nil {
				return err
			}
		}
		// Only do commit removed nodes if successful so far.
		if successful {
			// Commit removed nodes.
			t.logger.log(commitRemovedNodes)
			if successful, err = t.btreesBackend[0].nodeRepository.commitRemovedNodes(ctx, removedNodes); err != nil {
				return err
			}
		}
		if !successful {
			// Rollback partial changes.
			t.rollback(ctx, true)

			// Sleep in random seconds to allow different conflicting (Node modifying) transactions
			// (in-flight) to retry on different times.
			sleepTime := rand.Intn(4+1) + 5
			sleep(ctx, sleepTime)

			// Recreate the changes on latest committed nodes, if there is no conflict.
			if err = t.refetchAndMergeModifications(ctx); err != nil {
				return err
			}
		}
	}

	// Commit added nodes.
	t.logger.log(commitAddedNodes)
	if err := t.btreesBackend[0].nodeRepository.commitAddedNodes(ctx, addedNodes); err != nil {
		return err
	}

	// Commit stores update(CountDelta apply).
	t.logger.log(commitStoreInfo)
	if err := t.commitStores(ctx); err != nil {
		return err
	}

	// Prepare to switch to active "state" the (inactive) updated Nodes so they will
	// get started to be "seen" in such state on succeeding fetch. See phase2Commit for actual change.
	uh, err := t.btreesBackend[0].nodeRepository.activateInactiveNodes(ctx, updatedNodes)
	if err != nil {
		return err
	}
	// Prepare to update upsert time of removed nodes to signal that they are finalized.
	// See phase2Commit for actual change.
	rh, err := t.btreesBackend[0].nodeRepository.touchNodes(ctx, removedNodes)
	if err != nil {
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
	t.logger.log(finalizeCommit)
	if err := t.registry.Update(ctx, true, append(t.updatedNodeHandles, t.removedNodeHandles...)...); err != nil {
		return fmt.Errorf("Updated & removed nodes commit failed, details: %v", err)
	}

	updatedNodesInactiveIds := make([]cas.BlobsPayload[btree.UUID], len(t.updatedNodeHandles))
	for i := range t.updatedNodeHandles {
		updatedNodesInactiveIds[i].BlobTable = btree.ConvertToBlobTableName(t.updatedNodeHandles[i].RegistryTable)
		updatedNodesInactiveIds[i].Blobs = make([]btree.UUID, len(t.updatedNodeHandles[i].IDs))
		for ii := range t.updatedNodeHandles[i].IDs {
			// Since we've flipped the inactive to active, the new inactive Id is to be flushed out of Redis cache.
			updatedNodesInactiveIds[i].Blobs[ii] = t.updatedNodeHandles[i].IDs[ii].GetInActiveId()
		}
	}

	// Package the logically deleted Ids for actual physical deletes.
	deletedIds := make([]cas.RegistryPayload[btree.UUID], len(t.removedNodeHandles), len(t.updatedNodeHandles)+len(t.removedNodeHandles))
	for i := range t.removedNodeHandles {
		deletedIds[i].RegistryTable = t.removedNodeHandles[i].RegistryTable
		deletedIds[i].IDs = make([]btree.UUID, len(t.removedNodeHandles[i].IDs))
		for ii := range t.removedNodeHandles[i].IDs {
			// Removed nodes are marked deleted, thus, its active node Id can be safely removed.
			deletedIds[i].IDs[ii] = t.removedNodeHandles[i].IDs[ii].LogicalId
		}
	}
	t.deleteEntries(ctx, deletedIds, updatedNodesInactiveIds)

	// Unlock the items in Redis.
	t.logger.log(unlockTrackedItems)
	if err := t.unlockTrackedItems(ctx); err != nil {
		// Just log as warning any error as at this point, commit is already finalized.
		// Any partial changes before failure in unlock tracked items will just expire in Redis.
		log.Warn(err.Error())
	}
	return nil
}

// Checks if fetched items are intact.
func (t *transaction) commitForReaderTransaction(ctx context.Context) error {
	if t.forWriting {
		return nil
	}
	// For a reader transaction, conflict check is enough.
	startTime := getCurrentTime()
	for {
		if err := t.timedOut(ctx, startTime); err != nil {
			return err
		}
		// Check items if have not changed since fetching.
		_, _, _, fetchedNodes, _ := t.classifyModifiedNodes()
		if ok, err := t.btreesBackend[0].nodeRepository.areFetchedNodesIntact(ctx, fetchedNodes); err != nil {
			return err
		} else if ok {
			return nil
		}

		// Sleep in random seconds to allow different conflicting (Node modifying) transactions
		// (in-flight) to retry on different times.
		sleepTime := rand.Intn(4+1) + 5
		sleep(ctx, sleepTime)

		// Recreate the fetches on latest committed nodes & check if fetched Items are unchanged.
		if err := t.refetchAndMergeModifications(ctx); err != nil {
			return err
		}
	}
}

// Use tracked Items to refetch their Nodes(using B-Tree) and merge the changes in, if there is no conflict.
func (t *transaction) refetchAndMergeModifications(ctx context.Context) error {
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
				cacheNode.action == addAction && t.btreesBackend[i].getStoreInfo().RootNodeId == cacheNode.node.(btree.MetaDataType).GetId() {
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
		// Compute the count delta so Store Repository can reconcile.
		store.CountDelta = store.Count - t.btreesBackend[i].nodeRepository.count
		stores[i] = *store
	}
	return t.storeRepository.Update(ctx, stores...)
}

func (t *transaction) lockTrackedItems(ctx context.Context) error {
	for _, s := range t.btreesBackend {
		if err := s.lockTrackedItems(ctx, t.redisCache, t.maxTime); err != nil {
			return err
		}
	}
	return nil
}

func (t *transaction) unlockTrackedItems(ctx context.Context) error {
	var lastErr error
	for _, s := range t.btreesBackend {
		if err := s.unlockTrackedItems(ctx, t.redisCache); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Delete the registry entries and their node blobs. These type of deletes will be rare as trie nodes don't
// get deleted unless entire set of items in it were deleted.
// The second parameter(inactiveBlobIds), their nodes will just be flushed out of Redis.
func (t *transaction) deleteEntries(ctx context.Context,
	deletedRegistryIds []cas.RegistryPayload[btree.UUID], inactiveBlobIds []cas.BlobsPayload[btree.UUID]) {
	if len(deletedRegistryIds) == 0 && len(inactiveBlobIds) == 0 {
		return
	}
	if len(inactiveBlobIds) > 0 {
		// Delete from Redis the inactive nodes.
		// Leave the registry keys as there may be other in-flight transactions that need them
		// for conflict resolution, to rollback or to fail their "reader" transaction.
		deletedKeys := make([]string, cas.GetBlobPayloadCount[btree.UUID](inactiveBlobIds))
		ik := 0
		for i := range inactiveBlobIds {
			for ii := range inactiveBlobIds[i].Blobs {
				deletedKeys[ik] = t.btreesBackend[0].nodeRepository.formatKey(inactiveBlobIds[i].Blobs[ii].ToString())
				ik++
			}
		}
		if err := t.redisCache.Delete(ctx, deletedKeys...); err != nil && !redis.KeyNotFound(err) {
			log.Error("Redis Delete failed, details: %v", err)
		}
		// Only attempt to send the delete message to Kafka if the delete service is enabled.
		if IsDeleteServiceEnabled {
			_, err := kafka.Enqueue[[]cas.BlobsPayload[btree.UUID]](ctx, inactiveBlobIds)
			if err != nil {
				log.Error("Kafka Enqueue failed, details: %v", err)
			}
		} else {
			log.Warn("DeleteService is not enabled, skipping send of delete message to Kafka")
		}
	}

	// Create the delete blobs payload request.
	blobsIdsForDelete := make([]cas.BlobsPayload[btree.UUID], len(deletedRegistryIds))
	for i := range deletedRegistryIds {
		blobsIdsForDelete[i] = cas.BlobsPayload[btree.UUID]{
			BlobTable: btree.ConvertToBlobTableName(deletedRegistryIds[i].RegistryTable),
			Blobs:     make([]btree.UUID, len(deletedRegistryIds[i].IDs)),
		}
		for ii := range deletedRegistryIds[i].IDs {
			blobsIdsForDelete[i].Blobs[ii] = deletedRegistryIds[i].IDs[ii]
		}
	}
	// Delete the registry entries & their referenced node blobs.
	t.nodeBlobStore.Remove(ctx, blobsIdsForDelete...)
	t.registry.Remove(ctx, deletedRegistryIds...)
}

func (t *transaction) rollback(ctx context.Context, forRetry bool) error {
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
		// do nothing as the function failed, nothing to undo.
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
	// Don't unlock tracked item since rollback is for retry, inner scope of tracked items locking.
	if forRetry {
		// Rewind the transactoin log in case retry will check it.
		t.logger.log(commitNewRootNodes)
		return lastErr
	}
	if t.logger.committedState > lockTrackedItems {
		if err := t.unlockTrackedItems(ctx); err != nil {
			lastErr = err
		}
	}

	return lastErr
}
