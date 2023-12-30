package in_cas_s3

import (
	"context"
	"fmt"
	log "log/slog"
	"math/rand"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	cas "github.com/SharedCode/sop/in_cas_s3/cassandra"
	"github.com/SharedCode/sop/in_cas_s3/kafka"
	q "github.com/SharedCode/sop/in_cas_s3/kafka"
	"github.com/SharedCode/sop/in_cas_s3/redis"
	"github.com/SharedCode/sop/in_cas_s3/s3"
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

type transaction struct {
	// B-Tree instances, & their backend bits, managed within the transaction session.
	btreesBackend []StoreInterface[interface{}, interface{}]
	btrees        []*btree.Btree[interface{}, interface{}]
	// Needed by NodeRepository for Node data merging to the backend storage systems.
	nodeBlobStore   s3.BlobStore
	redisCache      redis.Cache
	storeRepository cas.StoreRepository
	// VirtualIdRegistry manages the virtual Ids, a.k.a. "handle".
	virtualIdRegistry cas.VirtualIdRegistry
	deletedItemsQueue q.Queue[q.DeletedItem]
	// true if transaction allows upserts & deletes, false(read-only mode) otherwise.
	forWriting bool
	// -1 = intial state, 0 = began, 1 = phase 1 commit done, 2 = phase 2 commit or rollback done.
	phaseDone int
	maxTime   time.Duration
	logger    *transactionLog
	// Phase 1 commit generated objects required for phase 2 commit.
	updatedNodeHandles      []sop.Handle
	removedNodeHandles      []sop.Handle
}

// Use lambda for time.Now so automated test can replace with replayable time if needed.
var getCurrentTime = time.Now

// NewTwoPhaseCommitTransaction will instantiate a transaction object for writing(forWriting=true)
// or for reading(forWriting=false). Pass in -1 on maxTime to default to 15 minutes
// of session duration.
func NewTwoPhaseCommitTransaction(forWriting bool, maxTime time.Duration) TwoPhaseCommitTransaction {
	if maxTime <= 0 {
		m := 15
		maxTime = time.Duration(m * int(time.Minute))
	}
	return &transaction{
		forWriting: forWriting,
		maxTime:    maxTime,
		// TODO: Allow caller to supply Redis & blob store settings.
		storeRepository:   cas.NewStoreRepository(),
		virtualIdRegistry: cas.NewVirtualIdRegistry(),
		redisCache:        redis.NewClient(redis.DefaultOptions()),
		nodeBlobStore:     s3.NewBlobStore(),
		deletedItemsQueue: q.NewDeletedItemsQueue(),
		logger:            newTransactionLogger(),
		phaseDone:         -1,
	}
}

func (t *transaction) Begin() error {
	if t.HasBegun() {
		return fmt.Errorf("Transaction is ongoing, 'can't begin again.")
	}
	if t.phaseDone == 2 {
		return fmt.Errorf("Transaction is done, 'create a new one.")
	}
	t.phaseDone = 0
	return nil
}

func (t *transaction) Phase1Commit(ctx context.Context) error {
	if !t.HasBegun() {
		return fmt.Errorf("No transaction to commit, call Begin to start a transaction.")
	}
	if t.phaseDone == 2 {
		return fmt.Errorf("Transaction is done, 'create a new one.")
	}
	t.phaseDone = 1
	if !t.forWriting {
		return t.commitForReaderTransaction(ctx)
	}
	if err := t.phase1Commit(ctx); err != nil {
		t.phaseDone = 2
		if rerr := t.rollback(ctx); rerr != nil {
			return fmt.Errorf("Phase 1 commit failed, details: %v, rollback error: %v.", err, rerr)
		}
		return fmt.Errorf("Phase 1 commit failed, details: %v.", err)
	}
	return nil
}

func (t *transaction) Phase2Commit(ctx context.Context) error {
	if !t.HasBegun() {
		return fmt.Errorf("No transaction to commit, call Begin to start a transaction.")
	}
	if t.phaseDone == 0 {
		return fmt.Errorf("Phase 1 commit has not been invoke yet.")
	}
	if t.phaseDone == 2 {
		return fmt.Errorf("Transaction is done, 'create a new one.")
	}
	t.phaseDone = 2
	if !t.forWriting {
		return nil
	}
	if err := t.phase2Commit(ctx); err != nil {
		if rerr := t.rollback(ctx); rerr != nil {
			return fmt.Errorf("Phase 2 commit failed, details: %v, rollback error: %v.", err, rerr)
		}
		return fmt.Errorf("Phase 2 commit failed, details: %v.", err)
	}
	return nil
}

func (t *transaction) Rollback(ctx context.Context) error {
	if t.phaseDone == 2 {
		return fmt.Errorf("Transaction is done, 'create a new one.")
	}
	if !t.HasBegun() {
		return fmt.Errorf("No transaction to rollback, call Begin to start a transaction.")
	}
	// Reset transaction status and mark done to end it without persisting any change.
	t.phaseDone = 2
	return nil
}

func (t *transaction) HasBegun() bool {
	return t.phaseDone >= 0
}

func (t *transaction) timedOut(startTime time.Time) error {
	if getCurrentTime().Sub(startTime).Minutes() > float64(t.maxTime) {
		return fmt.Errorf("Transaction timed out(maxTime=%v).", t.maxTime)
	}
	return nil
}

func (t *transaction) phase1Commit(ctx context.Context) error {
	// Mark session modified items as locked in Redis. If lock or there is conflict, return it as error.
	t.logger.log(lockTrackedItems)
	if err := t.lockTrackedItems(ctx); err != nil {
		return err
	}

	var updatedNodes, removedNodes, addedNodes, fetchedNodes []*btree.Node[interface{}, interface{}]
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
		if err := t.timedOut(startTime); err != nil {
			return err
		}

		successful = true
		// Classify modified Nodes into update, remove and add. Updated & removed nodes are processed differently,
		// has to do merging & conflict resolution. Add is simple upsert.
		updatedNodes, removedNodes, addedNodes, fetchedNodes = t.classifyModifiedNodes()

		// Check for conflict on fetched nodes.
		ok, err := t.btreesBackend[0].backendNodeRepository.areFetchedNodesIntact(ctx, fetchedNodes)
		if err != nil {
			return err
		}
		if ok {
			// Commit updated nodes.
			t.logger.log(commitUpdatedNodes)
			if ok, err := t.btreesBackend[0].backendNodeRepository.commitUpdatedNodes(ctx, updatedNodes); err != nil {
				return err
			} else if !ok {
				successful = false
				if err := t.btreesBackend[0].backendNodeRepository.rollbackUpdatedNodes(ctx, updatedNodes); err != nil {
					return err
				}
			}
		} else {
			successful = false
		}

		// Only do commit removed nodes if successful so far.
		if len(removedNodes) > 0 && successful {
			// Commit removed nodes.
			t.logger.log(commitRemovedNodes)
			successful, err = t.btreesBackend[0].backendNodeRepository.commitRemovedNodes(ctx, removedNodes)
			if err != nil {
				return err
			}
		}
		if !successful {
			// Sleep in random seconds to allow different conflicting (Node modifying) transactions
			// (in-flight) to retry on different times.
			sleepTime := rand.Intn(4+1) + 5
			time.Sleep(time.Duration(sleepTime) * time.Second)

			// Recreate the changes on latest committed nodes, if there is no conflict.
			if err := t.refetchAndMergeModifications(ctx); err != nil {
				return err
			}
		}
	}

	// Commit added nodes.
	t.logger.log(commitAddedNodes)
	if err := t.btreesBackend[0].backendNodeRepository.commitAddedNodes(ctx, addedNodes); err != nil {
		return err
	}

	// Switch to active "state" the (inactive) updated Nodes so they will
	// get started to be "seen" in such state on succeeding fetch.
	uh, err := t.btreesBackend[0].backendNodeRepository.activateInactiveNodes(ctx, updatedNodes)
	if err != nil {
		return err
	}
	// Update upsert time of removed nodes to signal that they are finalized.
	rh, err := t.btreesBackend[0].backendNodeRepository.touchNodes(ctx, removedNodes)
	if err != nil {
		return err
	}

	// Populate the phase 2 commit required objects.
	t.updatedNodeHandles = uh
	t.removedNodeHandles = rh

	return nil
}

func (t *transaction) phase2Commit(ctx context.Context) error {
	// Finalize the commit, it is all or nothing action, thus, no partial failure/success.
	t.logger.log(finalizeCommit)
	if err := t.virtualIdRegistry.Update(ctx, append(t.updatedNodeHandles, t.removedNodeHandles...)...); err != nil {
		return err
	}

	// Assemble & enqueue the deleted Ids, 'should not fail.
	updatedNodesInactiveIds := make([]btree.UUID, len(t.updatedNodeHandles))
	deletedIds := make([]btree.UUID, len(t.removedNodeHandles))
	for i := range t.updatedNodeHandles {
		// Since we've flipped the inactive to active, the new inactive Id is to be deleted(unused).
		updatedNodesInactiveIds[i] = t.updatedNodeHandles[i].GetInActiveId()
		t.updatedNodeHandles[i].ClearInactiveId()
	}
	if err := t.virtualIdRegistry.Update(ctx, t.updatedNodeHandles...); err != nil {
		// Exclude the updated nodes inactive Ids for deletion because they failed getting cleared in registry.
		updatedNodesInactiveIds = nil
		log.Warn(err.Error())
	}
	for i := range t.removedNodeHandles {
		// Removed nodes are marked deleted, thus, its active node Id can be safely removed.
		deletedIds[i] = t.removedNodeHandles[i].GetActiveId()
	}
	if updatedNodesInactiveIds != nil {
		deletedIds = append(deletedIds, updatedNodesInactiveIds...)
	}
	t.enqueueRemovedIds(ctx, deletedIds...)

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
		if err := t.timedOut(startTime); err != nil {
			return err
		}
		// Check items if have not changed since fetching.
		_, _, _, fetchedNodes := t.classifyModifiedNodes()
		if ok, err := t.btreesBackend[0].backendNodeRepository.areFetchedNodesIntact(ctx, fetchedNodes); err != nil {
			return err
		} else if ok {
			return nil
		}
		// Sleep in random seconds to allow different conflicting (Node modifying) transactions
		// (in-flight) to retry on different times.
		sleepTime := rand.Intn(4+1) + 5
		time.Sleep(time.Duration(sleepTime) * time.Second)

		// Recreate the changes on latest committed nodes & check if fetched Nodes are unchanged.
		if err := t.refetchAndMergeModifications(ctx); err != nil {
			return err
		}
	}
}

// Use tracked Items to refetch their Nodes(using B-Tree) and merge the changes in, if there is no conflict.
func (t *transaction) refetchAndMergeModifications(ctx context.Context) error {
	for b3Index, b3 := range t.btrees {
		b3ModifiedItems := t.btreesBackend[b3Index].backendItemActionTracker.items
		// Clear the backend "cache" so we can force B-Tree to re-fetch from Redis(or BlobStore).
		t.btreesBackend[b3Index].backendItemActionTracker.items = make(map[btree.UUID]cacheItem)
		t.btreesBackend[b3Index].backendNodeRepository.nodeLocalCache = make(map[btree.UUID]cacheNode)
		// Reset StoreInfo of B-Tree in prep to replay the "actions".
		if storeInfo, err := t.storeRepository.Get(ctx, b3.StoreInfo.Name); err != nil {
			return err
		} else {
			b3.StoreInfo.Count = storeInfo.Count
			b3.StoreInfo.RootNodeId = storeInfo.RootNodeId
		}

		for itemId, ci := range b3ModifiedItems {
			if ci.Action == addAction {
				if ok, err := b3.Add(ctx, ci.item.Key, *ci.item.Value); !ok || err != nil {
					if err != nil {
						return err
					}
					return fmt.Errorf("refetchAndMergeModifications failed to merge add item with key %v", ci.item.Key)
				}
				continue
			}
			if ok, err := b3.FindOneWithId(ctx, ci.item.Key, itemId); !ok || err != nil {
				if err != nil {
					return err
				}
				return fmt.Errorf("refetchAndMergeModifications failed to find item with key %v.", ci.item.Key)
			}

			// Check if the item read from backend has been updated since the time we read it.
			if item, err := b3.GetCurrentItem(ctx); err != nil || item.UpsertTime != ci.upsertTimeInDB {
				if err != nil {
					return err
				}
				return fmt.Errorf("refetchAndMergeModifications detected a newer version of item with key %v.", ci.item.Key)
			}

			if ci.Action == getAction {
				// GetCurrentItem call above already "marked" the "get" (or fetch) done.
				continue
			}
			if ci.Action == removeAction {
				if ok, err := b3.RemoveCurrentItem(ctx); !ok || err != nil {
					if err != nil {
						return err
					}
					return fmt.Errorf("refetchAndMergeModifications failed to merge remove item with key %v.", ci.item.Key)
				}
				continue
			}
			if ci.Action == updateAction {
				if ok, err := b3.UpdateCurrentItem(ctx, *ci.item.Value); !ok || err != nil {
					if err != nil {
						return err
					}
					return fmt.Errorf("refetchAndMergeModifications failed to merge update item with key %v.", ci.item.Key)
				}
			}
		}
	}
	return nil
}

// classifyModifiedNodes will classify modified Nodes into 3 tables & return them:
// a. updated Nodes, b. removed Nodes, c. added Nodes, d. fetched Nodes.
func (t *transaction) classifyModifiedNodes() ([]*btree.Node[interface{}, interface{}], []*btree.Node[interface{}, interface{}], []*btree.Node[interface{}, interface{}], []*btree.Node[interface{}, interface{}]) {
	var updatedNodes, removedNodes, addedNodes, fetchedNodes []*btree.Node[interface{}, interface{}]
	for _, s := range t.btreesBackend {
		for _, cacheNode := range s.backendNodeRepository.nodeLocalCache {
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
	}
	return updatedNodes, removedNodes, addedNodes, fetchedNodes
}

func (t *transaction) lockTrackedItems(ctx context.Context) error {
	for _, s := range t.btreesBackend {
		if err := s.backendItemActionTracker.lock(ctx, t.redisCache, t.maxTime); err != nil {
			return err
		}
	}
	return nil
}

func (t *transaction) unlockTrackedItems(ctx context.Context) error {
	for _, s := range t.btreesBackend {
		if err := s.backendItemActionTracker.unlock(ctx, t.redisCache); err != nil {
			return err
		}
	}
	return nil
}

// Enqueue the deleted node Ids for scheduled physical delete.
func (t *transaction) enqueueRemovedIds(ctx context.Context, deletedNodeIds ...btree.UUID) {
	deletedItems := make([]kafka.DeletedItem, len(deletedNodeIds))
	for _, did := range deletedNodeIds {
		deletedItems = append(deletedItems, kafka.DeletedItem{
			ItemType: kafka.BtreeNode,
			ItemId:   did,
		})
	}
	// Enqueue to Kafka should not fail, but in any case, log as Error to log file as last resort.
	if err := t.deletedItemsQueue.Enqueue(ctx, deletedItems...); err != nil {
		log.Error(fmt.Sprintf("Failed to enqueue deleted nodes Ids: %v.", deletedItems))
	}
}

func (t *transaction) rollback(ctx context.Context) error {
	if t.logger.committedState == unlockTrackedItems {
		// This state should not be reached and rollback invoked, but return an error about it, in case.
		return fmt.Errorf("Transaction got committed, 'can't rollback it.")
	}

	updatedNodes, removedNodes, addedNodes, _ := t.classifyModifiedNodes()

	var lastErr error
	if t.logger.committedState == finalizeCommit {
		// do nothing as the function failed, nothing to undo.
	}
	if t.logger.committedState > commitAddedNodes {
		if err := t.btreesBackend[0].backendNodeRepository.rollbackAddedNodes(ctx, addedNodes); err != nil {
			lastErr = err
		}
	}
	if t.logger.committedState > commitRemovedNodes {
		if err := t.btreesBackend[0].backendNodeRepository.rollbackRemovedNodes(ctx, removedNodes); err != nil {
			lastErr = err
		}
	}
	if t.logger.committedState > commitUpdatedNodes {
		if err := t.btreesBackend[0].backendNodeRepository.rollbackUpdatedNodes(ctx, updatedNodes); err != nil {
			lastErr = err
		}
	}
	if t.logger.committedState > lockTrackedItems {
		if err := t.unlockTrackedItems(ctx); err != nil {
			lastErr = err
		}
	}

	return lastErr
}
