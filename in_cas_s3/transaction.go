package in_cas_s3

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/SharedCode/sop/btree"
	cas "github.com/SharedCode/sop/in_cas_s3/cassandra"
	"github.com/SharedCode/sop/in_cas_s3/redis"
	"github.com/SharedCode/sop/in_cas_s3/s3"
)

// Transaction interface defines the "enduser facing" transaction methods.
type Transaction interface {
	// Begin the transaction.
	Begin() error
	// Commit the transaction.
	Commit(ctx context.Context) error
	// Rollback the transaction.
	Rollback(ctx context.Context) error
	// Returns true if transaction has begun, false otherwise.
	HasBegun() bool
}

type transaction struct {
	// stores(or its items) accessed/managed within the transaction session.
	btreesBackend []StoreInterface[interface{}, interface{}]
	btrees        []*btree.Btree[interface{}, interface{}]
	// across different transactions in same and/or different machines.
	// Needed by NodeRepository for Node data merging to the backend storage systems.
	// Needed by NodeRepository for Node data merging to the backend storage systems.
	nodeBlobStore      s3.BlobStore
	redisCache     redis.Cache
	storeRepository    cas.StoreRepository
	// VirtualIdRegistry is used to manage/access all objects keyed off of their virtual Ids (UUIDs).
	virtualIdRegistry cas.VirtualIdRegistry
	forWriting        bool
	hasBegun          bool
	done              bool
	maxTime           time.Duration
}

type nodeEntry struct {
	nodeId btree.UUID
	node   interface{}
}

// Use lambda for time.Now so automated test can replace with replayable time if needed.
var getCurrentTime = time.Now

// NewTransaction will instantiate a transaction object for writing(forWriting=true)
// or for reading(forWriting=false). Pass in -1 on maxTime to default to 15 minutes
// of session duration.
func NewTransaction(forWriting bool, maxTime time.Duration) Transaction {
	if maxTime <= 0 {
		m := 15
		maxTime = time.Duration(m * int(time.Minute))
	}
	return &transaction{
		forWriting: forWriting,
		maxTime:    maxTime,
		// TODO: Allow caller to supply Redis & blob store settings.
		storeRepository:    cas.NewStoreRepository(),
		virtualIdRegistry:  cas.NewVirtualIdRegistry(),
		redisCache: redis.NewClient(redis.DefaultOptions()),
		nodeBlobStore:  s3.NewBlobStore(),
	}
}

func (t *transaction) Begin() error {
	if t.done {
		return fmt.Errorf("Transaction is done, 'create a new one.")
	}
	if t.HasBegun() {
		return fmt.Errorf("Transaction is ongoing, 'can't begin again.")
	}
	t.hasBegun = true
	return nil
}

func (t *transaction) Commit(ctx context.Context) error {
	if t.done {
		return fmt.Errorf("Transaction is done, 'create a new one.")
	}
	if !t.HasBegun() {
		return fmt.Errorf("No transaction to commit, call Begin to start a transaction.")
	}
	t.hasBegun = false
	t.done = true
	if err := t.commit(ctx); err != nil {
		if rerr := t.rollback(ctx); rerr != nil {
			return fmt.Errorf("commit call failed, details: %v, rollback error: %v.", err, rerr)
		}
		return fmt.Errorf("commit call failed, details: %v.", err)
	}
	return t.cleanup(ctx)
}

func (t *transaction) Rollback(ctx context.Context) error {
	if t.done {
		return fmt.Errorf("Transaction is done, 'create a new one.")
	}
	if !t.HasBegun() {
		return fmt.Errorf("No transaction to rollback, call Begin to start a transaction.")
	}
	t.hasBegun = false
	t.done = true
	return t.rollback(ctx)
}

func (t *transaction) HasBegun() bool {
	return t.hasBegun
}

func (t *transaction) timedOut(startTime time.Time) error {
	if getCurrentTime().Sub(startTime).Minutes() > float64(t.maxTime) {
		return fmt.Errorf("Transaction timed out(maxTime=%v).", t.maxTime)
	}
	return nil
}

func (t *transaction) commit(ctx context.Context) error {
	// If reader transaction, only do a conflict check, that is enough.
	if !t.forWriting {
		if err := t.trackedItemsHasConflict(ctx); err != nil {
			return err
		}
		// Reader transaction only checks tracked items consistency, return success at this point.
		return nil
	}

	// Mark session modified items as locked in Redis. If lock or there is conflict, return it as error.
	if err := t.lockTrackedItems(ctx); err != nil {
		return err
	}

	var updatedNodes, removedNodes, addedNodes []nodeEntry
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
	// - Re-fetch the Nodes of these items, re-create the lookup table consisting only of these items & their re-fetched Nodes
	// - Loop end.
	// - Return error if loop timed out to trigger rollback.
	done := false
	for !done {
		if err := t.timedOut(startTime); err != nil {
			return err
		}

		done = true
		// Classify modified Nodes into update, remove and add. Updated & removed nodes are processed differently,
		// has to do merging & conflict resolution. Add is simple upsert.
		updatedNodes, removedNodes, addedNodes = t.classifyModifiedNodes()
		if ok, err := t.btreesBackend[0].backendNodeRepository.commitUpdatedNodes(ctx, updatedNodes); err != nil {
			return err
		} else if !ok {
			done = false
		}
		if !done {
			// Sleep in random seconds to allow different conflicting (Node modifying) transactions
			// (in-flight) to retry on different times.
			sleepTime := rand.Intn(4+1) + 5
			time.Sleep(time.Duration(sleepTime) * time.Second)
			if err := t.refetchAndMergeModifications(ctx); err != nil {
				return err
			}
		}
	}

	if err := t.btreesBackend[0].backendNodeRepository.commitAddedNodes(ctx, addedNodes); err != nil {
		return err
	}
	if ok, err := t.btreesBackend[0].backendNodeRepository.commitRemovedNodes(ctx, removedNodes); err != nil {
		return err
	} else if !ok {
		done = false
	}

	if err := t.storeRepository.CommitChanges(ctx); err != nil {
		return err
	}

	// Switch to active "state" the (inactive) updated/new Nodes so they will get started to be "seen" if fetched.
	if err := t.setActiveModifiedInactiveNodes(updatedNodes); err != nil {
		return err
	}
	// Unlock the items in Redis.
	if err := t.unlockTrackedItems(ctx); err != nil {
		return err
	}
	return nil
}

func (t *transaction) cleanup(ctx context.Context) error {
	sb := strings.Builder{}
	if err := t.deleteTransactionLogs(); err != nil {
		sb.WriteString(fmt.Sprintln(err.Error()))
	}
	// TODO: delete cached data sets of this transaction on Redis.
	if sb.Len() == 0 {
		return nil
	}
	return fmt.Errorf(sb.String())
}

func (t *transaction) deleteTransactionLogs() error {
	return nil
}

// Go through all Virtual IDs of the modified Nodes and update them so the inactive UUID becomes the active ones.
// Should be a lightweight operation & quick. Should use backend system's transaction for all or nothing commit.
func (t *transaction) setActiveModifiedInactiveNodes([]nodeEntry) error {

	return nil
}

// Use tracked Items to refetch their Nodes(using B-Tree) and merge the changes in.
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
			if ci.action == addAction {
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

			if ci.action == getAction {
				// GetCurrentItem call above already "marked" the "get" (or fetch) done.
				continue
			}
			if ci.action == removeAction {
				if ok, err := b3.RemoveCurrentItem(ctx); !ok || err != nil {
					if err != nil {
						return err
					}
					return fmt.Errorf("refetchAndMergeModifications failed to merge remove item with key %v.", ci.item.Key)
				}
				continue
			}
			if ci.action == updateAction {
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
func (t *transaction) classifyModifiedNodes() ([]nodeEntry, []nodeEntry, []nodeEntry) {
	var updatedNodes, removedNodes, addedNodes []nodeEntry
	for _, s := range t.btreesBackend {
		for nodeId, cacheNode := range s.backendNodeRepository.nodeLocalCache {
			switch cacheNode.action {
			case updateAction:
				updatedNodes = append(updatedNodes, nodeEntry{
					nodeId: nodeId,
					node:   cacheNode.node,
				})
			case removeAction:
				removedNodes = append(removedNodes, nodeEntry{
					nodeId: nodeId,
					node:   cacheNode.node,
				})
			case addAction:
				addedNodes = append(addedNodes, nodeEntry{
					nodeId: nodeId,
					node:   cacheNode.node,
				})
			}
		}
	}
	return updatedNodes, removedNodes, addedNodes
}

func (t *transaction) rollback(ctx context.Context) error {
	// TODO
	return t.cleanup(ctx)
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
	var lastError error
	for _, s := range t.btreesBackend {
		if err := s.backendItemActionTracker.unlock(ctx, t.redisCache); err != nil {
			lastError = err
		}
	}
	return lastError
}

// Check all explicitly fetched(i.e. - GetCurrentValue invoked) & managed(add/update/remove) items for conflict.
func (t *transaction) trackedItemsHasConflict(ctx context.Context) error {
	for _, s := range t.btreesBackend {
		if hasConflict, err := s.backendItemActionTracker.hasConflict(ctx, t.redisCache); hasConflict || err != nil {
			if hasConflict {
				return fmt.Errorf("hasConflict call detected conflict.")
			}
			return err
		}
	}
	return nil
}
