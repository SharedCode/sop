package in_cas_s3

import (
	"context"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_cas_s3/kafka"
	"github.com/SharedCode/sop/in_cas_s3/redis"
)

type cacheNode struct {
	node   interface{}
	action actionType
}

type nodeRepositoryTyped[TK btree.Comparable, TV any] struct {
	realNodeRepository *nodeRepository
}

// Add will upsert node to the map.
func (nr *nodeRepositoryTyped[TK, TV]) Add(n *btree.Node[TK, TV]) {
	nr.realNodeRepository.add(n.Id, n)
}

// Update will upsert node to the map.
func (nr *nodeRepositoryTyped[TK, TV]) Update(n *btree.Node[TK, TV]) {
	nr.realNodeRepository.update(n.Id, n)
}

// Get will retrieve a node with nodeId from the map.
func (nr *nodeRepositoryTyped[TK, TV]) Get(ctx context.Context, nodeId btree.UUID) (*btree.Node[TK, TV], error) {
	var target btree.Node[TK, TV]
	n, err := nr.realNodeRepository.get(ctx, nodeId, &target)
	return n.(*btree.Node[TK, TV]), err
}

// Remove will remove a node with nodeId from the map.
func (nr *nodeRepositoryTyped[TK, TV]) Remove(nodeId btree.UUID) {
	nr.realNodeRepository.remove(nodeId)
}

// nodeRepository implementation for "cassandra-S3"(in_cas_s3) exposes a standard NodeRepository interface
// but which, manages b-tree nodes in transaction cache, Redis and in Cassandra + S3,
// or File System, for debugging &/or "poor man's" setup(no AWS required!).
type nodeRepository struct {
	transaction *transaction
	// TODO: implement a MRU caching on node local cache so we only retain a handful in memory.
	nodeLocalCache map[btree.UUID]cacheNode
}

// NewNodeRepository instantiates a NodeRepository.
func newNodeRepository[TK btree.Comparable, TV any](t *transaction) *nodeRepositoryTyped[TK, TV] {
	nr := &nodeRepository{
		transaction:    t,
		nodeLocalCache: make(map[btree.UUID]cacheNode),
	}
	return &nodeRepositoryTyped[TK, TV]{
		realNodeRepository: nr,
	}
}

// Transaction "session" logic(in NodeRepository):
// Get or Fetch:
// - If not found locally(& no remove marker) & found in blobStore, fetch data & populate local cache(& redis).
//   Return not found if found locally & there is a remove marker on it.
// Add:
// - Add to local cache if not yet, for add to blobStore(& redis) on transaction commit.
//   Mark data as new.
// Update:
// - Update to local cache if not yet, for update to blobStore(& redis) on transaction commit.
//   Mark data as modified if not new.
// Remove:
// - If data is new(found in local cache only), then just remove from local cache.
// - Otherwise, mark data as removed, for actual remove from blobStore(& redis) on transaction commit.

// Get will retrieve a node with nodeId from the map.
func (nr *nodeRepository) get(ctx context.Context, logicalId btree.UUID, target interface{}) (interface{}, error) {
	if v, ok := nr.nodeLocalCache[logicalId]; ok {
		if v.action == removeAction {
			return nil, nil
		}
		return v.node, nil
	}
	h, err := nr.transaction.virtualIdRegistry.Get(ctx, logicalId)
	if err != nil {
		return nil, err
	}
	// Use active physical Id if in case different.
	nodeId := h.GetActiveId()
	if err := nr.transaction.redisCache.GetStruct(ctx, nodeId.ToString(), target); err != nil {
		if redis.KeyNotFound(err) {
			// Fetch from blobStore and cache to Redis/local.
			if err = nr.transaction.nodeBlobStore.Get(ctx, nodeId, target); err != nil {
				return nil, err
			}
			nr.transaction.redisCache.SetStruct(ctx, nodeId.ToString(), target, -1)
			nr.nodeLocalCache[logicalId] = cacheNode{
				action: getAction,
				node:   target,
			}
			return target, nil
		}
		return nil, err
	}
	n := target.(btree.Node[interface{},interface{}])
	n.UpsertTime = h.UpsertTime
	nr.nodeLocalCache[logicalId] = cacheNode{
		action: getAction,
		node:   target,
	}
	return target, nil
}

func (nr *nodeRepository) add(nodeId btree.UUID, node interface{}) {
	nr.nodeLocalCache[nodeId] = cacheNode{
		action: addAction,
		node:   node,
	}
}

func (nr *nodeRepository) update(nodeId btree.UUID, node interface{}) {
	if v, ok := nr.nodeLocalCache[nodeId]; ok {
		// Update the node and keep the "action" marker if new, otherwise update to "update" action.
		v.node = node
		if v.action != addAction {
			v.action = updateAction
		}
		nr.nodeLocalCache[nodeId] = v
		return
	}
	// Treat as add if not in local cache, because it should be there unless node is new.
	nr.nodeLocalCache[nodeId] = cacheNode{
		action: addAction,
		node:   node,
	}
}

func (nr *nodeRepository) remove(nodeId btree.UUID) {
	if v, ok := nr.nodeLocalCache[nodeId]; ok {
		if v.action == addAction {
			delete(nr.nodeLocalCache, nodeId)
			return
		}
		v.action = removeAction
		nr.nodeLocalCache[nodeId] = v
	}
	// Code should not reach this point, as B-tree will not issue a remove if node is not cached locally.
}

// Save to blob store, save node Id to the alternate(inactive) physical Id(see virtual Id).
func (nr *nodeRepository) commitUpdatedNodes(ctx context.Context, nodes []nodeEntry) (bool, error) {
	// 1st pass, update the virtual Id registry ensuring the set of nodes are only being modified by us.
	handles := make([]sop.Handle, len(nodes))
	for _, n := range nodes {
		h, err := nr.transaction.virtualIdRegistry.Get(ctx, n.nodeId)
		if err != nil {
			return false, err
		}
		// Node with such Id is marked deleted or had been updated since reading it.
		if h.IsDeleted || h.UpsertTime != n.node.(btree.Node[interface{}, interface{}]).UpsertTime{
			return false, nil
		}
		// Create new phys. UUID and auto-assign it to the available phys. Id(A or B) "Id slot".
		id := h.AllocateId()
		if id == btree.NilUUID {
			// Return false as there is an ongoing update on node by another transaction.
			return false, nil
		}
		if err := nr.transaction.virtualIdRegistry.Update(ctx, h); err != nil {
			return false, err
		}
		// Do a second "get" and check the lock id to see if we "won" the update, fail (for retry) if not.
		if h2, err := nr.transaction.virtualIdRegistry.Get(ctx, h.LogicalId); err != nil {
			return false, err
		} else if !h2.HasId(id) || h2.IsDeleted {
			return false, nil
		}
		handles = append(handles, h)
	}
	// 2nd pass, persist the nodes blobs to blob store and redis cache.
	for i, n := range nodes {
		h := handles[i]
		n.nodeId = h.GetInActiveId()
		// TODO: Should we support bulk update to blob store? Node tends to be medium-big data.
		if err := nr.transaction.nodeBlobStore.Add(ctx, n.nodeId, n); err != nil {
			return false, err
		}
		if err := nr.transaction.redisCache.SetStruct(ctx, n.nodeId.ToString(), n, -1); err != nil {
			return false, err
		}
	}
	return true, nil
}

// Add the removed Node(s) and their Item(s) Data(if not in node segment) to the recycler
// so they can get serviced for physical delete on schedule in the future.
func (nr *nodeRepository) commitRemovedNodes(ctx context.Context, nodes []nodeEntry) (bool, error) {
	deletedIds := make([]kafka.DeletedItem, len(nodes))
	for _, n := range nodes {
		h, err := nr.transaction.virtualIdRegistry.Get(ctx, n.nodeId)
		if err != nil {
			return false, err
		}
		// Node with such Id is already marked deleted, is in-flight change or had been updated since reading it,
		// fail it for "refetch" & retry.
		if h.IsDeleted || h.IsAandBinUse() || h.UpsertTime != n.node.(btree.Node[interface{}, interface{}]).UpsertTime {
			return false, nil
		}
		// Mark Id as deleted.
		h.IsDeleted = true
		if err := nr.transaction.virtualIdRegistry.Update(ctx, h); err != nil {
			return false, err
		}
		// Do a second "get" and check the lock id to see if we "won" the update, fail (for retry) if not.
		if h2, err := nr.transaction.virtualIdRegistry.Get(ctx, h.LogicalId); err != nil {
			return false, err
		} else if !h2.IsDeleted || h2.IsAandBinUse() {
			return false, nil
		}
		// Include this node for physical delete servicing.
		deletedIds = append(deletedIds, kafka.DeletedItem{
			ItemType: kafka.BtreeNode,
			ItemId: h.LogicalId,
		})
	}

	// Enqueue so deleted Ids' resources(nodes' or items values' blob) can get serviced for physical delete.
	if err := nr.transaction.deletedItemsQueue.Enqueue(ctx, deletedIds); err != nil {
		return false, err
	}

	return true, nil
}

func (nr *nodeRepository) commitAddedNodes(ctx context.Context, nodes []nodeEntry) error {
	/* UUID to Virtual Id story:
	   - (on commit) New(added) nodes will have their Ids converted to virtual Id with empty
	     phys Ids(or same Id with active & virtual Id).
	   - On get, 'will read the Node using currently active Id.
	   - (on commit) On update, 'will save and register the node phys Id to the "inactive Id" part of the virtual Id.
	   - On finalization of commit, inactive will be switched to active (node) Ids.
	*/
	for _, n := range nodes {
		// Add node to blob store.
		h := sop.NewHandle(n.nodeId)
		// Update upsert time.
		h.UpsertTime = time.Now().UnixMilli()
		// Register node Id as logical Id(handle).
		nr.transaction.virtualIdRegistry.Add(ctx, h)
		// Add node to blob store.
		if err := nr.transaction.nodeBlobStore.Add(ctx, n.nodeId, n); err != nil {
			return err
		}
		// Add node to Redis cache.
		if err := nr.transaction.redisCache.SetStruct(ctx, n.nodeId.ToString(), n, -1); err != nil {
			return err
		}
	}
	return nil
}

func (nr *nodeRepository) rollbackUpdatedNodes(ctx context.Context, nodes []nodeEntry) (bool, error) {
	var lastErr error
	for _, n := range nodes {
		h, err := nr.transaction.virtualIdRegistry.Get(ctx, n.nodeId)
		if err != nil {
			return false, err
		}
		// TODO: Should we support bulk update to blob store? Node tends to be medium-big data.
		inactiveId := h.PhysicalIdA
		if !h.IsActiveIdB {
			inactiveId = h.PhysicalIdB
		}
		if err := nr.transaction.nodeBlobStore.Remove(ctx, inactiveId); err != nil {
			lastErr = err
		}
		if err := nr.transaction.redisCache.Delete(ctx, inactiveId.ToString()); err != nil {
			if !redis.KeyNotFound(err) {
				lastErr = err
			}
		}
		if h.IsActiveIdB {
			h.PhysicalIdA = btree.NilUUID
		} else {
			h.PhysicalIdB = btree.NilUUID
		}
		if err := nr.transaction.virtualIdRegistry.Update(ctx, h); err != nil {
			lastErr = err
		}
	}
	return true, lastErr
}

func (nr *nodeRepository) rollbackRemovedNodes(ctx context.Context, nodes []nodeEntry) (bool, error) {
	var lastErr error
	for _, n := range nodes {
		h, err := nr.transaction.virtualIdRegistry.Get(ctx, n.nodeId)
		if err != nil {
			return false, err
		}
		// Mark Id as not deleted.
		h.IsDeleted = false
		if err := nr.transaction.virtualIdRegistry.Update(ctx, h); err != nil {
			lastErr = err
		}
	}
	return true, lastErr
}

// Set to active the inactive nodes. This is the last persistence step in transaction commit.
func (nr *nodeRepository) activateInactiveNodes(ctx context.Context, nodes []nodeEntry) (bool, error) {
	var lastErr error
	for _, n := range nodes {
		h, err := nr.transaction.virtualIdRegistry.Get(ctx, n.nodeId)
		if err != nil {
			return false, err
		}
		// Set the inactive as active Id.
		h.FlipActiveId()
		// Update upsert time, we are finalizing the commit for the node.
		h.UpsertTime = time.Now().UnixMilli()
		if err := nr.transaction.virtualIdRegistry.Update(ctx, h); err != nil {
			lastErr = err
		}
	}
	return true, lastErr
}
