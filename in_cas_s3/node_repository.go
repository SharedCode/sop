package in_cas_s3

import (
	"context"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_cas_s3/redis"
)

type cacheNode struct {
	node   *btree.Node[interface{}, interface{}]
	action actionType
}

type nodeRepositoryTyped[TK btree.Comparable, TV any] struct {
	realNodeRepository *nodeRepository
}

// Now is a lambda expression that returns the current time in Unix milliseconds.
var Now = time.Now().UnixMilli

// Add will upsert node to the map.
func (nr *nodeRepositoryTyped[TK, TV]) Add(n *btree.Node[TK, TV]) {
	var intf interface{} = n
	nr.realNodeRepository.add(n.Id, intf.(*btree.Node[interface{}, interface{}]))
}

// Update will upsert node to the map.
func (nr *nodeRepositoryTyped[TK, TV]) Update(n *btree.Node[TK, TV]) {
	var intf interface{} = n
	nr.realNodeRepository.update(n.Id, intf.(*btree.Node[interface{}, interface{}]))
}

// Get will retrieve a node with nodeId from the map.
func (nr *nodeRepositoryTyped[TK, TV]) Get(ctx context.Context, nodeId btree.UUID) (*btree.Node[TK, TV], error) {
	var target btree.Node[TK, TV]
	var intf interface{} = &target
	n, err := nr.realNodeRepository.get(ctx, nodeId, intf.(*btree.Node[interface{}, interface{}]))
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
func (nr *nodeRepository) get(ctx context.Context, logicalId btree.UUID, target *btree.Node[interface{}, interface{}]) (interface{}, error) {
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
	nodeId := h[0].GetActiveId()
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
	target.UpsertTime = h[0].Timestamp
	nr.nodeLocalCache[logicalId] = cacheNode{
		action: getAction,
		node:   target,
	}
	return target, nil
}

func (nr *nodeRepository) add(nodeId btree.UUID, node *btree.Node[interface{}, interface{}]) {
	nr.nodeLocalCache[nodeId] = cacheNode{
		action: addAction,
		node:   node,
	}
}

func (nr *nodeRepository) update(nodeId btree.UUID, node *btree.Node[interface{}, interface{}]) {
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
func (nr *nodeRepository) commitUpdatedNodes(ctx context.Context, nodes []*btree.Node[interface{}, interface{}]) (bool, error) {
	// 1st pass, update the virtual Id registry ensuring the set of nodes are only being modified by us.
	nids := make([]btree.UUID, len(nodes))
	for i := range nodes {
		nids[i] = nodes[i].Id
	}
	handles, err := nr.transaction.virtualIdRegistry.Get(ctx, nids...)
	if err != nil {
		return false, err
	}
	blobs := make([]sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]], len(nodes))
	for i := range handles {
		// Node with such Id is marked deleted or had been updated since reading it.
		if handles[i].IsDeleted || handles[i].Timestamp != nodes[i].UpsertTime {
			return false, nil
		}
		// Create new phys. UUID and auto-assign it to the available phys. Id(A or B) "Id slot".
		id := handles[i].AllocateId()
		if id == btree.NilUUID {
			if handles[i].IsExpiredInactive() {
				iid := handles[i].GetInActiveId()
				// For now, 'ignore any error while trying to cleanup the expired inactive phys Id.
				if err := nr.transaction.nodeBlobStore.Remove(ctx, iid); err == nil {
					if err := nr.transaction.redisCache.Delete(ctx, iid.ToString()); err == nil || redis.KeyNotFound(err) {
						handles[i].ClearInactiveId()
						id = handles[i].AllocateId()
					}
				}
			}
		}
		if id == btree.NilUUID {
			// Return false as there is an ongoing update on node by another transaction.
			return false, nil
		}
		blobs[i].Key = handles[i].GetInActiveId()
		blobs[i].Value = nodes[i]
	}
	if err := nr.transaction.virtualIdRegistry.Update(ctx, handles...); err != nil {
		return false, err
	}

	// 2nd pass, persist the nodes blobs to blob store and redis cache.
	if err := nr.transaction.nodeBlobStore.Add(ctx, blobs...); err != nil {
		return false, err
	}
	for i := range nodes {
		if err := nr.transaction.redisCache.SetStruct(ctx, handles[i].GetInActiveId().ToString(), nodes[i], -1); err != nil {
			return false, err
		}
	}
	return true, nil
}

// Add the removed Node(s) and their Item(s) Data(if not in node segment) to the recycler
// so they can get serviced for physical delete on schedule in the future.
func (nr *nodeRepository) commitRemovedNodes(ctx context.Context, nodes []*btree.Node[interface{}, interface{}]) (bool, error) {
	nids := make([]btree.UUID, len(nodes))
	for i := range nodes {
		nids[i] = nodes[i].Id
	}
	handles, err := nr.transaction.virtualIdRegistry.Get(ctx, nids...)
	if err != nil {
		return false, err
	}
	for i := range handles {
		// Node with such Id is already marked deleted, is in-flight change or had been updated since reading it,
		// fail it for "refetch" & retry.
		if handles[i].IsDeleted || handles[i].IsAandBinUse() || handles[i].Timestamp != nodes[i].UpsertTime {
			return false, nil
		}
		// Mark Id as deleted.
		handles[i].IsDeleted = true
		handles[i].WorkInProgressTimestamp = Now()
	}
	// Persist the handles changes.
	if err := nr.transaction.virtualIdRegistry.Update(ctx, handles...); err != nil {
		return false, err
	}
	return true, nil
}

func (nr *nodeRepository) commitAddedNodes(ctx context.Context, nodes []*btree.Node[interface{}, interface{}]) error {
	/* UUID to Virtual Id story:
	   - (on commit) New(added) nodes will have their Ids converted to virtual Id with empty
	     phys Ids(or same Id with active & virtual Id).
	   - On get, 'will read the Node using currently active Id.
	   - (on commit) On update, 'will save and register the node phys Id to the "inactive Id" part of the virtual Id.
	   - On finalization of commit, inactive will be switched to active (node) Ids.
	*/
	handles := make([]sop.Handle, len(nodes))
	blobs := make([]sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]], len(nodes))
	for i := range nodes {
		// Add node to blob store.
		h := sop.NewHandle(nodes[i].Id)
		// Update upsert time.
		h.Timestamp = Now()
		blobs[i].Key = nodes[i].Id
		blobs[i].Value = nodes[i]
		handles[i] = h
		// Add node to Redis cache.
		if err := nr.transaction.redisCache.SetStruct(ctx, nodes[i].Id.ToString(), nodes[i], -1); err != nil {
			return err
		}
	}
	// Register node Id as logical Id(handle).
	if err := nr.transaction.virtualIdRegistry.Add(ctx, handles...); err != nil {
		return err
	}
	// Add nodes to blob store.
	if err := nr.transaction.nodeBlobStore.Add(ctx, blobs...); err != nil {
		return err
	}
	return nil
}

func (nr *nodeRepository) areFetchedNodesIntact(ctx context.Context, nodes []*btree.Node[interface{}, interface{}]) (bool, error) {
	nids := make([]btree.UUID, len(nodes))
	for i := range nodes {
		nids[i] = nodes[i].Id
	}
	handles, err := nr.transaction.virtualIdRegistry.Get(ctx, nids...)
	if err != nil {
		return false, err
	}
	for i := range handles {
		// Node with Id had been updated(or deleted) since reading it.
		if handles[i].Timestamp != nodes[i].UpsertTime {
			return false, nil
		}
	}
	return true, nil
}

func (nr *nodeRepository) rollbackAddedNodes(ctx context.Context, nodes []*btree.Node[interface{}, interface{}]) error {
	ids := make([]btree.UUID, len(nodes))
	for i := range nodes {
		ids[i] = nodes[i].Id
	}
	for _, id := range ids {
		// Remove node from Redis cache.
		if err := nr.transaction.redisCache.Delete(ctx, id.ToString()); err != nil && !redis.KeyNotFound(err) {
			return err
		}
	}
	// Remove nodes from blob store.
	if err := nr.transaction.nodeBlobStore.Remove(ctx, ids...); err != nil {
		return err
	}
	// Unregister nodes Ids.
	if err := nr.transaction.virtualIdRegistry.Remove(ctx, ids...); err != nil {
		return err
	}
	return nil
}

func (nr *nodeRepository) rollbackUpdatedNodes(ctx context.Context, nodes []*btree.Node[interface{}, interface{}]) error {
	nids := make([]btree.UUID, len(nodes))
	for i := range nodes {
		nids[i] = nodes[i].Id
	}
	handles, err := nr.transaction.virtualIdRegistry.Get(ctx, nids...)
	if err != nil {
		return err
	}
	iids := make([]btree.UUID, len(nodes))
	for i := range handles {
		iids[i] = handles[i].GetInActiveId()
		handles[i].ClearInactiveId()
	}
	// Undo the nodes blobs to blob store and redis cache.
	for _, iid := range iids {
		if err = nr.transaction.redisCache.Delete(ctx, iid.ToString()); err != nil && !redis.KeyNotFound(err){
			return err
		}
	}
	if err = nr.transaction.nodeBlobStore.Remove(ctx, iids...); err != nil {
		return err
	}
	// Undo changes in virtual Id registry.
	if err = nr.transaction.virtualIdRegistry.Update(ctx, handles...); err != nil {
		return err
	}
	return nil
}

func (nr *nodeRepository) rollbackRemovedNodes(ctx context.Context, nodes []*btree.Node[interface{}, interface{}]) error {
	nids := make([]btree.UUID, len(nodes))
	for i := range nodes {
		nids[i] = nodes[i].Id
	}
	handles, err := nr.transaction.virtualIdRegistry.Get(ctx, nids...)
	if err != nil {
		return err
	}
	for i := range handles {
		// Undo the deleted mark for Id.
		handles[i].IsDeleted = false
		handles[i].WorkInProgressTimestamp = 0
	}

	// Persist the handles changes.
	return nr.transaction.virtualIdRegistry.Update(ctx, handles...)
}

// Set to active the inactive nodes. This is the last persistence step in transaction commit.
func (nr *nodeRepository) activateInactiveNodes(ctx context.Context, nodes []*btree.Node[interface{}, interface{}]) ([]sop.Handle, error) {
	nids := make([]btree.UUID, len(nodes))
	for i := range nodes {
		nids[i] = nodes[i].Id
	}
	handles, err := nr.transaction.virtualIdRegistry.Get(ctx, nids...)
	if err != nil {
		return nil, err
	}
	for i := range nodes {
		// Set the inactive as active Id.
		handles[i].FlipActiveId()
		// Update upsert time, we are finalizing the commit for the node.
		handles[i].Timestamp = Now()
		// Set work in progress timestamp to now as safety. After flipping inactive to active,
		// the previously active Id if not "cleaned up" then this timestamp will allow future
		// transactions to clean it up(self healing).
		handles[i].WorkInProgressTimestamp = Now()
	}
	// All or nothing batch update.
	return handles, nil
}

// Update upsert time of a given set of nodes.
func (nr *nodeRepository) touchNodes(ctx context.Context, nodes []*btree.Node[interface{}, interface{}]) ([]sop.Handle, error) {
	nids := make([]btree.UUID, len(nodes))
	for i := range nodes {
		nids[i] = nodes[i].Id
	}
	handles, err := nr.transaction.virtualIdRegistry.Get(ctx, nids...)
	if err != nil {
		return nil, err
	}
	for i := range handles {
		// Update upsert time, we are finalizing the commit for the node.
		handles[i].Timestamp = Now()
		handles[i].WorkInProgressTimestamp = 0
	}
	// All or nothing batch update.
	return handles, nil
}
