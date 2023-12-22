package in_cas_s3

import (
	"context"

	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_cas_s3/redis"
	"github.com/SharedCode/sop/in_cas_s3/s3"
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
	// Needed by NodeRepository for Node data merging to the backend storage systems.
	nodeBlobStore  s3.BlobStore
	nodeRedisCache redis.Cache
	// TODO: implement a MRU caching on node local cache so we only retain a handful in memory.
	nodeLocalCache map[btree.UUID]cacheNode
}

// NewNodeRepository instantiates a NodeRepository.
func newNodeRepository[TK btree.Comparable, TV any]() *nodeRepositoryTyped[TK, TV] {
	nr := &nodeRepository{
		nodeLocalCache: make(map[btree.UUID]cacheNode),
		// TODO: Allow caller to supply Redis & blob store settings.
		nodeRedisCache: redis.NewClient(redis.DefaultOptions()),
		nodeBlobStore:  s3.NewBlobStore(),
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
func (nr *nodeRepository) get(ctx context.Context, nodeId btree.UUID, target interface{}) (interface{}, error) {
	if v, ok := nr.nodeLocalCache[nodeId]; ok {
		if v.action == removeAction {
			return nil, nil
		}
		return v.node, nil
	}
	if err := nr.nodeRedisCache.GetStruct(ctx, nodeId.ToString(), target); err != nil {
		if redis.KeyNotFound(err) {
			// Fetch from blobStore and cache to Redis/local.
			if err = nr.nodeBlobStore.Get(ctx, nodeId, target); err != nil {
				return nil, err
			}
			nr.nodeRedisCache.SetStruct(ctx, nodeId.ToString(), target, -1)
			nr.nodeLocalCache[nodeId] = cacheNode{
				action: getAction,
				node:   target,
			}
			return target, nil
		}
		return nil, err
	}
	nr.nodeLocalCache[nodeId] = cacheNode{
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
