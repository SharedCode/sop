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

// nodeRepository implementation for "cassandra-S3"(in_cas_s3) exposes a standard NodeRepository interface
// but which, manages b-tree nodes in transaction cache, Redis and in Cassandra + S3,
// or File System, for debugging &/or "poor man's" setup(no AWS required!).
type nodeRepository[TK btree.Comparable, TV any] struct {
	// Needed by NodeRepository for Node data merging to the backend storage systems.
	nodeRedisCache redis.Cache
	nodeBlobStore  s3.BlobStore
	nodeLocalCache map[btree.UUID]cacheNode
}

// NewNodeRepository instantiates a NodeRepository.
func newNodeRepository[TK btree.Comparable, TV any]() *nodeRepository[TK, TV] {
	return &nodeRepository[TK, TV]{
		nodeLocalCache: make(map[btree.UUID]cacheNode),
		nodeRedisCache: redis.NewClient(redis.DefaultOptions()),
		nodeBlobStore: s3.NewBlobStore(),
	}
}

// Add will upsert node to the map.
func (nr *nodeRepository[TK, TV]) Add(n *btree.Node[TK, TV]) {
	nr.add(n.Id, n)
}

// Update will upsert node to the map.
func (nr *nodeRepository[TK, TV]) Update(n *btree.Node[TK, TV]) {
	nr.update(n.Id, n)
}

// Get will retrieve a node with nodeId from the map.
func (nr *nodeRepository[TK, TV]) Get(ctx context.Context, nodeId btree.UUID) (*btree.Node[TK, TV], error) {
	n, err := nr.get(ctx, nodeId)
	return n.(*btree.Node[TK, TV]), err
}

// Remove will remove a node with nodeId from the map.
func (nr *nodeRepository[TK, TV]) Remove( nodeId btree.UUID) {
	nr.Remove(nodeId)
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
func (nr *nodeRepository[TK, TV]) get(ctx context.Context, nodeId btree.UUID) (interface{}, error) {
	if v, ok := nr.nodeLocalCache[nodeId]; ok {
		if v.action == removeAction {
			return nil, nil
		}
		return v.node, nil
	}
	var node btree.Node[TK, TV]
	if err := nr.nodeRedisCache.GetStruct(ctx, nodeId.ToString(), &node); err != nil {
		if redis.KeyNotFound(err) {
			// Fetch from blobStore and cache to Redis/local.
			if err = nr.nodeBlobStore.Get(ctx, nodeId, &node); err != nil {
				return nil, err
			}
			nr.nodeRedisCache.SetStruct(ctx, nodeId.ToString(), interface{}(&node), -1)
			nr.nodeLocalCache[nodeId] = cacheNode{
				action: getAction,
				node: &node,
			}
			return &node, nil
		}
		return nil, err
	}
	nr.nodeLocalCache[nodeId] = cacheNode{
		action: getAction,
		node: &node,
	}
	return &node, nil
}

func (nr *nodeRepository[TK, TV]) add(nodeId btree.UUID, node interface{}) {
	nr.nodeLocalCache[nodeId] = cacheNode{
		action: addAction,
		node: &node,
	}
}

func (nr *nodeRepository[TK, TV]) update(nodeId btree.UUID, node interface{}) {
	if v,ok := nr.nodeLocalCache[nodeId]; ok {
		// Update the node and keep the "action" marker if new, otherwise update to "update" action.
		v.node = node
		if v.action != addAction {
			v.action = updateAction
		}
		nr.nodeLocalCache[nodeId] = v
		return
	}
	nr.nodeLocalCache[nodeId] = cacheNode{
		action: updateAction,
		node: &node,
	}
}

func (nr *nodeRepository[TK, TV]) remove(nodeId btree.UUID) {
	if v,ok := nr.nodeLocalCache[nodeId]; ok {
		if v.action == addAction {
			delete(nr.nodeLocalCache, nodeId)
			return
		}
		v.action = removeAction
		nr.nodeLocalCache[nodeId] = v
		return
	}
	// Code should not reach this point, as B-tree will not issue a remove if node is not cached locally.
}
