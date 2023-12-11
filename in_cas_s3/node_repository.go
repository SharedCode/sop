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
			// nr.nodeBlobStore.FindOne()
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

}

func (nr *nodeRepository[TK, TV]) update(nodeId btree.UUID, node interface{}) {

}

func (nr *nodeRepository[TK, TV]) remove(nodeId btree.UUID) {
}

/* Feature discussion:
  Transaction "session" logic(in NodeRepository):
    Get or Fetch:
	- If not found locally(& no remove marker) & found in blobStore, fetch data & populate local cache(& redis).
	  Return not found if found locally & there is a remove marker on it.
	Add:
	- Add to local cache if not yet, for add to blobStore(& redis) on transaction commit.
	  Mark data as new.
	Update:
	- Update to local cache if not yet, for update to blobStore(& redis) on transaction commit.
	  Mark data as modified.
	Remove:
	- If data is new(found in local cache only), then just remove from local cache.
	- Otherwise, mark data as removed, for actual remove from blobStore(& redis) on transaction commit.

  Transaction commit logic(in Transaction):
	NOTE: Any error in redis or Cassandra will return the error and should trigger a rollback. Writers will only
	work if redis and Cassandra are operational. Readers however, can still work despite redis failure(s).

	Reader transaction:
	- Check all explicitly fetched(i.e. - GetCurrentKey/GetCurrentValue invoked) & managed(add/update/remove) items
	  if they have the expected version number. If different, rollback.
	  Compare local vs redis/blobStore copy and see if different. Read from blobStore if not found in redis.
	  Commit to return error if there is at least an item with different version no. as compared to
	  local cache's copy.

	Writer transaction:
    1. Conflict Resolution:
	- Check all explicitly fetched(i.e. - GetCurrentKey/GetCurrentValue invoked) & managed(add/update/remove) items
	  if they have the expected version number. If different, rollback.
	  Compare local vs redis/blobStore copy and see if different. Read from blobStore if not found in redis.
	- Mark these items as locked in Redis.
	  Rollback if any failed to lock as alredy locked by another transaction. Or if Redis fetch failed(error).

	Applicable for writer transaction.
	2. Save the inactive Node(s):
	NOTE: a transaction Commit can timeout and thus, rollback if it exceeds the maximum time(defaults to 30 mins).

	Phase 1(modified Node(s) merging):
	NOTE: Return error to trigger rollback for any operation below that fails.
	- Create a lookup table of added/updated/removed items together with their Nodes
	  Specify whether Node is updated, added or removed
	* Repeat until timeout, for updated Nodes:
	- Upsert each Node from the lookup to blobStore(Add only if blobStore is S3)
	- Log UUID in transaction rollback log categorized as updated Node
	- Compare each updated Node to Redis copy if identical(active UUID is same)
	  NOTE: added Node(s) don't need this logic.
	  For identical Node(s), update the "inactive UUID" with the Node's UUID(in redis).
	  Collect each Node that are different in Redis(as updated by other transaction(s))
	  Gather all the items of these Nodes(using the lookup table)
	  Break if there are no more items different.
	- Re-fetch the Nodes of these items, re-create the lookup table consisting only of these items & their re-fetched Nodes
	- Loop end.
	- Return error if loop timed out to trigger rollback.

	* For removed Node(s):
	- Log removed Node(s) UUIDs in transaction rollback log categorized as removed Nodes.
	- Add removed Node(s) UUIDs to the trash bin so they can get physically removed later.
	* For newly added Node(s):
	- Log added Node(s) UUID(s) to transaction rollback log categorized as added virtual IDs.
	- Add added Node(s) UUID(s) to virtual ID registry(cassandra then redis)
	- Add added Node(s) data to Redis

	3. Mark inactive Node(s) as active (in both redis & Cassandra):
	NOTE: Return error to trigger rollback for any operation below that fails.
	- Mark all the updated Node(s)' virtual ID records as locked.
	  Detect if Node(s) in Redis had been modified, if yes, unlock them then return error to trigger rollback.
	- Update the virtual ID records to make inactive as active
	- Mark all the affected Node(s)' virtual ID records as unlocked
	- Mark all the items as unlocked in Redis
	- Delete the transaction logs for this transaction.

	4. Mark transaction session as committed(done).
	Transaction Cleanup:
	- Clear all local cache created in the transaction.
	- Mark transaction as completed(hasBegun=false).
	- Mark transaction as unusable, a begin action to the same instance will return error.
	- All B-Tree instances that are bound to the transaction will now be unbound, thus, any action
	  on them will not be bound to any transaction, thus, activate the on-the-fly transaction wrapping.

	5. Rollback
	- Read the transaction logs and delete all (temporary) data(in S3) created by this transaction or
	  mark "deleted=true" for the Cassandra records so they can be scheduled for deletion at a later, non-busy time.
	  Mark as appropriate according to different categories.
	- Call Transaction Cleanup to finalize rollback.
*/
