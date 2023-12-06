package in_cas_s3

import (
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_memory"
	"github.com/SharedCode/sop/in_cas_s3/redis"
)

// nodeRepository implementation for "cassandra-S3"(in_cas_s3) exposes a standard NodeRepository interface
// but which, manages b-tree nodes in transaction cache, Redis and in Cassandra + S3,
// or File System, for debugging &/or "poor man's" setup(no AWS required!).
type nodeRepository[TK btree.Comparable, TV any] struct {
	localCache in_memory.BtreeInterface[btree.UUID, interface{}]
	redisCache redis.Cache
	blobStore btree.BtreeInterface[btree.UUID, interface{}]
}

// NewNodeRepository instantiates a NodeRepository.
func newNodeRepository[TK btree.Comparable, TV any]() *nodeRepository[TK, TV] {
	return &nodeRepository[TK, TV]{
		localCache: in_memory.NewBtree[btree.UUID, interface{}](true),
		redisCache: redis.NewClient(redis.DefaultOptions()),
		// TODO: replace with real S3 or file system persisting repository.
		blobStore: in_memory.NewBtreeWithNoWrapper[btree.UUID, interface{}](true),
	}
}

// Upsert will upsert node to the map.
func (nr *nodeRepository[TK, TV]) Upsert(n *btree.Node[TK, TV]) error {
	return nr.upsert(n.Id, n)
}

// Get will retrieve a node with nodeId from the map.
func (nr *nodeRepository[TK, TV]) Get(nodeId btree.UUID) (*btree.Node[TK, TV], error) {
	n,err := nr.get(nodeId)
	return n.(*btree.Node[TK,TV]), err
}

// Remove will remove a node with nodeId from the map.
func (nr *nodeRepository[TK, TV]) Remove(nodeId btree.UUID) error {
	return nr.Remove(nodeId)
}


// Upsert will upsert node to the map.
func (nr *nodeRepository[TK, TV]) upsert(nodeId btree.UUID, node interface{}) error {
	return nil
}

// Get will retrieve a node with nodeId from the map.
func (nr *nodeRepository[TK, TV]) get(nodeId btree.UUID) (interface{}, error) {
	// // Somewhat implemented.
	// n, err := nr.redisCache.Get(nodeId)
	// if err != nil {
	// 	return nil, err
	// }
	// if n != nil {
	// 	return n, nil
	// }
	// n, err = nr.s3Repository.Get(nodeId)
	// if err != nil {
	// 	return nil, err
	// }
	// if n != nil {
	// 	err = nr.redisCache.Upsert(n)
	// 	if err != nil {
	// 		// TODO: Log redis cache error.
	// 	}
	// 	return n, nil
	// }
	return nil, nil
}

func (nr *nodeRepository[TK, TV]) remove(nodeId btree.UUID) error {
	return nr.Remove(nodeId)
}

/* Feature discussion:
  Transaction "session" logic:
    Get or Fetch:
	- If not found locally(& no remove marker) & found in blobStore, fetch data & populate local cache(& redis).
	  Return not found if there is a remove marker on it.
	Upsert:
	- Upsert to local cache if not yet, for upsert to blobStore(& redis) on transaction commit.
	  Mark data as new/modified.
	- Remove any marker for removal if there is.
	Remove:
	- Mark data as removed if not yet, for actual remove from blobStore(& redis) on transaction commit.

  Transaction commit logic:
    Applicable for both reader & writer transaction.
    1. Conflict Resolution:
	- Check all explicitly fetched data(i.e. - GetCurrentKey/GetCurrentValue invoked) if they have the
	  expected version number. If different, rollback.
	  Compare local vs redis/blobStore copy and see if different. Read from blobStore if not found in redis.

	Applicable for writer transaction.
	2. Save the inactive Node(s):
	NOTE: Any error in redis or Cassandra will return the error and should trigger a rollback.

	Upsert:
	- Upsert to blobStore
	- Upsert to Redis
	- Upsert to local cache

	Remove:
	- Remove from blobStore
	- Remove from Redis
	- Remove from local cache

	3. Mark inactive Node(s) as active (in both redis & Cassandra):
	- Mark all the affected Node(s)' virtual ID records as locked
	- Update the virtual ID records to make inactive as active
	- Mark all the affected Node(s)' virtual ID records as unlocked

	4. Mark transaction session as committed(done).
	- Clear all local cache created in the transaction.
	- Mark transaction as completed(hasBegun=false).
	- Mark transaction as unusable, a begin action to the same instance will return error.
	- All B-Tree instances that are bound to the transaction will now be unbound, thus, any action
	  on them will not be bound to any transaction, thus, activate the on-the-fly transaction wrapping.
*/
