package in_cas_s3

import (
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_cas_s3/redis"
)

// nodeRepository implementation for "cassandra-S3"(in_cas_s3) exposes a standard NodeRepository interface
// but which, manages b-tree nodes in transaction cache, Redis and in Cassandra + S3,
// or File System, for debugging & poor man's setup.
type nodeRepository[TK btree.Comparable, TV any] struct {
	localCache btree.BtreeInterface[btree.UUID, interface{}]
	redisCache redis.Cache
	s3Repository btree.BtreeInterface[btree.UUID, interface{}]
}

// NewNodeRepository instantiates a NodeRepository that uses a map to manage items.
func newNodeRepository[TK btree.Comparable, TV any]() *nodeRepository[TK, TV] {
	return &nodeRepository[TK, TV]{}
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
