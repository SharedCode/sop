package in_memory

import (
	"context"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
)

// in-memory implementation of NodeRepository. Uses a map to manage nodes in memory.
type nodeRepository[TK btree.Comparable, TV any] struct {
	lookup map[sop.UUID]*btree.Node[TK, TV]
}

// NewNodeRepository instantiates a NodeRepository that uses a map to manage items.
func newNodeRepository[TK btree.Comparable, TV any]() btree.NodeRepository[TK, TV] {
	return &nodeRepository[TK, TV]{
		lookup: make(map[sop.UUID]*btree.Node[TK, TV]),
	}
}

// Add will upsert node to the map.
func (nr *nodeRepository[TK, TV]) Add(n *btree.Node[TK, TV]) {
	nr.lookup[n.Id] = n
}

// Update will upsert node to the map.
func (nr *nodeRepository[TK, TV]) Update(n *btree.Node[TK, TV]) {
	nr.lookup[n.Id] = n
}

// Get will retrieve a node with nodeId from the map.
func (nr *nodeRepository[TK, TV]) Get(ctx context.Context, nodeId sop.UUID) (*btree.Node[TK, TV], error) {
	v, _ := nr.lookup[nodeId]
	return v, nil
}

// Fetched does nothing for in-memory.
func (nr *nodeRepository[TK, TV]) Fetched(nodeId sop.UUID) {}

// Remove will remove a node with nodeId from the map.
func (nr *nodeRepository[TK, TV]) Remove(nodeId sop.UUID) {
	delete(nr.lookup, nodeId)
}
