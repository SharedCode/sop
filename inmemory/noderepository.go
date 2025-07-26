package inmemory

import (
	"context"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
)

// in-memory implementation of NodeRepository. Uses a map to manage nodes in memory.
type nodeRepository[TK btree.Ordered, TV any] struct {
	lookup map[sop.UUID]*btree.Node[TK, TV]
}

// NewNodeRepository instantiates a NodeRepository that uses a map to manage items.
func newNodeRepository[TK btree.Ordered, TV any]() btree.NodeRepository[TK, TV] {
	return &nodeRepository[TK, TV]{
		lookup: make(map[sop.UUID]*btree.Node[TK, TV]),
	}
}

// Add will upsert node to the map.
func (nr *nodeRepository[TK, TV]) Add(n *btree.Node[TK, TV]) {
	nr.lookup[n.ID] = n
}

// Update will upsert node to the map.
func (nr *nodeRepository[TK, TV]) Update(n *btree.Node[TK, TV]) {
	nr.lookup[n.ID] = n
}

// Get will retrieve a node with nodeID from the map.
func (nr *nodeRepository[TK, TV]) Get(ctx context.Context, nodeID sop.UUID) (*btree.Node[TK, TV], error) {
	v, _ := nr.lookup[nodeID]
	return v, nil
}

// Fetched does nothing for in-memory.
func (nr *nodeRepository[TK, TV]) Fetched(nodeID sop.UUID) {}

// Remove will remove a node with nodeID from the map.
func (nr *nodeRepository[TK, TV]) Remove(nodeID sop.UUID) {
	delete(nr.lookup, nodeID)
}
