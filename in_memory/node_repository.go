package in_memory

import (
	"context"

	"github.com/sharedcode/sop/btree"
)

// in-memory implementation of NodeRepository. Uses a map to manage nodes in memory.
type nodeRepository[TK btree.Comparable, TV any] struct {
	lookup map[btree.UUID]*btree.Node[TK, TV]
}

// NewNodeRepository instantiates a NodeRepository that uses a map to manage items.
func newNodeRepository[TK btree.Comparable, TV any]() btree.NodeRepository[TK, TV] {
	return &nodeRepository[TK, TV]{
		lookup: make(map[btree.UUID]*btree.Node[TK, TV]),
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
func (nr *nodeRepository[TK, TV]) Get(ctx context.Context, nodeId btree.UUID) (*btree.Node[TK, TV], error) {
	v, _ := nr.lookup[nodeId]
	return v, nil
}

// Remove will remove a node with nodeId from the map.
func (nr *nodeRepository[TK, TV]) Remove(nodeId btree.UUID) {
	delete(nr.lookup, nodeId)
}
