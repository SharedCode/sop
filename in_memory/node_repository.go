package in_memory

import (
	"context"

	"github.com/SharedCode/sop/btree"
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

// Upsert will upsert node to the map.
func (nr *nodeRepository[TK, TV]) Upsert(ctx context.Context, n *btree.Node[TK, TV]) error {
	nr.lookup[n.Id] = n
	return nil
}

// Get will retrieve a node with nodeId from the map.
func (nr *nodeRepository[TK, TV]) Get(ctx context.Context, nodeId btree.UUID) (*btree.Node[TK, TV], error) {
	v, _ := nr.lookup[nodeId]
	return v, nil
}

// Remove will remove a node with nodeId from the map.
func (nr *nodeRepository[TK, TV]) Remove(ctx context.Context, nodeId btree.UUID) error {
	delete(nr.lookup, nodeId)
	return nil
}
