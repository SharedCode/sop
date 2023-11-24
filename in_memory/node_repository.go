package btree

// in-memory implementation of NodeRepository. Uses a map to manage nodes in memory.
type inMemoryNodeRepository[TK Comparable, TV any] struct {
	lookup map[UUID]*Node[TK, TV]
}

// NewInMemoryNodeRepository instantiates a NodeRepository that uses a map to manage items.
// NOTE: this is defined here to enable mock testing in this directory.
func NewInMemoryNodeRepository[TK Comparable, TV any]() NodeRepository[TK, TV] {
	return &inMemoryNodeRepository[TK, TV]{
		lookup: make(map[UUID]*Node[TK, TV]),
	}
}

// Upsert will upsert node to the map.
func (nr *inMemoryNodeRepository[TK, TV]) Upsert(n *Node[TK, TV]) error {
	nr.lookup[n.Id] = n
	return nil
}

// Get will retrieve a node with nodeId from the map.
func (nr *inMemoryNodeRepository[TK, TV]) Get(nodeId UUID) (*Node[TK, TV], error) {
	v, _ := nr.lookup[nodeId]
	return v, nil
}

// Remove will remove a node with nodeId from the map.
func (nr *inMemoryNodeRepository[TK, TV]) Remove(nodeId UUID) error {
	delete(nr.lookup, nodeId)
	return nil
}
