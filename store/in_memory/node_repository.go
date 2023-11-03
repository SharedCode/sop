package in_memory

import "github.com/SharedCode/sop/btree"

type nodeRepository[TK btree.Comparable, TV any] struct {
	lookup map[btree.UUID]*btree.Node[TK, TV]
}

func newNodeRepository[TK btree.Comparable, TV any]() btree.NodeRepository[TK, TV] {
	return &nodeRepository[TK, TV]{
		lookup: make(map[btree.UUID]*btree.Node[TK, TV]),
	}
}

func (nr *nodeRepository[TK, TV]) Upsert(n *btree.Node[TK, TV]) error {
	nr.lookup[n.Id] = n
	return nil
}
func (nr *nodeRepository[TK, TV]) Get(nodeId btree.UUID) (*btree.Node[TK, TV], error) {
	v, _ := nr.lookup[nodeId]
	return v, nil
}

func (nr *nodeRepository[TK, TV]) Remove(nodeId btree.UUID) error {
	delete(nr.lookup, nodeId)
	return nil
}
