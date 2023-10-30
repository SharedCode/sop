package in_memory

import "github.com/SharedCode/sop/btree"

type nodeRepository[TKey btree.Comparable, TValue any] struct {
	lookup map[btree.UUID]*btree.Node[TKey, TValue]
}

func NewNodeRepository[TKey btree.Comparable, TValue any]() btree.NodeRepository[TKey, TValue] {
	return &nodeRepository[TKey, TValue]{
		lookup: make(map[btree.UUID]*btree.Node[TKey, TValue]),
	}
}

func (nr *nodeRepository[TKey, TValue]) Add(n *btree.Node[TKey, TValue]) error {
	nr.lookup[n.Id] = n
	return nil
}

func (nr *nodeRepository[TKey, TValue]) Update(n *btree.Node[TKey, TValue]) error {
	nr.lookup[n.Id] = n
	return nil
}
func (nr *nodeRepository[TKey, TValue]) Get(nodeId btree.UUID) (*btree.Node[TKey, TValue], error) {
	v, _ := nr.lookup[nodeId]
	return v, nil
}

func (nr *nodeRepository[TKey, TValue]) Remove(nodeId btree.UUID) error {
	delete(nr.lookup, nodeId)
	return nil
}
