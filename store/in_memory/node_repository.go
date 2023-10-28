package in_memory

import "github.com/SharedCode/sop/btree"

type Node[TKey btree.Comparable, TValue any] btree.Node[TKey, TValue]

func NewNodeRepository[TKey btree.Comparable, TValue any]() btree.NodeRepository[TKey, TValue] {
	return Node[TKey, TValue]{}
}

func (Node[TKey, TValue]) Add(n *btree.Node[TKey, TValue]) error {
	return nil
}

func (Node[TKey, TValue]) Update(n *btree.Node[TKey, TValue]) error {
	return nil
}
func (Node[TKey, TValue]) Get(nodeID btree.UUID) (*btree.Node[TKey, TValue], error) {
	return &btree.Node[TKey, TValue]{}, nil
}
func (Node[TKey, TValue]) Remove(nodeID btree.UUID) error {
	return nil
}
