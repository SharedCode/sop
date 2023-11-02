package aws_s3

import "github.com/SharedCode/sop/btree"

type Node[TK btree.Comparable, TV any] btree.Node[TK, TV]

func NewNodeRepository[TK btree.Comparable, TV any]() btree.NodeRepository[TK, TV] {
	return &Node[TK, TV]{}
}

func (Node[TK, TV]) Add(n *btree.Node[TK, TV]) error {
	return nil
}

func (Node[TK, TV]) Update(n *btree.Node[TK, TV]) error {
	return nil
}
func (Node[TK, TV]) Get(nodeId btree.UUID) (*btree.Node[TK, TV], error) {
	return &btree.Node[TK, TV]{}, nil
}
func (Node[TK, TV]) Remove(nodeID btree.UUID) error {
	return nil
}
