package btree;

import "sop/btree"

type Node btree.Node;

func NewNodeRepository() btree.NodeRepository{
	return Node{};
}

func (Node) Add(n *btree.Node) error {
	return nil;
}

func (Node) Update(n *btree.Node) error {
	return nil;
}
func (Node) Get(nodeID *btree.Handle) (*btree.Node, error) {
	return &btree.Node{}, nil;
}
func (Node) Remove(nodeID *btree.Handle) error {
	return nil;
}

// todo: node serialization functions here.
