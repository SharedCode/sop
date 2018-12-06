package btree;

import "../../../btree"

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
func (Node) Get(nodeID btree.UUID) (*btree.Node, error) {
	return &btree.Node{}, nil;
}
func (Node) Remove(nodeID btree.UUID) error {
	return nil;
}

// todo: node serialization functions here.
