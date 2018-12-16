package store;

import "../btree"

type nc Connection

func (conn *nc) Add(n *btree.Node) error {
	return nil;
}

func (conn *nc) Update(n *btree.Node) error {
	return nil;
}
func (conn *nc) Get(nodeID btree.UUID) (*btree.Node, error) {
	return &btree.Node{}, nil;
}
func (conn *nc) Remove(nodeID btree.UUID) error {
	return nil;
}

// todo: node serialization functions here.
