package store;

import "../btree"

type vc Connection

func (conn *vc) Add(vid *btree.VirtualID) error {
	return nil;
}

func (conn *vc) Update(vid *btree.VirtualID) error {
	return nil;
}
func (conn *vc) Get(logicalID btree.UUID) (*btree.VirtualID, error) {
	return &btree.VirtualID{}, nil;
}
func (conn *vc) Remove(logicalID btree.UUID) error {
	return nil;
}
