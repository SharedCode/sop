package btree;

import "../../../btree"

// todo: change to some useful struct!
type VirtualID btree.VirtualID;

func NewVirtualIDRepository() btree.VirtualIDRepository{
	return VirtualID{};
}

func (VirtualID) Add(vid *btree.VirtualID) error {
	return nil;
}

func (VirtualID) Update(vid *btree.VirtualID) error {
	return nil;
}
func (VirtualID) Get(logicalID btree.UUID) (*btree.VirtualID, error) {
	return &btree.VirtualID{}, nil;
}
func (VirtualID) Remove(logicalID btree.UUID) error {
	return nil;
}

// todo: node serialization functions here.
