package in_memory

import (
	"github.com/SharedCode/sop/btree"
)

// VirtualIdRepository is stubbed, 'just for demo in in_memory.

type vid_repository struct{}

func newVirtualIdRepository() btree.VirtualIdRepository {
	return &vid_repository{}
}

func (conn *vid_repository) Add(h btree.Handle) error {
	return nil
}

func (conn *vid_repository) Update(h btree.Handle) error {
	return nil
}
func (conn *vid_repository) Get(logicalID btree.UUID) (btree.Handle, error) {
	return btree.Handle{
		LogicalId:   logicalID,
		PhysicalIdA: logicalID,
	}, nil
}
func (conn *vid_repository) Remove(logicalID btree.UUID) error {
	return nil
}
