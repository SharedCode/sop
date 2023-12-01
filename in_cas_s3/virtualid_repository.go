package in_cas_s3

import (
	"github.com/SharedCode/sop/btree"
)

// VirtualIdRepository interface specifies the "virtualized Id" repository, a.k.a. Id registry.
type VirtualIdRepository interface {
	Get(lid btree.UUID) (Handle, error)
	Add(Handle) error
	Update(Handle) error
	Remove(lid btree.UUID) error
}

type vid_repository struct{}

func newVirtualIdRepository() VirtualIdRepository {
	return &vid_repository{}
}

func (conn *vid_repository) Add(h Handle) error {
	return nil
}

func (conn *vid_repository) Update(h Handle) error {
	return nil
}
func (conn *vid_repository) Get(logicalID btree.UUID) (Handle, error) {
	return Handle{
		LogicalId:   logicalID,
		PhysicalIdA: logicalID,
	}, nil
}
func (conn *vid_repository) Remove(logicalID btree.UUID) error {
	return nil
}
