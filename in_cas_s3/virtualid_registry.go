package in_cas_s3

import (
	"github.com/SharedCode/sop/btree"
)

// VirtualIdRegistry interface specifies the "virtualized Id" repository.
type VirtualIdRegistry interface {
	Get(lid btree.UUID) (Handle, error)
	Add(Handle) error
	Update(Handle) error
	Remove(lid btree.UUID) error
}

type vid_registry struct{}

func newVirtualIdRegistry() VirtualIdRegistry {
	return &vid_registry{}
}

func (conn *vid_registry) Add(h Handle) error {
	return nil
}

func (conn *vid_registry) Update(h Handle) error {
	return nil
}
func (conn *vid_registry) Get(logicalID btree.UUID) (Handle, error) {
	return Handle{
		LogicalId:   logicalID,
		PhysicalIdA: logicalID,
	}, nil
}
func (conn *vid_registry) Remove(logicalID btree.UUID) error {
	return nil
}
