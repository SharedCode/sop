// Package Cassandra contains code for integration or inter-operation with Cassandra DB.
package cassandra

import (
	"context"

	"sop"
	"sop/btree"
)

// Virtual Id registry is essential in our support for all or nothing (sub)feature,
// which is essential in "fault tolerant" & "self healing" feature.
//
// All methods are taking in a set of items and need to be implemented to do
// all or nothing feature, e.g. wrapped in transaction in Cassandra.
type VirtualIdRegistry interface {
	Get(context.Context, ...btree.UUID) ([]sop.Handle, error)
	Add(context.Context, ...sop.Handle) error
	Update(context.Context, ...sop.Handle) error
	Remove(context.Context, ...btree.UUID) error
}

type vid_registry struct {
	lookup map[btree.UUID]sop.Handle
}

// TODO: NewVirtualIdRegistry manages the Handle in Cassandra table, cached in Redis.
func NewVirtualIdRegistry() VirtualIdRegistry {
	return &vid_registry{
		lookup: make(map[btree.UUID]sop.Handle),
	}
}

func (v *vid_registry) Add(ctx context.Context, handles ...sop.Handle) error {
	for _, h := range handles {
		v.lookup[h.LogicalId] = h
	}
	return nil
}

func (v *vid_registry) Update(ctx context.Context, handles ...sop.Handle) error {
	for _, h := range handles {
		v.lookup[h.LogicalId] = h
	}
	return nil
}
func (v *vid_registry) Get(ctx context.Context, logicalIds ...btree.UUID) ([]sop.Handle, error) {
	handles := make([]sop.Handle, 0, len(logicalIds))
	for _, lid := range logicalIds {
		h, _ := v.lookup[lid]
		handles = append(handles, h)
	}
	return handles, nil
}
func (v *vid_registry) Remove(ctx context.Context, logicalIDs ...btree.UUID) error {
	for _, lid := range logicalIDs {
		delete(v.lookup, lid)
	}
	return nil
}
