package cassandra

import (
	"context"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
)

type VirtualIdRegistry interface {
	Get(ctx context.Context, lid btree.UUID) (sop.Handle, error)
	Add(ctx context.Context, h sop.Handle) error
	Update(ctx context.Context, h sop.Handle) error
	Remove(ctx context.Context, lid btree.UUID) error
}

type vid_registry struct{
	lookup map[btree.UUID]sop.Handle
}

// TODO: NewVirtualIdRegistry manages the Handle in Cassandra table.
func NewVirtualIdRegistry() VirtualIdRegistry {
	return &vid_registry{
		lookup: make(map[btree.UUID]sop.Handle),
	}
}

func (v *vid_registry) Add(ctx context.Context, h sop.Handle) error {
	v.lookup[h.LogicalId] = h
	return nil
}

func (v *vid_registry) Update(ctx context.Context, h sop.Handle) error {
	v.lookup[h.LogicalId] = h
	return nil
}
func (v *vid_registry) Get(ctx context.Context, logicalID btree.UUID) (sop.Handle, error) {
	h,_ := v.lookup[logicalID]
	return h, nil
}
func (v *vid_registry) Remove(ctx context.Context, logicalID btree.UUID) error {
	delete(v.lookup, logicalID)
	return nil
}
