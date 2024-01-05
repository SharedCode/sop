// Package Cassandra contains code for integration or inter-operation with Cassandra DB.
package cassandra

import (
	"context"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
)

type mock_vid_registry struct {
	lookup map[btree.UUID]sop.Handle
}

// NewMockVirtualIdRegistry manages the Handle in memory for mocking.
func NewMockVirtualIdRegistry() VirtualIdRegistry {
	return &mock_vid_registry{
		lookup: make(map[btree.UUID]sop.Handle),
	}
}

func (v *mock_vid_registry) Add(ctx context.Context, storesHandles ...VirtualIdPayload[sop.Handle]) error {
	for _, storeHandles := range storesHandles {
		for _, h := range storeHandles.IDs {
			v.lookup[h.LogicalId] = h
		}
	}
	return nil
}

func (v *mock_vid_registry) Update(ctx context.Context, storesHandles ...VirtualIdPayload[sop.Handle]) error {
	for _, storeHandles := range storesHandles {
		for _, h := range storeHandles.IDs {
			v.lookup[h.LogicalId] = h
		}
	}
	return nil
}
func (v *mock_vid_registry) Get(ctx context.Context, storesLids ...VirtualIdPayload[btree.UUID]) ([]VirtualIdPayload[sop.Handle], error) {
	var storesHandles []VirtualIdPayload[sop.Handle]
	for _, storeLids := range storesLids {
		handles := make([]sop.Handle, 0, len(storeLids.IDs))
		for _, lid := range storeLids.IDs {
			h, _ := v.lookup[lid]
			handles = append(handles, h)
		}
		storesHandles = append(storesHandles, VirtualIdPayload[sop.Handle]{
			RegistryTable: storeLids.RegistryTable,
			IDs:           handles,
		})
	}
	return storesHandles, nil
}
func (v *mock_vid_registry) Remove(ctx context.Context, storesLids ...VirtualIdPayload[btree.UUID]) error {
	for _, storeLids := range storesLids {
		for _, lid := range storeLids.IDs {
			delete(v.lookup, lid)
		}
	}
	return nil
}
