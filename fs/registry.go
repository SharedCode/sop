package fs

import (
	"context"

	"github.com/SharedCode/sop"
)

type registryOnDisk struct {
	Lookup                           map[sop.UUID]sop.Handle
	replicatorTracker                *replicationTracker
}

// TODO: Implement this to do "hash map on disk" (file based) registry entries storage & management.

// NewRegistry manages the Handle in memory for mocking.
func NewRegistry(rt *replicationTracker) sop.Registry {
	return &registryOnDisk{
		Lookup: make(map[sop.UUID]sop.Handle),
		replicatorTracker: rt,
	}
}

func (v *registryOnDisk) Add(ctx context.Context, storesHandles ...sop.RegistryPayload[sop.Handle]) error {
	for _, storeHandles := range storesHandles {
		for _, h := range storeHandles.IDs {
			v.Lookup[h.LogicalID] = h
		}
	}
	return nil
}

func (v *registryOnDisk) Update(ctx context.Context, allOrNothing bool, storesHandles ...sop.RegistryPayload[sop.Handle]) error {
	for _, storeHandles := range storesHandles {
		for _, h := range storeHandles.IDs {
			v.Lookup[h.LogicalID] = h
		}
	}
	return nil
}
func (v *registryOnDisk) Get(ctx context.Context, storesLids ...sop.RegistryPayload[sop.UUID]) ([]sop.RegistryPayload[sop.Handle], error) {
	var storesHandles []sop.RegistryPayload[sop.Handle]
	for _, storeLids := range storesLids {
		handles := make([]sop.Handle, 0, len(storeLids.IDs))
		for _, lid := range storeLids.IDs {
			h := v.Lookup[lid]
			if h.LogicalID.IsNil() {
				continue
			}
			handles = append(handles, h)
		}
		storesHandles = append(storesHandles, sop.RegistryPayload[sop.Handle]{
			RegistryTable: storeLids.RegistryTable,
			IDs:           handles,
		})
	}
	return storesHandles, nil
}
func (v *registryOnDisk) Remove(ctx context.Context, storesLids ...sop.RegistryPayload[sop.UUID]) error {
	for _, storeLids := range storesLids {
		for _, lid := range storeLids.IDs {
			delete(v.Lookup, lid)
		}
	}
	return nil
}
