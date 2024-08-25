package fs

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop"
)

type Mock_vid_registry struct {
	Lookup                           map[sop.UUID]sop.Handle
	InducedErrorOnUpdateAllOrNothing bool
}

// NewMockRegistry manages the Handle in memory for mocking.
func NewMockRegistry(inducedErrorOnUpdateAllOrNothing bool) sop.Registry {
	return &Mock_vid_registry{make(map[sop.UUID]sop.Handle), inducedErrorOnUpdateAllOrNothing}
}

func (v *Mock_vid_registry) Add(ctx context.Context, storesHandles ...sop.RegistryPayload[sop.Handle]) error {
	for _, storeHandles := range storesHandles {
		for _, h := range storeHandles.IDs {
			v.Lookup[h.LogicalID] = h
		}
	}
	return nil
}

func (v *Mock_vid_registry) Update(ctx context.Context, allOrNothing bool, storesHandles ...sop.RegistryPayload[sop.Handle]) error {
	if v.InducedErrorOnUpdateAllOrNothing && allOrNothing {
		return fmt.Errorf("Induced error on Update w/ allOrNothing true.")
	}
	for _, storeHandles := range storesHandles {
		for _, h := range storeHandles.IDs {
			v.Lookup[h.LogicalID] = h
		}
	}
	return nil
}
func (v *Mock_vid_registry) Get(ctx context.Context, storesLids ...sop.RegistryPayload[sop.UUID]) ([]sop.RegistryPayload[sop.Handle], error) {
	var storesHandles []sop.RegistryPayload[sop.Handle]
	for _, storeLids := range storesLids {
		handles := make([]sop.Handle, 0, len(storeLids.IDs))
		for _, lid := range storeLids.IDs {
			h, _ := v.Lookup[lid]
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
func (v *Mock_vid_registry) Remove(ctx context.Context, storesLids ...sop.RegistryPayload[sop.UUID]) error {
	for _, storeLids := range storesLids {
		for _, lid := range storeLids.IDs {
			delete(v.Lookup, lid)
		}
	}
	return nil
}
