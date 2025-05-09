package fs

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop"
)

type registryMap struct {
	hashmap *hashmap
}

func newRegistryMap(readWrite bool, hashModValue int, replicationTracker *replicationTracker, cache sop.Cache, useCacheForFileRegionLocks bool) *registryMap {
	return &registryMap{
		hashmap: newHashmap(readWrite, hashModValue, replicationTracker, cache, useCacheForFileRegionLocks),
	}
}

// Add a given set of Handle(s) record(s) on file(s) where they are supposed to get stored in.
func (rm registryMap) add(ctx context.Context, items ...sop.Tuple[string, []sop.Handle]) error {
	// Individually write to the file area occupied by the handle so we don't create "lock pressure".
	for _, item := range items {
		for _, h := range item.Second {
			frd, err := rm.hashmap.findAndLockFileRegion(ctx, item.First, h.LogicalID)
			if err != nil {
				return err
			}

			// Fail if item exists in target.
			if !frd[0].handle.IsEmpty() {
				rm.hashmap.unlockFileRegion(ctx, frd...)
				return fmt.Errorf("registryMap.add failed, can't overwrite an item at offset=%v, item details: %v", frd[0].getOffset(), frd[0].handle)
			}

			frd[0].handle = h
			if err := rm.hashmap.updateFileRegion(ctx, frd...); err != nil {
				rm.hashmap.unlockFileRegion(ctx, frd...)
				return err
			}
			if err := rm.hashmap.unlockFileRegion(ctx, frd...); err != nil {
				return err
			}
		}
	}
	return nil
}

// Update a given set of Handle(s) record(s) on file(s) where they are stored in.
func (rm registryMap) set(ctx context.Context, allOrNothing bool, items ...sop.Tuple[string, []sop.Handle]) error {
	if allOrNothing {
		// Supports update (including update to prepare for deleting) of Handle records.
		unlockItemFileRegions := func(items ...fileRegionDetails) error {
			if err := rm.hashmap.unlockFileRegion(ctx, items...); err != nil {
				return err
			}
			return nil
		}
		lockedItems := make([]fileRegionDetails, 0, len(items))
		for _, item := range items {
			frds, err := rm.hashmap.findAndLockFileRegion(ctx, item.First, getIDs(item.Second...)...)
			if err != nil {
				unlockItemFileRegions(lockedItems...)
				return err
			}
			// Update the Handles read w/ the items' values.
			for i := 0; i < len(frds); i++ {
				// Check if the record in the target file region is different.
				if !frds[i].handle.IsEmpty() && frds[i].handle.LogicalID != item.Second[i].LogicalID {
					// Fail if the record on target is different.
					lockedItems = append(lockedItems, frds...)
					unlockItemFileRegions(lockedItems...)
					return fmt.Errorf("registryMap.set allOrNothing failed, an item(target lid=%v) at offset=%v is different (source lid=%v)",
						frds[i].handle.LogicalID, frds[i].getOffset(), item.Second[i].LogicalID)
				}

				frds[i].handle = item.Second[i]
			}
			lockedItems = append(lockedItems, frds...)
		}
		if err := rm.hashmap.updateFileRegion(ctx, lockedItems...); err != nil {
			unlockItemFileRegions(lockedItems...)
			return err
		}
		return unlockItemFileRegions(lockedItems...)
	}
	// Individually manage/update the file area occupied by the handle so we don't create "lock pressure".
	for _, item := range items {
		for _, h := range item.Second {
			frd, err := rm.hashmap.findAndLockFileRegion(ctx, item.First, h.LogicalID)
			if err != nil {
				return err
			}
			// Check if the record in the target file region is different.
			if !frd[0].handle.IsEmpty() && frd[0].handle.LogicalID != h.LogicalID {
				rm.hashmap.unlockFileRegion(ctx, frd...)
				return fmt.Errorf("registryMap.set failed, an item(target lid=%v) at offset=%v is different (source lid=%v)",
					frd[0].handle.LogicalID, frd[0].getOffset(), h.LogicalID)
			}

			frd[0].handle = h
			if err := rm.hashmap.updateFileRegion(ctx, frd...); err != nil {
				rm.hashmap.unlockFileRegion(ctx, frd...)
				return err
			}
			if err := rm.hashmap.unlockFileRegion(ctx, frd...); err != nil {
				return err
			}
		}
	}
	return nil
}

// Fetch the Handle record(s) from a given set of file(s) & their UUID(s).
func (rm registryMap) get(ctx context.Context, keys ...sop.Tuple[string, []sop.UUID]) ([]sop.Tuple[string, []sop.Handle], error) {
	result := make([]sop.Tuple[string, []sop.Handle], 0, len(keys))
	for _, k := range keys {
		handles, err := rm.hashmap.get(ctx, k.First, k.Second...)
		if err != nil {
			return nil, fmt.Errorf("registryMap.get failed, details: %v", err)
		}
		result = append(result, sop.Tuple[string, []sop.Handle]{
			First:  k.First,
			Second: handles,
		})
	}
	return result, nil
}

// Mark the Handle record(s) on file to be deleted & reuse ready.
func (rm registryMap) remove(ctx context.Context, keys ...sop.Tuple[string, []sop.UUID]) error {
	// Individually delete the file area occupied by the handle so we don't create "lock pressure".
	for _, key := range keys {
		for _, id := range key.Second {
			frd, err := rm.hashmap.findAndLockFileRegion(ctx, key.First, id)
			if err != nil {
				return err
			}
			// If read handle is empty, it means the item is already marked deleted in disk.
			if frd[0].handle.IsEmpty() {
				// Fail if there is no record on target, can't delete a missing item.
				rm.hashmap.unlockFileRegion(ctx, frd...)
				return fmt.Errorf("registryMap.remove failed, an item at offset=%v was not found, can't delete a missing item", frd[0].getOffset())
			}
			// Check if the record in the target file region is different.
			if frd[0].handle.LogicalID != id {
				// Fail if the found record on target is different.
				rm.hashmap.unlockFileRegion(ctx, frd...)
				return fmt.Errorf("registryMap.remove failed, an item(target lid=%v) at offset=%v is different (source lid=%v)",
					frd[0].handle.LogicalID, frd[0].getOffset(), id)
			}

			if err := rm.hashmap.markDeleteFileRegion(ctx, frd...); err != nil {
				rm.hashmap.unlockFileRegion(ctx, frd...)
				return err
			}
			if err := rm.hashmap.unlockFileRegion(ctx, frd...); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close all files opened by this hashmap on disk.
func (rm registryMap) close() error {
	return rm.hashmap.close()
}

func getIDs(items ...sop.Handle) []sop.UUID {
	IDs := make([]sop.UUID, len(items))
	for i := range items {
		IDs[i] = items[i].LogicalID
	}
	return IDs
}
