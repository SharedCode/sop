package fs

import (
	"context"

	"github.com/SharedCode/sop"
)

type registryMap struct {
	hashmap *hashmap
}

func newRegistryMap(readWrite bool, hashModValue HashModValueType, replicationTracker *replicationTracker, cache sop.Cache, useCacheForFileRegionLocks bool) *registryMap {
	return &registryMap{
		hashmap: newHashmap(readWrite, hashModValue, replicationTracker, cache, useCacheForFileRegionLocks),
	}
}

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
	// Support Update & Add of new Handle record(s).
	for _, item := range items {
		for _, h := range item.Second {
			frd, err := rm.hashmap.findAndLockFileRegion(ctx, item.First, h.LogicalID)
			if err != nil {
				return err
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

func (rm registryMap) get(ctx context.Context, keys ...sop.Tuple[string, []sop.UUID]) ([]sop.Tuple[string, []sop.Handle], error) {
	result := make([]sop.Tuple[string, []sop.Handle], 0, len(keys))
	for _, k := range keys {
		handles, err := rm.hashmap.get(ctx, k.First, k.Second...)
		if err != nil {
			return nil, err
		}
		result = append(result, sop.Tuple[string, []sop.Handle]{
			First:  k.First,
			Second: handles,
		})
	}
	return result, nil
}

func (rm registryMap) remove(ctx context.Context, keys ...sop.Tuple[string, []sop.UUID]) error {
	// Individually delete the file area occupied by the handle so we don't create "lock pressure".
	for _, key := range keys {
		for _, id := range key.Second {
			frd, err := rm.hashmap.findAndLockFileRegion(ctx, key.First, id)
			if err != nil {
				return err
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
