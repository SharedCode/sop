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

func (rm registryMap) set(ctx context.Context, allOrNothing bool, areItemsLocked func() error, items ...sop.Tuple[string, []sop.Handle]) error {
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
			frds, err := rm.hashmap.lockFileRegion(ctx, true, item.First, getIDs(item.Second...)...)
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
		if areItemsLocked != nil {
			// Ensure the batch are all locked as seen in Redis, to address race condition.
			// This is the 4th letter R in the (SOP proprietary) Redis RSRR algorithm.
			//
			// NOTE: Redis exclusive lock check for this implementation is more rigid because there is no other
			// "all or nothing" guarantee except our algorithm check in Redis and the hashmap.updateFileRegion implementation
			// which relies on NFS' distributed file lock support. We want to be 200% sure no race condition. :)
			// As can be seen, the Redis "items locked" check is done after the "lockFileRegion" call, which means,
			// code had given plenty of time for race condition not to occur. If network is flaky or slow, it will
			// fail in the "lockFileRegion" call and if it passes, it is sure there is absolutely no race condition caused
			// item to get double locked by two or more different processes.
			// Relativity theory in action.
			if err := areItemsLocked(); err != nil {
				unlockItemFileRegions(lockedItems...)
				return err
			}
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
		frds, err := rm.hashmap.lockFileRegion(ctx, true, item.First, getIDs(item.Second...)...)
		if err != nil {
			return err
		}
		for i := range frds {
			frds[i].handle = item.Second[i]
		}
		if err := rm.hashmap.updateFileRegion(ctx, frds...); err != nil {
			rm.hashmap.unlockFileRegion(ctx, frds...)
			return err
		}
		if err := rm.hashmap.unlockFileRegion(ctx, frds...); err != nil {
			return err
		}
	}
	return nil
}

func (rm registryMap) get(ctx context.Context, keys ...sop.Tuple[string, []sop.UUID]) ([]sop.Tuple[string, []sop.Handle], error) {
	result := make([]sop.Tuple[string, []sop.Handle], len(keys), 0)
	for _, k := range keys {
		frds, err := rm.hashmap.lockFileRegion(ctx, false, k.First, k.Second...)
		if err != nil {
			return nil, err
		}

		if err := rm.hashmap.unlockFileRegion(ctx, frds...); err != nil {
			return nil, err
		}

		handles := make([]sop.Handle, 0, len(k.Second))
		for i := 0; i < len(frds); i++ {
			handles = append(handles, frds[i].handle)
		}

		result = append(result, sop.Tuple[string, []sop.Handle]{
			First:  k.First,
			Second: handles,
		})
	}
	return result, nil
}

func (rm registryMap) remove(ctx context.Context, keys ...sop.Tuple[string, []sop.UUID]) error {
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
