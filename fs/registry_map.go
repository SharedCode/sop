package fs

import (
	"github.com/SharedCode/sop"
)

type registryMap struct {
	hashModValue       int
	replicationTracker *replicationTracker
	readWrite          bool
	// File handles of all known (traversed & opened) data segment file of the hash map.
	fileHandles map[string]*directIO
}

func newRegistryMap(hashModValue int, replicationTracker *replicationTracker, readWrite bool) *registryMap {
	return &registryMap{
		hashModValue:       hashModValue,
		replicationTracker: replicationTracker,
		readWrite:          readWrite,
		fileHandles:        make(map[string]*directIO, 5),
	}
}

func getIDs(items ...sop.Handle) []sop.UUID {
	IDs := make([]sop.UUID, len(items))
	for i := range items {
		IDs[i] = items[i].LogicalID
	}
	return IDs
}

func (hm *registryMap) set(allOrNothing bool, areItemsLocked func() error, items ...sop.Tuple[string, []sop.Handle]) error {
	if allOrNothing {
		unlockItemFileRegions := func(items ...sop.Tuple[string, []sop.Handle]) error {
			var lastErr error
			for _, item := range items {
				if err := hm.unlockFileRegion(item.First, getIDs(item.Second...)...); err != nil {
					lastErr = err
				}
			}
			return lastErr
		}
		lockedItems := make([]sop.Tuple[string, []sop.Handle], 0, len(items))
		for _, item := range items {
			if err := hm.lockFileRegion(true, item.First, getIDs(item.Second...)...); err != nil {
				unlockItemFileRegions(lockedItems...)
				return err
			}
			lockedItems = append(lockedItems, item)
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
		for _, item := range items {
			if err := hm.updateFileRegion(item.First, item.Second...); err != nil {
				unlockItemFileRegions(lockedItems...)
				return err
			}
		}
		return unlockItemFileRegions(lockedItems...)
	}
	// Individually manage/update the file area occupied by the handle so we don't create "lock pressure".
	for _, item := range items {
		for _, h := range item.Second {
			itemID := getIDs(h)
			if err := hm.lockFileRegion(true, item.First, itemID...); err != nil {
				return err
			}
			if err := hm.updateFileRegion(item.First, h); err != nil {
				hm.unlockFileRegion(item.First, itemID...)
				return err
			}
			if err := hm.unlockFileRegion(item.First, itemID...); err != nil {
				return err
			}
		}
	}
	return nil
}

func (hm *registryMap) get(keys ...sop.Tuple[string, []sop.UUID]) ([]sop.Tuple[string, []sop.Handle], error) {	
	// Individually manage/update the file area occupied by the handle so we don't create "lock pressure".
	result := make([]sop.Tuple[string, []sop.Handle], len(keys), 0)
	for _, k := range keys {
		for _, h := range k.Second {
			if err := hm.lockFileRegion(false, k.First, h); err != nil {
				return nil, err
			}
			d, err := hm.readFileRegion(k.First, h)
			if err != nil {
				return nil, err
			}
			result = append(result, sop.Tuple[string, []sop.Handle]{
				First: k.First,
				Second: d,
			})
			if err := hm.unlockFileRegion(k.First, h); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func (hm *registryMap) remove(keys ...sop.Tuple[string, []sop.UUID]) error {
	return nil
}

// TODO:
func (hm *registryMap) lockFileRegion(forWriting bool, filename string, id ...sop.UUID) error {
	var dio *directIO
	if f, ok := hm.fileHandles[filename]; ok {
		dio = f
	} else {
		dio = newDirectIO()
		//fn := hm.replicationTracker.getActiveFolderFilename(filename)
		//dio.open(fn, )
		hm.fileHandles[filename] = dio
	}

	//dio.lockFileRegion(hm.readWrite, )

	return nil
}
func (hm *registryMap) unlockFileRegion(filename string, id ...sop.UUID) error {
	return nil
}
func (hm *registryMap) updateFileRegion(filename string, h ...sop.Handle) error {
	return nil
}
func (hm *registryMap) readFileRegion(filename string, id ...sop.UUID) ([]sop.Handle, error) {
	return nil, nil
}

// Close all files opened by this hashmap on disk.
func (hm *registryMap) close() error {
	var lastError error
	for _, f := range hm.fileHandles {
		if err := f.close(); err != nil {
			lastError = err
		}
	}
	// Clear the file handles for cleanup.
	hm.fileHandles = make(map[string]*directIO)
	return lastError
}
