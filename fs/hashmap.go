package fs

import (
	"github.com/SharedCode/sop"
)

type hashmap struct {
	hashModValue       int
	replicationTracker *replicationTracker
	readWrite          bool
	// File handles of all known (traversed & opened) data segment file of the hash map.
	fileHandles map[string]*directIO
}

const(
	fullPermission = 0666
)

func newHashmap(hashModValue int, replicationTracker *replicationTracker, readWrite bool) *hashmap {
	return &hashmap{
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

func (hm *hashmap) set(allOrNothing bool, items ...sop.Tuple[string, []sop.Handle]) error {
	if allOrNothing {
		for _, item := range items {
			if err := hm.lockFileRegion(true, item.First, getIDs(item.Second...)...); err != nil {
				return err
			}
		}
		for _, item := range items {
			if err := hm.updateFileRegion(item.First, item.Second...); err != nil {
				return err
			}
		}
		for _, item := range items {
			if err := hm.unlockFileRegion(item.First, getIDs(item.Second...)...); err != nil {
				return err
			}
		}
		return nil
	}
	// Individually manage/update the file area occupied by the handle so we don't create "lock pressure".
	for _, item := range items {
		for _, h := range item.Second {
			if err := hm.lockFileRegion(true, item.First, getIDs(h)...); err != nil {
				return err
			}
			if err := hm.updateFileRegion(item.First, h); err != nil {
				return err
			}
			if err := hm.unlockFileRegion(item.First, getIDs(h)...); err != nil {
				return err
			}
		}
	}
	return nil
}

func (hm *hashmap) get(keys ...sop.Tuple[string, []sop.UUID]) ([]sop.Tuple[string, []sop.Handle], error) {	
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

func (hm *hashmap) remove(keys ...sop.Tuple[string, []sop.UUID]) error {
	return nil
}

// TODO:
func (hm *hashmap) lockFileRegion(forWriting bool, filename string, id ...sop.UUID) error {
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
func (hm *hashmap) unlockFileRegion(filename string, id ...sop.UUID) error {
	return nil
}
func (hm *hashmap) updateFileRegion(filename string, h ...sop.Handle) error {
	return nil
}
func (hm *hashmap) readFileRegion(filename string, id ...sop.UUID) ([]sop.Handle, error) {
	return nil, nil
}

// Close all files opened by this hashmap on disk.
func (hm *hashmap) close() error {
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
