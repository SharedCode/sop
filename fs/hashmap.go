package fs

import (
	"time"

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

func (hm *hashmap) findBlockLocation(forWriting bool, filename string, id sop.UUID) (*directIO, int64) {
	var dio *directIO

	// fn := hm.replicationTracker.formatActiveFolderFilename(filename)

	// if f, ok := hm.fileHandles[fn]; ok {
	// 	dio = f
	// } else {
	// 	dio = newDirectIO()
	// 	dio.open(fn, , fullPermission)
	// 	hm.fileHandles[fn] = dio
	// }
	return dio, 0
}

func (hm *hashmap) lockFileRegion(forWriting bool, filename string, ids ...sop.UUID) error {
	undo := func(items []sop.Tuple[*directIO, int64]) {
		for _, item := range items {
			item.First.unlockFileRegion(item.Second, sop.HandleSizeInBytes)
		}
	}
	completedItems := make([]sop.Tuple[*directIO, int64], 0, len(ids))
	for _, id := range ids {
		dio, offset := hm.findBlockLocation(hm.readWrite, filename, id)
		if ok, err := dio.isRegionLocked(hm.readWrite, offset, sop.HandleSizeInBytes); ok || err != nil {
			undo(completedItems)
			return err
		}
		if err := dio.lockFileRegion(hm.readWrite, offset, sop.HandleSizeInBytes, time.Duration(5*time.Minute)); err != nil {
			undo(completedItems)
			return err
		}
		completedItems = append(completedItems, sop.Tuple[*directIO, int64]{First:dio, Second:offset})
	}
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
