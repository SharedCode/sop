package fs

import (
	"fmt"
	"os"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
)

type hashmap struct {
	hashModValue       int
	replicationTracker *replicationTracker
	readWrite          bool
	// File handles of all known (traversed & opened) data segment file of the hash map.
	fileHandles map[string]directIO
}

type fileRegionDetails struct {
	dio directIO
	offset int64
	handle sop.Handle
}

const(
	fullPermission = 0666
	handlesPerBlock = 66
	lockFileRegionTimeout = time.Duration(5 * time.Minute)
)

func newHashmap(hashModValue int, replicationTracker *replicationTracker, readWrite bool) *hashmap {
	return &hashmap{
		hashModValue:       hashModValue,
		replicationTracker: replicationTracker,
		readWrite:          readWrite,
		fileHandles:        make(map[string]directIO, 5),
	}
}

// Iterate through the entire set of hashmap (bucket) files, from 1 to the last bucket file.
// Each bucket file is allocated about 250,000 "sector blocks" and in total, contains ~16,650,000
// addressable virtual ID (handle). Typically, there should be only one bucket file as this file
// with the default numbers shown, can be used to hold 825 million items of the B-Tree, given a
// slot length of 500.
func (hm *hashmap) findAndLock(forWriting bool, filename string, id sop.UUID) (fileRegionDetails, error) {
	var dio directIO

	var result fileRegionDetails
	i := 1
	for {
		fn := hm.replicationTracker.formatActiveFolderFilename(fmt.Sprintf("%s-%d.reg",filename,i))
		if f, ok := hm.fileHandles[fn]; ok {
			dio = f
		} else {
			dio = *newDirectIO()
			flag := os.O_CREATE|os.O_RDWR
			if !hm.readWrite {
				flag = os.O_RDONLY
			}
			if err := dio.open(fn, flag, fullPermission); err != nil {
				return result, err
			}
			hm.fileHandles[fn] = dio
		}

		// Split UUID into high & low int64 parts.
		bytes := id[:]

		var high int64
		for i := 0; i < 8; i++ {
			high = high<<8 | int64(bytes[i])
		}
		var low int64
		for i := 8; i < 16; i++ {
			low = low<<8 | int64(bytes[i])
		}

		blockOffset := high % int64(hm.hashModValue)
		offsetInBlock := low % handlesPerBlock

		offset := (blockOffset * blockSize) + (offsetInBlock*sop.HandleSizeInBytes)
		ba := make([]byte, sop.HandleSizeInBytes)
		n, err := dio.readAt(ba, offset)
		if err != nil {
			//if err == io.EOF 
			return result, err
		}
		if n != len(ba) {
			return result, nil
		}

		// for i := 0; i < len(ba); i++ {
		// 	if buffer[i] != 0 {
		// 		return false, nil // Found a non-zero byte
		// 	}
		// }

		m := encoding.NewHandleMarshaler()
		var h sop.Handle
		if err := m.Unmarshal(ba, &h); err != nil {
			return result, err
		}
		if h.LogicalID == id {
			result.offset = offset
			result.handle = h
			if ok, err := dio.isRegionLocked(forWriting, offset, sop.HandleSizeInBytes); ok || err != nil {
				if ok {
					err = fmt.Errorf("can't lock (forWriting=%v) file region w/ offset %d as it is locked", forWriting, offset)
				}
				return result, err
			}
			if err := dio.lockFileRegion(forWriting, offset, sop.HandleSizeInBytes, lockFileRegionTimeout); err != nil {
				return result, err
			}
			result.dio = dio
			return result, nil
		}

		i++
		if i > 1000 {
			break
		}
	}
	result.dio = dio
	return result, nil
}

// Lock file region(s) that a set of UUIDs correlate to and return these region(s)' offsett/Handle if in case
// useful to the caller.
func (hm *hashmap) lockFileRegion(forWriting bool, filename string, ids ...sop.UUID) ([]fileRegionDetails,error) {
	undo := func(items []fileRegionDetails) {
		for _, item := range items {
			item.dio.unlockFileRegion(item.offset, sop.HandleSizeInBytes)
		}
	}
	completedItems := make([]fileRegionDetails, 0, len(ids))
	for _, id := range ids {
		frd, err := hm.findAndLock(hm.readWrite, filename, id)
		if err != nil {
			undo(completedItems)
			return nil, err
		}
		completedItems = append(completedItems, frd)
	}
	return completedItems, nil
}

// Unlock file region(s).
func (hm *hashmap) unlockFileRegion(fileRegionDetails ...fileRegionDetails) error {
	return nil
}

func (hm *hashmap) updateFileRegion(fileRegionDetails ...fileRegionDetails) error {
	return nil
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
	hm.fileHandles = make(map[string]directIO)
	return lastError
}
