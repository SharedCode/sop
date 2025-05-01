package fs

import (
	"fmt"
	"os"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
)

/*
	Hashmap on disk manages registry on file. Each Handle record is persisted to a block and the address of this block is determined using
	"modulo hash". Each block can store up to 66 Handles with the last 4 bytes optionally containing a file offset to the "extended blocks"
	location beyond the file segment. Extended blocks' location is an area of the file beyond the regular border of the file segment. This
	area is allocated half of the file segment.

	To give illustration, for example, if the computed file segment is 1GB, the total will be 1.5GB including the extended location. This gives
	some hash mod number to have a "linked list" of allocated blocks. The extended area was designed to accommodate "module hash collision" edge cases.
	This should allow "denser" file segments & reduce empty or unused space.
*/

type hashmap struct {
	hashModValue       HashModValueType
	replicationTracker *replicationTracker
	readWrite          bool
	// File handles of all known (traversed & opened) data segment file of the hash map.
	fileHandles map[string]directIO
}

// File reqion details is a response struct of 'findAndLock' function. It puts together
// details about discovered (file &) location, i.e. - file offset, of a given UUID & its current record's
// value (unmarshalled to a Handle) read from the file.
type fileRegionDetails struct {
	dio    directIO
	offset int64
	handle sop.Handle
}

const (
	fullPermission        = 0644
	handlesPerBlock       = 66
	lockFileRegionTimeout = time.Duration(5 * time.Minute)
)

type HashModValueType int

const (
	MinimumModValue   = 100000 // 100k, should generate 400MB file segment, 600MB total
	SuperTinyModValue = 125000 // 125k, should generate 500MB file segment, 750MB total
	TinyModValue      = 200000 // 200k, should generate 800MB file segment, 1.2GB total
	DefaultModValue   = TinyModValue
	SmallModValue     = 250000  // 250k, should generate 1GB file segment, 1.5GB total
	MediumModValue    = 300000  // 300k, should generate 1.2GB file segment, 1.8GB total
	LargeModValue     = 500000  // 500k, 2GB file segment, 3GB total
	XLModValue        = 750000  // 750k, 3GB file segment, 4.5GB total
	DoubleXLModValue  = 1000000 // 1m, 4GB file segment, 6GB total
)

// Hashmap constructor, hashModValue can't be negative nor beyond 10mil otherwise it will be reset to 250k.
func newHashmap(readWrite bool, hashModValue HashModValueType, replicationTracker *replicationTracker) *hashmap {
	return &hashmap{
		hashModValue:       hashModValue,
		replicationTracker: replicationTracker,
		readWrite:          readWrite,
		fileHandles:        make(map[string]directIO, 5),
	}
}

// Total file size is hash mod value X 4096 X 3. We pre-allocate the file to have three segments.
// That is up to three blocks to accommodate collisions. A block can contain 66 handles, that means
// 66 * 3 = 198 slots to accomodate collisions before triggering to add a new file.
func (hm *hashmap) getTotalFileSize() int64 {
	smfs := hm.getSegmentFileSize()
	return smfs + (smfs / 2)
}

func (hm *hashmap) getSegmentFileSize() int64 {
	return int64(hm.hashModValue) * blockSize
}

// Iterate through the entire set of hashmap (bucket) files, from 1 to the last bucket file.
// Each bucket file is allocated about 250,000 "sector blocks" and in total, contains ~16,650,000
// addressable virtual ID (handle). Typically, there should be only one bucket file as this file
// with the default numbers shown, can be used to hold 825 million items of the B-Tree, given a
// slot length of 500.
func (hm *hashmap) findAndLock(forWriting bool, filename string, id sop.UUID) (fileRegionDetails, error) {
	var dio directIO
	var result fileRegionDetails
	var createdNewFile bool

	i := 1
	for {
		fn := hm.replicationTracker.formatActiveFolderFilename(fmt.Sprintf("%s-%d.reg", filename, i))
		if f, ok := hm.fileHandles[fn]; ok {
			dio = f
		} else {
			dio = *newDirectIO()
			flag := os.O_CREATE | os.O_RDWR
			if !hm.readWrite {
				flag = os.O_RDONLY
			}
			fileEmpty, err := dio.isFileEmpty(fn)
			if err != nil {
				return result, err
			}
			if err := dio.open(fn, flag, fullPermission); err != nil {
				return result, err
			}
			// Pre-allocate if new file.
			if forWriting && fileEmpty {
				if err := dio.file.Truncate(hm.getTotalFileSize()); err != nil {
					return result, err
				}
				createdNewFile = true
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

		// Compute bucket file offset.
		offset := (blockOffset * blockSize) + (offsetInBlock * sop.HandleSizeInBytes)
		ba := dio.createAlignedBlock()

		n, err := dio.readAt(ba, offset)
		if err != nil {
			return result, err
		}
		if n != len(ba) {
			return result, fmt.Errorf("only able to read partially (%d bytes) the Handle record at offset %v", n, offset)
		}

		// Handle empty block.
		if isZeroData(ba) {
			result.offset = offset
			result.dio = dio
			return result, nil
		}

		// Unmarshal and check if this is the Handler record we are looking for.
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
		// Stop the loop of we've just created a new file segment or reaching ridiculous file check.
		if i > 1000 || createdNewFile {
			break
		}
	}
	result.dio = dio
	return result, nil
}

// Lock file region(s) that a set of UUIDs correlate to and return these region(s)' offsett/Handle if in case
// useful to the caller.
func (hm *hashmap) lockFileRegion(forWriting bool, filename string, ids ...sop.UUID) ([]fileRegionDetails, error) {
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

func isZeroData(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}
