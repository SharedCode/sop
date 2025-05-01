package fs

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
)

/*
	Hashmap on disk manages registry on file. Each Handle record is persisted to a block and the address of this block is determined using
	"modulo hash" operator. Each block can store up to 66 Handle "records". A registry can span across one or more segment files.
*/

type hashmap struct {
	hashModValue       HashModValueType
	replicationTracker *replicationTracker
	readWrite          bool
	// File handles of all known (traversed & opened) data segment file of the hash map.
	fileHandles                map[string]*directIO
	cache                      sop.Cache
	useCacheForFileRegionLocks bool
}

// File reqion details is a response struct of 'findAndLock' function. It puts together
// details about discovered (file &) location, i.e. - file offset, of a given UUID & its current record's
// value (unmarshalled to a Handle) read from the file.
type fileRegionDetails struct {
	dio    *directIO
	offset int64
	handle sop.Handle
}

const (
	fullPermission        = 0644
	handlesPerBlock       = 66
	lockFileRegionTimeout = time.Duration(5 * time.Minute)
	// Growing the file needs more time to complete.
	lockPreallocateFileTimeout = time.Duration(25 * time.Minute)
	registryFileIOLockKey      = "infs_reg"
)

type HashModValueType int

const (
	MinimumModValue   = 100000  // 100k, should generate 400MB file segment
	SuperTinyModValue = 125000  // 125k, should generate 500MB file segment
	TinyModValue      = 200000  // 200k, should generate 800MB file segment
	SmallModValue     = 250000  // 250k, should generate 1GB file segment
	MediumModValue    = 350000  // 350k, should generate 1.4GB file segment
	LargeModValue     = 500000  // 500k, 2GB file segment
	XLModValue        = 750000  // 750k, 3GB file segment
	XXLModValue       = 1000000 // 1m, 4GB file segment
)

// Hashmap constructor, hashModValue can't be negative nor beyond 10mil otherwise it will be reset to 250k.
func newHashmap(readWrite bool, hashModValue HashModValueType, replicationTracker *replicationTracker, cache sop.Cache, useCacheForFileRegionLocks bool) *hashmap {
	return &hashmap{
		hashModValue:               hashModValue,
		replicationTracker:         replicationTracker,
		readWrite:                  readWrite,
		fileHandles:                make(map[string]*directIO, 5),
		cache:                      cache,
		useCacheForFileRegionLocks: useCacheForFileRegionLocks,
	}
}

// Maximum file size is segment file size (hash mod value X 4096) + (.5 of segment file size).
// Upon reaching this max file size, hashmap should create a new file on succeeding block insert.
func (hm *hashmap) getMaxFileSize() int64 {
	smfs := hm.getSegmentFileSize()
	return smfs + (smfs / 2)
}

func (hm *hashmap) getSegmentFileSize() int64 {
	return int64(hm.hashModValue) * blockSize
}

func (hm *hashmap) getBlockOffsetAndHandleInBlockOffset(id sop.UUID) (int64, int64) {
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

	return blockOffset * blockSize, offsetInBlock * sop.HandleSizeInBytes
}

// Iterate through the entire set of hashmap (bucket) files, from 1 to the last bucket file.
// Each bucket file is allocated about 250,000 "sector blocks"(configurable) and in total, contains ~16,650,000
// addressable virtual IDs (handles). Typically, there should be only one bucket file as this file
// with the default numbers shown, can be used to hold 825 million items of the B-Tree, given a
// slot length of 500.
func (hm *hashmap) findAndLock(ctx context.Context, forWriting bool, filename string, id sop.UUID) (fileRegionDetails, error) {
	var dio *directIO
	var result fileRegionDetails
	var createdNewFile bool

	i := 1
	for {
		fn := hm.replicationTracker.formatActiveFolderFilename(fmt.Sprintf("%s-%d.reg", filename, i))
		if f, ok := hm.fileHandles[fn]; ok {
			dio = f
		} else {
			dio = newDirectIO(hm.cache, hm.useCacheForFileRegionLocks)
			flag := os.O_CREATE | os.O_RDWR
			if !hm.readWrite {
				flag = os.O_RDONLY
			}
			fileExists := dio.fileExists(fn)
			if !fileExists {
				if !forWriting {
					return result, fmt.Errorf("can't read a registry file(%s) that is missing", fn)
				}

				lk := hm.cache.CreateLockKeys(registryFileIOLockKey)
				if err := hm.cache.Lock(ctx, lockPreallocateFileTimeout, lk...); err != nil {
					return result, err
				}

				if err := dio.open(fn, flag, fullPermission); err != nil {
					hm.cache.Unlock(ctx, lk...)
					return result, err
				}

				// Handle properly a newly created file.
				// Pre-allocate entire segment if new file. Should we Redis lock to allow only one process to win Truncate?
				// NFS should be able to allow one and others to fail, error out.
				if err := dio.file.Truncate(hm.getSegmentFileSize()); err != nil {
					hm.cache.Unlock(ctx, lk...)
					return result, err
				}
				hm.cache.Unlock(ctx, lk...)

				hm.fileHandles[fn] = dio
				// New file, 'prepare to let caller write the new handle to this block's first slot.
				blockOffset, handleInBlockOffset := hm.getBlockOffsetAndHandleInBlockOffset(id)
				result.offset = blockOffset + handleInBlockOffset
				if ok, err := dio.isRegionLocked(ctx, forWriting, result.offset, sop.HandleSizeInBytes); ok || err != nil {
					if ok {
						err = fmt.Errorf("can't lock (forWriting=%v) file region w/ offset %v as it is locked", forWriting, result.offset)
					}
					return result, err
				}
				if err := dio.lockFileRegion(ctx, forWriting, result.offset, sop.HandleSizeInBytes, lockFileRegionTimeout); err != nil {
					return result, err
				}
				result.dio = dio
				return result, nil
			}
			hm.fileHandles[fn] = dio
		}

		// Read entire block for the ID hash mod, deserialize each Handle and check if anyone matches the one we are trying to find.
		// For add use-case with "collision", when there is no more slot on the block, we need to automatically create a new segment file.
		ba := dio.createAlignedBlock()
		blockOffset, handleInBlockOffset := hm.getBlockOffsetAndHandleInBlockOffset(id)

		n, err := dio.readAt(ba, blockOffset)
		if err != nil {
			return result, err
		}
		if n != len(ba) {
			return result, fmt.Errorf("only able to read partially (%d bytes) the block record at offset %v", n, blockOffset)
		}

		// Unmarshal and check if this is the Handler record we are looking for.
		m := encoding.NewHandleMarshaler()
		bao := 0
		for i := 0; i < handlesPerBlock; i++ {
			var h sop.Handle
			hbuf := ba[bao : bao+sop.HandleSizeInBytes]

			// Handle properly a zero block.
			if isZeroData(hbuf) {
				if !forWriting {
					return result, fmt.Errorf("can't lock for reading, handle record at %v is zero block", blockOffset+int64(bao))
				}

				// Try to lock (for writing) the block.
				if ok, err := dio.isRegionLocked(ctx, forWriting, blockOffset+int64(bao), sop.HandleSizeInBytes); ok || err != nil {
					if ok {
						err = fmt.Errorf("can't lock (forWriting=%v) file region w/ offset %v as it is locked", forWriting, blockOffset+int64(bao))
					}
					return result, err
				}
				if err := dio.lockFileRegion(forWriting, blockOffset+int64(bao), sop.HandleSizeInBytes, lockFileRegionTimeout); err != nil {
					return result, err
				}
				result.dio = dio
				result.offset = blockOffset + +int64(bao)
				return result, nil

			}
			if err := m.Unmarshal(hbuf, &h); err != nil {
				return result, err
			}

			bao += sop.HandleSizeInBytes
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
			if ok, err := dio.isRegionLocked(ctx, forWriting, offset, sop.HandleSizeInBytes); ok || err != nil {
				if ok {
					err = fmt.Errorf("can't lock (forWriting=%v) file region w/ offset %v as it is locked", forWriting, offset)
				}
				return result, err
			}
			if err := dio.lockFileRegion(ctx, forWriting, offset, sop.HandleSizeInBytes, lockFileRegionTimeout); err != nil {
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
func (hm *hashmap) lockFileRegion(ctx context.Context, forWriting bool, filename string, ids ...sop.UUID) ([]fileRegionDetails, error) {
	undo := func(items []fileRegionDetails) {
		for _, item := range items {
			item.dio.unlockFileRegion(ctx, item.offset, sop.HandleSizeInBytes)
		}
	}
	completedItems := make([]fileRegionDetails, 0, len(ids))
	for _, id := range ids {
		frd, err := hm.findAndLock(ctx, hm.readWrite, filename, id)
		if err != nil {
			undo(completedItems)
			return nil, err
		}
		completedItems = append(completedItems, frd)
	}
	return completedItems, nil
}

// Unlock file region(s).
func (hm *hashmap) unlockFileRegion(ctx context.Context, fileRegionDetails ...fileRegionDetails) error {

	return nil
}

func (hm *hashmap) updateFileRegion(ctx context.Context, fileRegionDetails ...fileRegionDetails) error {
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
