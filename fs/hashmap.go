package fs

import (
	"context"
	"fmt"
	log "log/slog"
	"os"
	"strings"
	"time"

	"github.com/ncw/directio"
	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/encoding"
)

/*
	Hashmap on disk manages registry on file. Each Handle record is persisted to a block and the address of this block is determined using
	"modulo hash" operator. Each block can store up to 66 Handle "records". A registry can span across one or more segment files.
*/

type hashmap struct {
	hashModValue       int
	replicationTracker *replicationTracker
	readWrite          bool
	// File handles of all known (traversed & opened) data segment file of the hash map.
	fileHandles map[string]*fileDirectIO
	cache       sop.Cache
}

// File reqion details is a response struct of 'findAndLock' function. It puts together
// details about discovered (file &) location, i.e. - file offset, of a given UUID & its current record's
// value (unmarshalled to a Handle) read from the file.
type fileRegionDetails struct {
	dio                 *fileDirectIO
	blockOffset         int64
	handleInBlockOffset int64
	handle              sop.Handle
}

func (fr *fileRegionDetails) getOffset() int64 {
	return fr.blockOffset + fr.handleInBlockOffset
}

const (
	handlesPerBlock        = 66
	preallocateFileLockKey = "infs_reg"
	// Growing the file needs more time to complete.
	lockPreallocateFileTimeout = time.Duration(20 * time.Minute)
	lockFileRegionKeyPrefix    = "infs"
	idNotFoundErr              = "unable to find the item with id"

	registryFileExtension = ".reg"

	// 250, should generate 1MB file segment. Formula: 250 X 4096 = 1MB
	// Given a 50 slot size per node, should be able to manage 825,000 B-Tree items (key/value pairs).
	//
	// Formula: 250 * 66 * 50 = 825,000
	// Or if you use 100 slot size per node, 'will give you 1,650,000 items, or assuming you have about 65%
	// b-tree utilization, 1,072,500 usable space.
	MinimumModValue = 250
	// 750k, should generate 3GB file segment.  Formula: 750k X 4096 = 3GB
	MaximumModValue = 750000
)

// Configurable lock TTL and slack for file-region operations.
// Defaults retain previous behavior: 5m TTL and ~2% slack (min 2s).
var (
	LockFileRegionDuration   = time.Duration(5 * time.Minute)
	LockDeadlineSlackPercent = 0.02
)

// SetLockFileRegionDuration updates the TTL for per-block file region locks.
// Non-positive values are ignored.
func SetLockFileRegionDuration(d time.Duration) {
	if d > 0 {
		LockFileRegionDuration = d
	}
}

// SetLockDeadlineSlackPercent sets the fraction of the TTL to reserve as slack
// when computing I/O deadlines (0 <= p < 1). Values outside range are clamped.
// A minimum of 2s is applied when deriving deadlines.
func SetLockDeadlineSlackPercent(p float64) {
	if p < 0 {
		p = 0
	}
	if p >= 1 {
		p = 0.5
	}
	LockDeadlineSlackPercent = p
}

// Hashmap constructor, hashModValue can't be negative nor beyond 10mil otherwise it will be reset to 250k.
func newHashmap(readWrite bool, hashModValue int, replicationTracker *replicationTracker, cache sop.Cache) *hashmap {
	if hashModValue <= 0 {
		hashModValue = MinimumModValue
	}
	return &hashmap{
		hashModValue:       hashModValue,
		replicationTracker: replicationTracker,
		readWrite:          readWrite,
		fileHandles:        make(map[string]*fileDirectIO, 5),
		cache:              cache,
	}
}

// Iterate through the entire set of hashmap (bucket) files, from 1 to the last bucket file to findOneFileRegion the file region
// containing the item (handle record) with 'id' (if for read), or a place for it (if for write).
//
// Each bucket file is allocated about 250,000 "sector blocks"(configurable) and in total, contains ~16,650,000
// addressable virtual IDs (handles). Typically, there should be only one bucket file as this file
// with the default numbers shown, can be used to hold 825 million items of the B-Tree, given a
// slot length of 500.
func (hm *hashmap) findOneFileRegion(ctx context.Context, forWriting bool, filename string, id sop.UUID) (fileRegionDetails, error) {
	var dio *fileDirectIO
	var result fileRegionDetails

	// Allocate a block-aligned buffer for direct I/O reads of a single block.
	alignedBuffer := directio.AlignedBlock(blockSize)
	i := 0
	for {
		// Iterate segment files (buckets) until the record is found or space is located.
		i++

		// Guardrail: avoid unbounded growth or accidental infinite loops.
		if i > 1000 {
			return result, fmt.Errorf("reached the maximum numer of segment files (1000), can't create another one")
		}

		// Compute the target segment filename for this iteration.
		segmentFilename := fmt.Sprintf("%s-%d%s", filename, i, registryFileExtension)

		if i > 1 {
			log.Debug(fmt.Sprintf("checking segment file %s", segmentFilename))
		}

		// Resolve the active path for the segment file and reuse an open handle if available.
		fn := hm.replicationTracker.formatActiveFolderEntity(fmt.Sprintf("%s%c%s", filename, os.PathSeparator, segmentFilename))
		if f, ok := hm.fileHandles[fn]; ok {
			dio = f
		} else {
			// Open existing file or create a new one (when writing). Also ensure it’s the expected size.
			dio = newFileDirectIO()
			fileExists := dio.fileExists(fn)
			var fs int64
			if fileExists {
				fs, _ = dio.getFileSize(fn)
			}
			if !fileExists || fs < hm.getSegmentFileSize() {
				if !forWriting {
					return result, fmt.Errorf("%s '%v'", idNotFoundErr, id)
				}
				// Initialize a new segment file and return the location for the first write.
				frd, err := hm.setupNewFile(ctx, forWriting, fn, id, dio)
				if dio.file != nil {
					dio.filename = segmentFilename
					hm.fileHandles[fn] = dio
				}
				return frd, err
			} else {
				// Open the existing segment file for read/write (or read-only in RO mode).
				flag := os.O_RDWR
				if !hm.readWrite {
					flag = os.O_RDONLY
				}
				if err := dio.open(ctx, fn, flag, permission); err != nil {
					return result, err
				}
				dio.filename = segmentFilename
			}
			hm.fileHandles[fn] = dio
		}

		// Calculate the block offset and the ideal slot (handleInBlockOffset) for this UUID.
		blockOffset, handleInBlockOffset := hm.getBlockOffsetAndHandleInBlockOffset(id)

		// Read one block containing the target slot.
		n, err := dio.readAt(ctx, alignedBuffer, blockOffset)
		if err != nil {
			// If we reached EOF on a short file, either return a write location or continue to next segment.
			if dio.isEOF(err) {
				if forWriting {
					result.blockOffset = blockOffset
					result.handleInBlockOffset = handleInBlockOffset
					result.dio = dio
					return result, nil
				}
				// Not found here; check next file segment.
				continue
			} else {
				return result, err
			}
		}
		if n != len(alignedBuffer) {
			// A full block is expected; partial reads indicate an unexpected short read.
			return result, fmt.Errorf("only able to read partially (%d bytes) the block record at offset %v", n, blockOffset)
		}

		// Unmarshal and check the ideal slot first.
		m := encoding.NewHandleMarshaler()
		var h sop.Handle
		hbuf := alignedBuffer[handleInBlockOffset : handleInBlockOffset+sop.HandleSizeInBytes]
		if isZeroData(hbuf) {
			// Ideal slot is empty; use it if writing, otherwise fall through to scan the block.
			if forWriting {
				result.blockOffset = blockOffset
				result.handleInBlockOffset = handleInBlockOffset
				result.dio = dio
				return result, nil
			}
		} else {
			// Ideal slot is occupied; check if it matches the requested logical ID.
			if lid, err := m.UnmarshalLogicalID(hbuf); err != nil {
				return result, err
			} else if lid == id {
				if err := m.Unmarshal(hbuf, &h); err != nil {
					return result, err
				}
				result.handle = h
				result.blockOffset = blockOffset
				result.handleInBlockOffset = handleInBlockOffset
				result.dio = dio
				return result, nil
			}
		}

		// Scan the rest of the block for either a free slot (write) or a matching ID (read).
		var bao int64
		result.dio = dio
		result.blockOffset = blockOffset
		for range handlesPerBlock {
			// Skip the ideal slot since it was already processed above.
			if bao == handleInBlockOffset {
				bao += sop.HandleSizeInBytes
				continue
			}

			hbuf := alignedBuffer[bao : bao+sop.HandleSizeInBytes]
			result.handleInBlockOffset = int64(bao)

			if isZeroData(hbuf) {
				if forWriting {
					result.dio = dio
					return result, nil
				}
			} else {
				if lid, err := m.UnmarshalLogicalID(hbuf); err != nil {
					return result, err
				} else if lid == id {
					// Found the handle; deserialize and return its location.
					if err := m.Unmarshal(hbuf, &h); err != nil {
						return result, err
					}
					result.handle = h
					result.dio = dio
					return result, nil
				}
			}

			bao += sop.HandleSizeInBytes
		}
	}
}

// Fetch the Handle record with a given UUID (LogicalID) from a given file, without locking the file region it resides in.
func (hm *hashmap) fetch(ctx context.Context, filename string, ids []sop.UUID) ([]sop.Handle, error) {
	completedItems := make([]sop.Handle, 0, len(ids))
	for _, id := range ids {
		frd, err := hm.findOneFileRegion(ctx, false, filename, id)
		if err != nil {
			// Missing IDs are normal in a sparse registry; skip them.
			if strings.Contains(err.Error(), idNotFoundErr) {
				continue
			}
			return nil, err
		}
		if frd.handle.IsEmpty() {
			continue
		}
		completedItems = append(completedItems, frd.handle)
	}
	return completedItems, nil
}

// Find the file region(s) that a set of UUIDs correlate to and return these region(s)' offsett/Handle if in case
// useful to the caller.
func (hm *hashmap) findFileRegion(ctx context.Context, filename string, ids []sop.UUID) ([]fileRegionDetails, error) {
	foundItems := make([]fileRegionDetails, 0, len(ids))
	for _, id := range ids {
		// Pre-resolve each UUID to its target file region (block + slot) for batch callers.
		frd, err := hm.findOneFileRegion(ctx, true, filename, id)
		if err != nil {
			return nil, err
		}
		foundItems = append(foundItems, frd)
	}
	return foundItems, nil
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
	hm.fileHandles = make(map[string]*fileDirectIO)
	return lastError
}

func (hm *hashmap) getBlockOffsetAndHandleInBlockOffset(id sop.UUID) (int64, int64) {
	high, low := id.Split()
	blockOffset := high % uint64(hm.hashModValue)
	offsetInBlock := low % uint64(handlesPerBlock)
	return int64(blockOffset * blockSize), int64(offsetInBlock * sop.HandleSizeInBytes)
}

func (hm *hashmap) setupNewFile(ctx context.Context, forWriting bool, filename string, id sop.UUID, dio *fileDirectIO) (fileRegionDetails, error) {
	var result fileRegionDetails
	flag := os.O_CREATE | os.O_RDWR
	if !forWriting {
		flag = os.O_RDONLY
	}

	// Coordinate file preallocation across processes using a distributed lock.
	lk := hm.cache.CreateLockKeys([]string{preallocateFileLockKey})
	if ok, _, err := hm.cache.Lock(ctx, lockPreallocateFileTimeout, lk); !ok || err != nil {
		if err == nil {
			err = fmt.Errorf("can't acquire a lock to preallocate file %s", filename)
		}
		return result, err
	}

	if err := dio.open(ctx, filename, flag, permission); err != nil {
		hm.cache.Unlock(ctx, lk)
		return result, err
	}

	// Pre-allocate the full segment file to ensure subsequent direct I/O works with fixed offsets.
	if err := dio.file.Truncate(hm.getSegmentFileSize()); err != nil {
		hm.cache.Unlock(ctx, lk)
		return result, err
	}
	hm.cache.Unlock(ctx, lk)

	// Return the computed location for the caller to write the first record.
	blockOffset, handleInBlockOffset := hm.getBlockOffsetAndHandleInBlockOffset(id)
	result.blockOffset = blockOffset
	result.handleInBlockOffset = handleInBlockOffset
	result.dio = dio
	return result, nil
}

func (hm *hashmap) getSegmentFileSize() int64 {
	return int64(hm.hashModValue) * blockSize
}

func isZeroData(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}
