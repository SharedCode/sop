package fs

import (
	"bytes"
	"context"
	"fmt"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
)

func (hm *hashmap) lockFoundFileRegion(ctx context.Context, fileRegionDetails ...*fileRegionDetails) error {
	for _, frd := range fileRegionDetails {
		if hm.useCacheForFileRegionLocks {
			frd.lockKey = hm.cache.CreateLockKeys(hm.formatLockKey(frd.dio.filename, frd.getOffset()))[0]
			if ok, err := hm.cache.Lock(ctx, lockFileRegionDuration, frd.lockKey); ok {
				continue
			} else if err == nil {
				return fmt.Errorf("can't lock file (%s) region offset %v, already locked", frd.dio.filename, frd.getOffset())
			} else {
				return err
			}
		}
		if err := frd.dio.lockFileRegion(ctx, frd.getOffset(), sop.HandleSizeInBytes, lockFileRegionAttemptTimeout); err != nil {
			return err
		}
	}
	return nil
}

// Unlock file region(s).
func (hm *hashmap) unlockFileRegion(ctx context.Context, fileRegionDetails ...fileRegionDetails) error {
	for _, frd := range fileRegionDetails {
		if hm.useCacheForFileRegionLocks {
			if err := hm.cache.Unlock(ctx, frd.lockKey); err != nil {
				return err
			}
			continue
		}
		if err := frd.dio.unlockFileRegion(frd.getOffset(), sop.HandleSizeInBytes); err != nil {
			return err
		}
	}
	return nil
}

func (hm *hashmap) isRegionLocked(ctx context.Context, dio *directIO, offset int64) (bool, error) {
	if hm.useCacheForFileRegionLocks {
		return hm.cache.IsLockedByOthers(ctx, hm.formatLockKey(dio.filename, offset))
	}
	return dio.isRegionLocked(ctx, true, offset, sop.HandleSizeInBytes)
}

func (hm *hashmap) updateFileRegion(ctx context.Context, fileRegionDetails ...fileRegionDetails) error {
	dio := newDirectIO()
	ba := dio.createAlignedBlock()
	m := encoding.NewHandleMarshaler()
	for _, frd := range fileRegionDetails {
		ba2, _ := m.Marshal(frd.handle)
		if err := hm.updateFileBlockRegion(ctx, frd.dio, frd.blockOffset, int(frd.handleInBlockOffset), ba2, ba); err != nil {
			return err
		}
	}
	return nil
}

func (hm *hashmap) markDeleteFileRegion(ctx context.Context, fileRegionDetails ...fileRegionDetails) error {
	dio := newDirectIO()
	ba := dio.createAlignedBlock()
	// Study whether we want to zero out only the "Logical ID" part. For now, zero out entire Handle block
	// which could aid in cleaner deleted blocks(as marked w/ all zeroes). Negligible difference in IO.
	ba2 := bytes.Repeat([]byte{0}, sop.HandleSizeInBytes)
	for _, frd := range fileRegionDetails {
		if err := hm.updateFileBlockRegion(ctx, frd.dio, frd.blockOffset, int(frd.handleInBlockOffset), ba2, ba); err != nil {
			return err
		}
	}
	return nil
}

func (hm *hashmap) updateFileBlockRegion(ctx context.Context, dio *directIO, blockOffset int64, handleInBlockOffset int, handleData []byte, alignedBuffer []byte) error {
	// Lock the block file region.
	var lk *sop.LockKey
	var err error
	if lk, err = hm.lockFileBlockRegion(ctx, dio, blockOffset); err != nil {
		return err
	}

	// Read the block file region data.
	if n, err := dio.readAt(alignedBuffer, blockOffset); n != blockSize || err != nil {
		hm.unlockFileBlockRegion(ctx, dio, blockOffset, lk)
		if err == nil {
			return fmt.Errorf("only partially (n=%d) read the block at offset %v", n, blockOffset)
		}
		return err
	}

	// Merge the updated Handle record w/ the read block file region data.
	copy(alignedBuffer[handleInBlockOffset:handleInBlockOffset+sop.HandleSizeInBytes], handleData)
	// Update the block file region with merged data.
	if n, err := dio.writeAt(alignedBuffer, blockOffset); n != blockSize || err != nil {
		hm.unlockFileBlockRegion(ctx, dio, blockOffset, lk)
		if err == nil {
			return fmt.Errorf("only partially (n=%d) wrote at block offset %v, data: %v", n, blockOffset, handleData)
		}
		return err
	}
	// Unlock the block file region.
	return hm.unlockFileBlockRegion(ctx, dio, blockOffset, lk)
}

func (hm *hashmap) lockFileBlockRegion(ctx context.Context, dio *directIO, offset int64) (*sop.LockKey, error) {
	if hm.useCacheForFileRegionLocks {
		lk := hm.cache.CreateLockKeys(hm.formatLockKey(dio.filename, offset))[0]
		if ok, err := hm.cache.Lock(ctx, lockFileRegionDuration, lk); ok {
			return lk, nil
		} else if err == nil {
			return nil, fmt.Errorf("can't lock file (%s) block region offset %v, already locked", dio.filename, offset)
		} else {
			return nil, err
		}
	}
	if err := dio.lockFileRegion(ctx, offset, int64(blockSize), lockFileRegionDuration); err != nil {
		return nil, err
	}
	return nil, nil
}
func (hm *hashmap) unlockFileBlockRegion(ctx context.Context, dio *directIO, offset int64, lk *sop.LockKey) error {
	if hm.useCacheForFileRegionLocks {
		return hm.cache.Unlock(ctx, lk)
	}
	return dio.unlockFileRegion(offset, int64(blockSize))
}

func (hm *hashmap) formatLockKey(filename string, offset int64) string {
	return hm.cache.FormatLockKey(fmt.Sprintf("%s%s%v", lockFileRegionKeyPrefix, filename, offset))
}
