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
			frd.lockKey = hm.cache.CreateLockKeys(hm.formatLockKey(frd.dio.filename, frd.offset))[0]
			if ok, err := hm.cache.Lock(ctx, lockFileRegionDuration, frd.lockKey); ok {
				continue
			} else if err == nil {
				return &sop.UpdateAllOrNothingError{
					Err: fmt.Errorf("can't lock file (%s) region offset %v, already locked", frd.dio.filename, frd.offset),
				}
			} else {
				return err
			}
		}
		if err := frd.dio.lockFileRegion(ctx, frd.offset, sop.HandleSizeInBytes, lockFileRegionAttemptTimeout); err != nil {
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
		if err := frd.dio.unlockFileRegion(frd.offset, sop.HandleSizeInBytes); err != nil {
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

func (hm *hashmap) updateFileRegion(fileRegionDetails ...fileRegionDetails) error {
	m := encoding.NewHandleMarshaler()
	for _, frd := range fileRegionDetails {
		ba, _ := m.Marshal(frd.handle)
		if n, err := frd.dio.writeAt(ba, frd.offset); n != len(ba) || err != nil {
			if err != nil {
				return err
			}
			return fmt.Errorf("only partially (n=%d) wrote at offset %v, data: %v", n, frd.offset, frd.handle)
		}
	}
	return nil
}

func (hm *hashmap) markDeleteFileRegion(fileRegionDetails ...fileRegionDetails) error {
	// Study whether we want to zero out only the "Logical ID" part. For now, zero out entire Handle block
	// which could aid in cleaner deleted blocks(as marked w/ all zeroes). Negligible difference in IO.
	ba := bytes.Repeat([]byte{0}, sop.HandleSizeInBytes)
	for _, frd := range fileRegionDetails {
		if n, err := frd.dio.writeAt(ba, frd.offset); n != len(ba) || err != nil {
			if err != nil {
				return err
			}
			return fmt.Errorf("only partially (n=%d) wrote 0s at offset %v", n, frd.offset)
		}
	}
	return nil
}

func (hm *hashmap) formatLockKey(filename string, offset int64) string {
	return hm.cache.FormatLockKey(fmt.Sprintf("%s%s%v", lockFileRegionKeyPrefix, filename, offset))
}
