package fs

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop"
)

func (hm *hashmap) formatLockKey(filename string, offset int64) string {
	return hm.cache.FormatLockKey(fmt.Sprintf("%s%s%v", lockFileRegionKeyPrefix, filename, offset))
}

func (hm *hashmap) lockFoundFileRegion(ctx context.Context, fileRegionDetails ...*fileRegionDetails) error {
	for _, frd := range fileRegionDetails {
		if hm.useCacheForFileRegionLocks {
			frd.lockKey = hm.cache.CreateLockKeys(hm.formatLockKey(frd.dio.filename, frd.offset))[0]
			if err := hm.cache.Lock(ctx, lockFileRegionDuration, frd.lockKey); err != nil {
				return err
			}
			continue
		}
		if err := frd.dio.lockFileRegion(ctx, hm.readWrite, frd.offset, sop.HandleSizeInBytes, lockFileRegionTimeout); err != nil {
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
		if err := frd.dio.unlockFileRegion(ctx, frd.offset, sop.HandleSizeInBytes); err != nil {
			return err
		}
	}
	return nil
}

func (hm *hashmap) isRegionLocked(ctx context.Context, dio *directIO, offset int64) (bool, error) {
	if hm.useCacheForFileRegionLocks {
		lkn := hm.formatLockKey(dio.filename, offset)
		return hm.cache.IsLockedByOthers(ctx, lkn)
	}
	return dio.isRegionLocked(ctx, hm.readWrite, offset, sop.HandleSizeInBytes)
}

func (hm *hashmap) updateFileRegion(ctx context.Context, fileRegionDetails ...fileRegionDetails) error {
	// for _, frd := range fileRegionDetails {
	// 	frd.dio.writeAt()
	// }
	return nil
}
