package fs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/SharedCode/sop"
	"github.com/ncw/directio"
)

type directIO struct {
	file                       *os.File
	filename                   string
	cache                      sop.Cache
	useCacheForFileRegionLocks bool
}

const (
	blockSize              = directio.BlockSize
	lockKeyPrefix          = "infs"
	lockFileRegionDuration = time.Duration(15 * time.Minute)
)

var errBlocked = errors.New("acquiring lock is blocked by another process")

// Instantiate a direct File IO object.
func newDirectIO(cache sop.Cache, useCacheForFileRegionLocks bool) *directIO {
	return &directIO{
		cache:                      cache,
		useCacheForFileRegionLocks: useCacheForFileRegionLocks,
	}
}

// Open the file with a given filename.
func (dio *directIO) open(filename string, flag int, permission os.FileMode) error {
	if dio.file != nil {
		return fmt.Errorf("there is an opened file for this directIO object, 'not allowed to open file again")
	}
	f, err := directio.OpenFile(filename, flag, permission)
	if err != nil {
		return err
	}
	dio.file = f
	dio.filename = filename
	return nil
}

func (dio *directIO) fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

// Create a buffer that is aligned to the file sector size, usable as buffer for reading file data, directly.
func (dio *directIO) createAlignedBlock() []byte {
	return dio.createAlignedBlockOfSize(directio.BlockSize)
}

// Create a buffer that is aligned to the file sector size, usable as buffer for reading file data, directly.
func (dio *directIO) createAlignedBlockOfSize(blockSize int) []byte {
	return directio.AlignedBlock(blockSize)
}

func (dio *directIO) writeAt(block []byte, offset int64) (int, error) {
	if dio.file == nil {
		return 0, fmt.Errorf("can't write, there is no opened file")
	}
	return dio.file.WriteAt(block, offset)
}

func (dio *directIO) readAt(block []byte, offset int64) (int, error) {
	if dio.file == nil {
		return 0, fmt.Errorf("can't read, there is no opened file")
	}
	return dio.file.ReadAt(block, offset)
}

func (dio *directIO) lockFileRegion(ctx context.Context, readWrite bool, offset int64, length int64, timeout time.Duration) error {
	if dio.file == nil {
		return fmt.Errorf("can't lock file region, there is no opened file")
	}

	if dio.useCacheForFileRegionLocks {
		lk := dio.cache.CreateLockKeys(fmt.Sprintf("%s%s%v", lockKeyPrefix, dio.filename, offset))
		if err := dio.cache.Lock(ctx, lockFileRegionDuration, lk...); err != nil {
			return err
		}
		return nil
	}

	var t int16 = syscall.F_WRLCK
	if !readWrite {
		t = syscall.F_RDLCK
	}
	flock := syscall.Flock_t{
		Type:  t,
		Start: offset,
		Len:   length,
		Pid:   int32(syscall.Getpid()),
	}

	if timeout <= 0 {
		return syscall.FcntlFlock(dio.file.Fd(), syscall.F_SETLK, &flock)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		err := syscall.FcntlFlock(dio.file.Fd(), syscall.F_SETLKW, &flock)
		done <- err
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return errBlocked
	}
}

func (dio *directIO) isRegionLocked(ctx context.Context, readWrite bool, offset int64, length int64) (bool, error) {
	if dio.file == nil {
		return false, fmt.Errorf("can't check if region is locked, there is no opened file")
	}

	if dio.useCacheForFileRegionLocks {
		lk := dio.cache.FormatLockKey(fmt.Sprintf("%s%s%v", lockKeyPrefix, dio.filename, offset))
		return dio.cache.IsLockedByOthers(ctx, lk)
	}

	var t int16 = syscall.F_WRLCK
	if !readWrite {
		t = syscall.F_RDLCK
	}
	flock := syscall.Flock_t{
		Type:   t,
		Start:  offset,
		Len:    length,
		Pid:    0,
		Whence: 0,
	}

	err := syscall.FcntlFlock(dio.file.Fd(), syscall.F_GETLK, &flock)
	if err != nil {
		return false, err
	}

	// If lock.Type is F_UNLCK, no lock exists
	return flock.Type != syscall.F_UNLCK, nil
}

func (dio *directIO) unlockFileRegion(ctx context.Context, offset int64, length int64) error {
	if dio.file == nil {
		return fmt.Errorf("can't unlock file region, there is no opened file")
	}

	if dio.useCacheForFileRegionLocks {
		lk := dio.cache.CreateLockKeys(fmt.Sprintf("%s%s%v", lockKeyPrefix, dio.filename, offset))
		if err := dio.cache.Unlock(ctx, lk...); err != nil {
			return err
		}
		return nil
	}

	flock := syscall.Flock_t{
		Type:  syscall.F_UNLCK, // Unlock
		Start: offset,
		Len:   length,
		Pid:   int32(syscall.Getpid()),
	}

	return syscall.FcntlFlock(dio.file.Fd(), syscall.F_SETLK, &flock)
}

func (dio *directIO) close() error {
	if dio.file == nil {
		return nil
	}

	err := dio.file.Close()
	dio.file = nil
	return err
}
