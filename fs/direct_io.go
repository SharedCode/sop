package fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/ncw/directio"
)

type directIO struct {
	file     *os.File
	filename string
}

const (
	blockSize = directio.BlockSize
)

var errBlocked = errors.New("acquiring lock is blocked by another process")

// Instantiate a direct File IO object.
func newDirectIO() *directIO {
	return &directIO{}
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

func (dio *directIO) getFileSize(filePath string) (int64, error) {
	s, err := os.Stat(filePath)
	return s.Size(), err
}

func (dio *directIO) isEOF(err error) bool {
	return io.EOF == err
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

func (dio *directIO) lockFileRegion(ctx context.Context, offset int64, length int64, timeout time.Duration) error {
func (dio *directIO) lockFileRegion(ctx context.Context, offset int64, length int64, timeout time.Duration) error {
	if dio.file == nil {
		return fmt.Errorf("can't lock file region, there is no opened file")
	}

	flock := syscall.Flock_t{
		Type:  syscall.F_WRLCK,
		Whence: 0,	// SEEK_SET
		Start: offset,
		Len:   length,
		Pid:   int32(syscall.Getpid()),
	}

	if timeout <= 0 {
		return syscall.FcntlFlock(dio.file.Fd(), syscall.F_SETLK, &flock)
	}

	sleep, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	done := make(chan error, 1)


	go func() {
		err := syscall.FcntlFlock(dio.file.Fd(), syscall.F_SETLKW, &flock)
		done <- err
		close(done)
		close(done)
	}()

	select {
	case err := <-done:
		return err
	case <-sleep.Done():
		return errBlocked
	}
}

func (dio *directIO) isRegionLocked(ctx context.Context, readWrite bool, offset int64, length int64) (bool, error) {
	if dio.file == nil {
		return false, fmt.Errorf("can't check if region is locked, there is no opened file")
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

func (dio *directIO) unlockFileRegion(offset int64, length int64) error {
	if dio.file == nil {
		return fmt.Errorf("can't unlock file region, there is no opened file")
	}

	flock := syscall.Flock_t{
		Type:  syscall.F_UNLCK, // Unlock
		Start: offset,
		Len:   length,
		Whence: 0,	// SEEK_SET
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
	dio.filename = ""
	return err
}
