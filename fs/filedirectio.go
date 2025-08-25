package fs

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/ncw/directio"
)

type fileDirectIO struct {
	file     *os.File
	filename string
	directIO DirectIO
}

// DirectIOSim is a simulated DirectIO implementation for testing.
var DirectIOSim DirectIO

// Instantiate a direct File IO object.
// Uses DirectIO implementation to perform aligned, page-sized reads/writes
// suitable for registry segment access.
func newFileDirectIO() *fileDirectIO {
	return newFileDirectIOInjected(DirectIOSim)
}

// Allows unit tests to inject a fake DirectIO.
func newFileDirectIOInjected(dio DirectIO) *fileDirectIO {
	directIO := dio
	if directIO == nil {
		directIO = NewDirectIO()
	}
	return &fileDirectIO{
		directIO: directIO,
	}
}

// open the file with a given filename.
// Enforces single open per instance to avoid handle leaks.
func (fio *fileDirectIO) open(ctx context.Context, filename string, flag int, permission os.FileMode) error {
	if fio.file != nil {
		return fmt.Errorf("there is an opened file for this directIO object, 'not allowed to open file again")
	}
	f, err := fio.directIO.Open(ctx, filename, flag, permission)
	if err != nil {
		return err
	}
	fio.file = f
	fio.filename = filename
	return nil
}

// writeAt writes an aligned block at a specific offset using direct I/O.
func (fio *fileDirectIO) writeAt(ctx context.Context, block []byte, offset int64) (int, error) {
	if fio.file == nil {
		return 0, fmt.Errorf("can't write, there is no opened file")
	}
	return fio.directIO.WriteAt(ctx, fio.file, block, offset)
}

// readAt reads an aligned block at a specific offset using direct I/O.
func (fio *fileDirectIO) readAt(ctx context.Context, block []byte, offset int64) (int, error) {
	if fio.file == nil {
		return 0, fmt.Errorf("can't read, there is no opened file")
	}
	return fio.directIO.ReadAt(ctx, fio.file, block, offset)
}

// close closes the underlying file handle if open.
func (fio *fileDirectIO) close() error {
	if fio.file == nil {
		return nil
	}

	err := fio.directIO.Close(fio.file)
	fio.file = nil
	fio.filename = ""
	return err
}

// fileExists reports whether a path exists on disk.
func (fio *fileDirectIO) fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

// getFileSize returns the current size of the file at filePath.
func (fio *fileDirectIO) getFileSize(filePath string) (int64, error) {
	s, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}
	return s.Size(), nil
}

// isEOF reports whether the error indicates end-of-file.
func (fio *fileDirectIO) isEOF(err error) bool {
	return io.EOF == err
}

// createAlignedBlock creates a sector-aligned buffer sized to the default block size.
func (fio *fileDirectIO) createAlignedBlock() []byte {
	return fio.createAlignedBlockOfSize(directio.BlockSize)
}

// createAlignedBlockOfSize creates a sector-aligned buffer sized to blockSize bytes.
func (fio *fileDirectIO) createAlignedBlockOfSize(blockSize int) []byte {
	return directio.AlignedBlock(blockSize)
}
