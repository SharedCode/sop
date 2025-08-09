package fs

import (
	"context"
	"os"

	"github.com/ncw/directio"

	retry "github.com/sethvargo/go-retry"
	"github.com/sharedcode/sop"
)

// DirectIO exposes unbuffered file operations using O_DIRECT semantics where
// supported. It is intended for large, block-aligned I/O on segment files.
// Implementations should be used with directio.AlignedBlock buffers and block-aligned offsets.
type DirectIO interface {
	// Open opens a file with the given name and flags using direct I/O when possible.
	Open(ctx context.Context, filename string, flag int, permission os.FileMode) (*os.File, error)
	// WriteAt writes a block at the given offset.
	WriteAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error)
	// ReadAt reads a block at the given offset.
	ReadAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error)
	// Close closes the provided file handle.
	Close(file *os.File) error
}

const (
	// blockSize is the alignment size required by the direct I/O implementation.
	blockSize = directio.BlockSize
)

type directIO struct{}

// NewDirectIO returns a DirectIO implementation backed by github.com/ncw/directio.
func NewDirectIO() DirectIO {
	return &directIO{}
}

// Open wraps directio.OpenFile with SOP retry semantics. Transient errors are wrapped
// as retryable to allow the caller's policy to reattempt.
func (dio directIO) Open(ctx context.Context, filename string, flag int, permission os.FileMode) (*os.File, error) {
	var f *os.File
	err := sop.Retry(ctx, func(context.Context) error {
		var err error
		f, err = directio.OpenFile(filename, flag, permission)
		if sop.ShouldRetry(err) {
			return retry.RetryableError(
				sop.Error{
					Code: sop.FileIOError,
					Err:  err,
				})
		}
		return nil
	}, nil)
	return f, err
}

// WriteAt writes a block at an aligned offset, retrying transient errors via SOP's retry helper.
// The caller is responsible for providing an aligned buffer (e.g., via directio.AlignedBlock).
func (dio directIO) WriteAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
	var i int
	err := sop.Retry(ctx, func(context.Context) error {
		var err error
		i, err = file.WriteAt(block, offset)
		if sop.ShouldRetry(err) {
			return retry.RetryableError(
				sop.Error{
					Code: sop.FileIOError,
					Err:  err,
				})
		}
		return nil
	}, nil)
	return i, err
}

// ReadAt reads a block at an aligned offset, retrying transient errors via SOP's retry helper.
// The caller is responsible for providing an aligned buffer (e.g., via directio.AlignedBlock).
func (dio directIO) ReadAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
	var i int
	err := sop.Retry(ctx, func(context.Context) error {
		var err error
		i, err = file.ReadAt(block, offset)
		if sop.ShouldRetry(err) {
			return retry.RetryableError(
				sop.Error{
					Code: sop.FileIOError,
					Err:  err,
				})
		}
		return nil
	}, nil)
	return i, err
}

func (dio directIO) Close(file *os.File) error {
	return file.Close()
}
