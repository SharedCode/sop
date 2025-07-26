package fs

import (
	"context"
	"os"

	"github.com/ncw/directio"

	retry "github.com/sethvargo/go-retry"
	"github.com/sharedcode/sop"
)

// DirectIO API.
type DirectIO interface {
	// Open the file with a given filename.
	Open(ctx context.Context, filename string, flag int, permission os.FileMode) (*os.File, error)
	WriteAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error)
	ReadAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error)
	Close(file *os.File) error
}

const (
	blockSize = directio.BlockSize
)

type directIO struct{}

// NewDirectIO creates the DirectIO instance that implements the "direct IO" calls.
func NewDirectIO() DirectIO {
	return &directIO{}
}

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
