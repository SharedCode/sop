package fs

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/SharedCode/sop"
	retry "github.com/sethvargo/go-retry"
)

// Functions for File I/O defaults to "os" file I/O functions.
type FileIO interface {
	WriteFile(ctx context.Context, name string, data []byte, perm os.FileMode) error
	ReadFile(ctx context.Context, name string) ([]byte, error)
	Remove(ctx context.Context, name string) error
	Exists(ctx context.Context, path string) bool

	// Directory API.
	RemoveAll(ctx context.Context, path string) error
	MkdirAll(ctx context.Context, path string, perm os.FileMode) error
	ReadDir(ctx context.Context, sourceDir string) ([]os.DirEntry, error)
}

type defaultFileIO struct {
}

func NewFileIO() FileIO {
	return &defaultFileIO{}
}

func (dio defaultFileIO) WriteFile(ctx context.Context, name string, data []byte, perm os.FileMode) error {
	if err := os.WriteFile(name, data, perm); err != nil {
		dirPath := filepath.Dir(name)
		if derr := dio.MkdirAll(ctx, dirPath, perm); derr == nil {
			return sop.Retry(ctx, func(context.Context) error {
				err := os.WriteFile(name, data, perm)
				if err != nil {
					return retry.RetryableError(
						sop.Error{
							Code: sop.FileIOError,
							Err:  err,
						})
				}
				return nil
			}, nil)
		}
		return err
	}
	return nil
}
func (dio defaultFileIO) ReadFile(ctx context.Context, name string) ([]byte, error) {
	var ba []byte
	err := sop.Retry(ctx, func(context.Context) error {
		var err error
		ba, err = os.ReadFile(name)
		if err != nil {
			return retry.RetryableError(
				sop.Error{
					Code: sop.FileIOError,
					Err:  err,
				})
		}
		return nil
	}, nil)
	return ba, err
}
func (dio defaultFileIO) Remove(ctx context.Context, name string) error {
	return sop.Retry(ctx, func(context.Context) error {
		err := os.Remove(name)
		if err != nil {
			return retry.RetryableError(
				sop.Error{
					Code: sop.FileIOError,
					Err:  err,
				})
		}
		return nil
	}, nil)
}

func (dio defaultFileIO) MkdirAll(ctx context.Context, path string, perm os.FileMode) error {
	return sop.Retry(ctx, func(context.Context) error {
		err := os.MkdirAll(path, perm)
		if err != nil && !strings.Contains(err.Error(), "read-only file system") {
			return retry.RetryableError(
				sop.Error{
					Code: sop.FileIOError,
					Err:  err,
				})
		}
		return nil
	}, nil)
}
func (dio defaultFileIO) RemoveAll(ctx context.Context, path string) error {
	return sop.Retry(ctx, func(context.Context) error {
		err := os.RemoveAll(path)
		if err != nil {
			return retry.RetryableError(
				sop.Error{
					Code: sop.FileIOError,
					Err:  err,
				})
		}
		return nil
	}, nil)
}
func (dio defaultFileIO) Exists(ctx context.Context, path string) bool {
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		return true
	}
	return false
}
func (dio defaultFileIO) ReadDir(ctx context.Context, sourceDir string) ([]os.DirEntry, error) {
	var r []os.DirEntry
	err := sop.Retry(ctx, func(context.Context) error {
		var err error
		r, err = os.ReadDir(sourceDir)
		if err != nil {
			return retry.RetryableError(sop.Error{
				Code: sop.FileIOError,
				Err:  err,
			})
		}
		return nil
	}, nil)
	return r, err
}
