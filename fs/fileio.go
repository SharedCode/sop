// Package fs contains filesystem-backed implementations used by SOP.
// This file provides a thin FileIO abstraction over os with retry semantics.
package fs

import (
	"context"
	"os"
	"path/filepath"

	retry "github.com/sethvargo/go-retry"
	"github.com/sharedcode/sop"
)

// FileIO defines filesystem operations used by this package. The default
// implementation delegates to the standard library's os package with retry
// semantics for transient errors.
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
	// stateless implementation; methods delegate to os with retries
}

// NewFileIO returns a FileIO that performs I/O via the os package with basic
// retry handling for transient errors (e.g., NFS hiccups). Directories are
// created on-demand for writes.
func NewFileIO() FileIO {
	return &defaultFileIO{}
}

// WriteFile writes data to a file, creating parent directories if needed, and
// retries on transient errors using SOP's retry policy.
func (dio defaultFileIO) WriteFile(ctx context.Context, name string, data []byte, perm os.FileMode) error {
	if err := os.WriteFile(name, data, perm); err != nil {
		dirPath := filepath.Dir(name)
		// Ensure parent directories exist with sensible directory permissions.
		if derr := dio.MkdirAll(ctx, dirPath, 0o755); derr == nil {
			var nonRetryableError error
			// Parent created (or already existed): retry the write to tolerate transient errors.
			derr = sop.Retry(ctx, func(context.Context) error {
				err := os.WriteFile(name, data, perm)
				if sop.ShouldRetry(err) {
					return retry.RetryableError(
						sop.Error{
							Code: sop.FileIOError,
							Err:  err,
						})
				}
				nonRetryableError = err
				// Tell Retry not to retry on non-retryable errors.
				return nil
			}, nil)
			if derr != nil {
				return derr
			}
			return nonRetryableError
		}
		// Parent creation failed: surface the original write error to the caller.
		return err
	}
	return nil
}

// ReadFile reads an entire file into memory with retry on transient errors.
func (dio defaultFileIO) ReadFile(ctx context.Context, name string) ([]byte, error) {
	var ba []byte
	var nonRetryableError error
	err := sop.Retry(ctx, func(context.Context) error {
		var err error
		ba, err = os.ReadFile(name)
		if sop.ShouldRetry(err) {
			return retry.RetryableError(
				sop.Error{
					Code: sop.FileIOError,
					Err:  err,
				})
		}
		nonRetryableError = err
		return nil
	}, nil)
	if err != nil {
		return ba, err
	}
	return ba, nonRetryableError
}

// Remove deletes a file with retry on transient errors.
func (dio defaultFileIO) Remove(ctx context.Context, name string) error {
	var nonRetryableError error
	err := sop.Retry(ctx, func(context.Context) error {
		err := os.Remove(name)
		if sop.ShouldRetry(err) {
			return retry.RetryableError(
				sop.Error{
					Code: sop.FileIOError,
					Err:  err,
				})
		}
		nonRetryableError = err
		return nil
	}, nil)
	if err != nil {
		return err
	}
	return nonRetryableError
}

// MkdirAll creates a directory tree with retry on transient errors.
func (dio defaultFileIO) MkdirAll(ctx context.Context, path string, perm os.FileMode) error {
	var nonRetryableError error
	err := sop.Retry(ctx, func(context.Context) error {
		err := os.MkdirAll(path, perm)
		if sop.ShouldRetry(err) {
			return retry.RetryableError(
				sop.Error{
					Code: sop.FileIOError,
					Err:  err,
				})
		}
		nonRetryableError = err
		return nil
	}, nil)
	if err != nil {
		return err
	}
	return nonRetryableError
}

// RemoveAll removes a directory tree with retry on transient errors.
func (dio defaultFileIO) RemoveAll(ctx context.Context, path string) error {
	var nonRetryableError error
	err := sop.Retry(ctx, func(context.Context) error {
		err := os.RemoveAll(path)
		if sop.ShouldRetry(err) {
			return retry.RetryableError(
				sop.Error{
					Code: sop.FileIOError,
					Err:  err,
				})
		}
		nonRetryableError = err
		return nil
	}, nil)
	if err != nil {
		return err
	}
	return nonRetryableError
}

// Exists returns true if the given path exists (file or directory).
func (dio defaultFileIO) Exists(ctx context.Context, path string) bool {
	// Treat any error other than os.ErrNotExist as an "exists" signal.
	// Permission or transient I/O errors should not be interpreted as missing path.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		return true
	}
	return false
}

// ReadDir reads directory entries with retry on transient errors.
func (dio defaultFileIO) ReadDir(ctx context.Context, sourceDir string) ([]os.DirEntry, error) {
	var r []os.DirEntry
	var nonRetryableError error
	// Use SOP retry policy to soften intermittent filesystem/NFS glitches during listings.
	err := sop.Retry(ctx, func(context.Context) error {
		var err error
		r, err = os.ReadDir(sourceDir)
		if sop.ShouldRetry(err) {
			// Signal retry with a wrapped error that preserves the root cause and policy code.
			return retry.RetryableError(sop.Error{
				Code: sop.FileIOError,
				Err:  err,
			})
		}
		nonRetryableError = err
		return nil
	}, nil)

	// return the last error.
	if err != nil {
		return r, err
	}
	return r, nonRetryableError
}
