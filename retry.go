package sop

import (
	"context"
	"errors"
	log "log/slog"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/sethvargo/go-retry"
)

// Retry executes task with Fibonacci backoff up to 5 retries.
// If retries are exhausted, gaveUpTask is invoked (when not nil) and the final error is returned.
func Retry(ctx context.Context, task func(ctx context.Context) error, gaveUpTask func(ctx context.Context)) error {
	b := retry.NewFibonacci(1 * time.Second)
	if err := retry.Do(ctx, retry.WithMaxRetries(5, b), task); err != nil {
		log.Warn(err.Error() + ", gave up")
		if gaveUpTask != nil {
			gaveUpTask(ctx)
		}
		return err
	}
	return nil
}

// ShouldRetry reports whether the error is retryable (non-nil and not a known permanent failure).
func ShouldRetry(err error) bool {
	if err == nil {
		return false
	}
	// Context cancellations/timeouts are permanent from the caller's POV.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Common non-retryable OS errors and conditions.
	if errors.Is(err, os.ErrNotExist) ||
		errors.Is(err, os.ErrPermission) ||
		errors.Is(err, os.ErrClosed) ||
		errors.Is(err, os.ErrExist) {
		return false
	}

	// Non-retryable syscall errno encountered during file/dir operations.
	// Treat resource/quota/readonly/path errors as permanent to avoid tight retry loops.
	switch {
	case errors.Is(err, syscall.EROFS), // read-only filesystem
		errors.Is(err, syscall.ENOSPC), // no space left on device
		errors.Is(err, syscall.EDQUOT), // disk quota exceeded
		errors.Is(err, syscall.EMFILE), // too many open files (per-process)
		errors.Is(err, syscall.ENFILE), // too many open files (system-wide)
		errors.Is(err, syscall.EACCES), // permission denied
		errors.Is(err, syscall.EPERM),  // operation not permitted
		errors.Is(err, syscall.ENAMETOOLONG),
		errors.Is(err, syscall.ENOTDIR),
		errors.Is(err, syscall.EISDIR),
		errors.Is(err, syscall.ENOTEMPTY),
		errors.Is(err, syscall.EMLINK),
		errors.Is(err, syscall.ELOOP),
		errors.Is(err, syscall.EXDEV),  // invalid cross-device link
		errors.Is(err, syscall.EEXIST), // file exists
		errors.Is(err, syscall.EINVAL): // invalid argument (for many FS ops typically caller bug)
		return false
	}

	// Last-resort heuristic for EROFS text across platforms/drivers.
	if strings.Contains(err.Error(), "read-only file system") {
		return false
	}

	return true
}
