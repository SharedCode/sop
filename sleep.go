package sop

import (
	"context"
	"errors"
	"fmt"
	log "log/slog"
	"math/rand"
	"os"
	"strings"
	"time"
	"syscall"

	"github.com/sethvargo/go-retry"
)

// jitterRNG is the random source used for sleep jitter. It is seeded once at init time.
var jitterRNG = rand.New(rand.NewSource(time.Now().UnixNano()))

// SetJitterRNG overrides the RNG used for sleep jitter. Useful for deterministic tests.
func SetJitterRNG(r *rand.Rand) {
	if r != nil {
		jitterRNG = r
	}
}

// TimedOut returns an error if the context is done or if the elapsed time since startTime exceeds maxTime.
func TimedOut(ctx context.Context, name string, startTime time.Time, maxTime time.Duration) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	diff := Now().Sub(startTime)
	if diff > maxTime {
		return fmt.Errorf("%s timed out(maxTime=%v)", name, maxTime)
	}
	return nil
}

// RandomSleepWithUnit sleeps for a random multiple (1..4) of the provided unit duration.
// Useful to jitter conflicting transactions and reduce contention.
func RandomSleepWithUnit(ctx context.Context, unit time.Duration) {
	sleepTime := time.Duration(jitterRNG.Intn(5))
	if sleepTime == 0 {
		sleepTime = 1
	}
	st := sleepTime * unit
	log.Debug("sleep jitter", "multiplier", sleepTime, "unit", unit, "duration", st)
	Sleep(ctx, st)
}

// RandomSleep sleeps for a random duration between 20ms and 80ms to stagger retries.
func RandomSleep(ctx context.Context) {
	RandomSleepWithUnit(ctx, 20*time.Millisecond)
}

// Sleep blocks for the specified duration or until the context is done, whichever happens first.
func Sleep(ctx context.Context, sleepTime time.Duration) {
	if sleepTime <= 0 {
		return
	}
	sleep, cancel := context.WithTimeout(ctx, sleepTime)
	defer cancel()
	<-sleep.Done()
}

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
