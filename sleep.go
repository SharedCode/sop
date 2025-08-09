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
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) || errors.Is(err, os.ErrClosed) {
		return false
	}
	if strings.Contains(err.Error(), "read-only file system") {
		return false
	}

	return true
}
