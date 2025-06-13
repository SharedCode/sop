package sop

import (
	"context"
	"fmt"
	log "log/slog"
	"math/rand"
	"time"

	"github.com/sethvargo/go-retry"
)

// Is it timed out yet?
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

// Random sleep also but you can specify a value of sleep unit amount.
func RandomSleepWithUnit(ctx context.Context, unit time.Duration) {
	sleepTime := time.Duration(rand.Intn(5))
	if sleepTime == 0 {
		sleepTime = 1
	}
	st := sleepTime * unit
	log.Debug(fmt.Sprintf("sleep for %d * %d unit", sleepTime, unit))
	Sleep(ctx, st)
}

// Sleep in random milli-seconds to allow different conflicting (Node modifying) transactions
// to retry on different times, thus, increasing chance to succeed one after the other.
func RandomSleep(ctx context.Context) {
	RandomSleepWithUnit(ctx, 20*time.Millisecond)
}

// sleep with context.
func Sleep(ctx context.Context, sleepTime time.Duration) {
	if sleepTime <= 0 {
		return
	}
	sleep, cancel := context.WithTimeout(ctx, sleepTime)
	defer cancel()
	<-sleep.Done()
}

// Retry with default backoff & increasing wait in between retries.
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
