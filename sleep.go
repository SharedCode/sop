package sop

import (
	"context"
	"fmt"
	log "log/slog"
	"math/rand"
	"time"
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

// Sleep in random milli-seconds to allow different conflicting (Node modifying) transactions
// to retry on different times, thus, increasing chance to succeed one after the other.
func RandomSleep(ctx context.Context) {
	sleepTime := rand.Intn(5) * 20
	if sleepTime == 0 {
		sleepTime = 2
	}
	log.Debug(fmt.Sprintf("sleep for %d millis", sleepTime))
	Sleep(ctx, time.Duration(sleepTime)*time.Millisecond)
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
