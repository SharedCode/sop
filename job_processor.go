package sop

import (
	"context"
	log "log/slog"

	"golang.org/x/sync/errgroup"
)

// JobProcessor function launches a task (thread) spinner & returns a channel (errgroup)
// you can use to enqueue function tasks and for awaiting completion
// of all "spinned off" threads from the tasks enqueued.
func JobProcessor(ctx context.Context, bufferSize int) (chan func() error, *errgroup.Group) {
	workChannel := make(chan func() error, bufferSize)

	eg, ctx2 := errgroup.WithContext(ctx)

	// Spin off a worker thread that spins off task workers & listens for close the channel signal.
	go (func() {
		for {
			select {
			case <-ctx2.Done():
				log.Debug("ctx2 receieved a done signal")
				return
			default:
				task, ok := <-workChannel
				if !ok {
					return
				}
				eg.Go(task)
			}
		}
	})()

	return workChannel, eg
}
