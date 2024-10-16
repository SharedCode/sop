package sop

import (
	"context"
	log "log/slog"

	"golang.org/x/sync/errgroup"
)

// JobProcessor function launches a task (thread) spinner & returns a channel (& errgroup)
// you can use to enqueue function tasks(the channel) and for awaiting
// completion(the errgroup) of all "spinned off" threads from the tasks enqueued.
func JobProcessor(ctx context.Context, maxThreadCount int) (chan bool, *errgroup.Group) {
	workChannel := make(chan bool, maxThreadCount)

	eg, ctx2 := errgroup.WithContext(ctx)

	// Spin off a worker thread that spins off task workers & listens for close the channel signal.
	go (func() {
		for {
			select {
			case <-ctx2.Done():
				log.Debug("ctx2 received a done signal")
				return
			default:
				// Just free up space on the channel.
				continueSignal, ok := <-workChannel
				if !ok || !continueSignal{
					return
				}
			}
		}
	})()

	return workChannel, eg
}
