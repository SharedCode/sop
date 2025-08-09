package sop

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// TaskRunner is a thin wrapper around errgroup.Group that carries a context for convenience.
// Consider using errgroup directly in new code.
type TaskRunner struct {
	eg      *errgroup.Group
	context context.Context
}

// NewTaskRunner creates a new TaskRunner. maxThreadCount > 0 limits the number of concurrent goroutines.
func NewTaskRunner(ctx context.Context, maxThreadCount int) *TaskRunner {
	eg, ctx2 := errgroup.WithContext(ctx)
	if maxThreadCount > 0 {
		eg.SetLimit(maxThreadCount)
	}
	return &TaskRunner{
		eg:      eg,
		context: ctx2,
	}
}

// GetContext returns the TaskRunner's context.
func (tr *TaskRunner) GetContext() context.Context {
	return tr.context
}

// Go runs the provided task function in a new goroutine managed by the underlying errgroup.
func (tr *TaskRunner) Go(task func() error) {
	tr.eg.Go(task)
}

// Wait waits for all launched tasks to complete and returns the first encountered error, if any.
func (tr *TaskRunner) Wait() error {
	return tr.eg.Wait()
}
