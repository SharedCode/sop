package sop

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// Task Runner struct is a very simple wrapper of errgroup. Not much here, 'just use errgroup hehe.
// Most likely to drop this file and switch to errgroup soon. It is a little nice to tuck in the context.
type TaskRunner struct {
	eg      *errgroup.Group
	context context.Context
}

// Create a new task runner. maxThreadCount specifies threads limit, -1 or 0 means no limit.
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

// Returns the contexr.
func (tr *TaskRunner) GetContext() context.Context {
	return tr.context
}

// Spin up a new go thread to run a task function.
func (tr *TaskRunner) Go(task func() error) {
	tr.eg.Go(task)
}

// Wrapper to errgroup.Wait.
func (tr *TaskRunner) Wait() error {
	return tr.eg.Wait()
}
