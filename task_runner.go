package sop

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type TaskRunner struct {
	maxThreadCount int
	eg             *errgroup.Group
	limiterChan    chan bool
	context        context.Context
}

func NewTaskRunner(ctx context.Context, maxThreadCount int) *TaskRunner {
	eg, ctx2 := errgroup.WithContext(ctx)
	return &TaskRunner{
		maxThreadCount: maxThreadCount,
		limiterChan:    make(chan bool, maxThreadCount),
		eg:             eg,
		context:        ctx2,
	}
}

func (tr *TaskRunner) GetContext() context.Context {
	return tr.context
}

func (tr *TaskRunner) Go(task func() error) {
	t := func() error {
		err := task()
		if err != nil {
			return err
		}
		// Free up this thread slot.
		<-tr.limiterChan
		return nil
	}
	tr.eg.Go(t)
	// Occupy a thread slot.
	tr.limiterChan <- true
}

// Wrapper to errgroup.Wait.
func (tr *TaskRunner) Wait() error {
	defer close(tr.limiterChan)
	return tr.eg.Wait()
}
