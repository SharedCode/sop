package common

// Extra consolidated scenarios for transaction:
// - onIdle runs without backends and with dummy priority log
// - Close invokes io.Closer when registry provides it
// - Phase2Commit early paths for reader/no-check modes
// - handleRegistrySectorLockTimeout success/fail branches

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// dummyCloserRegistry augments Mock_registry with io.Closer to exercise Close()
// Implemented via embedding and adding Close method at use site in test using type aliasing is not
// feasible across packages; instead, we wrap but do not actually assert io.Closer runtime type.
// We simply invoke Close and ensure no panic occurs through type assertion branch in Close().

type dummyCloserRegistry struct{ sop.Registry }

func (d dummyCloserRegistry) Close() error { return nil }

func Test_Transaction_OnIdle_NoBackends_NoPanics(t *testing.T) {
	tx := &Transaction{}
	tx.onIdle(context.Background())
}

func Test_Transaction_Close_InvokesCloser(t *testing.T) {
	tx := &Transaction{registry: dummyCloserRegistry{mocks.NewMockRegistry(false)}}
	if err := tx.Close(); err != nil {
		t.Fatalf("Close unexpected err: %v", err)
	}
}

func Test_Transaction_Phase2Commit_ReaderAndNoCheck_EarlyReturn(t *testing.T) {
	ctx := context.Background()
	tx := &Transaction{mode: sop.ForReading}
	tx.phaseDone = 1
	if err := tx.Phase2Commit(ctx); err != nil {
		t.Fatalf("reader Phase2Commit should early-return nil, got %v", err)
	}
	tx2 := &Transaction{mode: sop.NoCheck}
	tx2.phaseDone = 1
	if err := tx2.Phase2Commit(ctx); err != nil {
		t.Fatalf("no-check Phase2Commit should early-return nil, got %v", err)
	}
}

func Test_Transaction_handleRegistrySectorLockTimeout_Scenarios(t *testing.T) {
	ctx := context.Background()
	mc := mocks.NewMockClient()
	tx := &Transaction{l2Cache: mc, logger: newTransactionLogger(mocks.NewMockTransactionLog(), true)}

	// Case 1: with sop.Error but missing UserData *sop.LockKey -> returns original
	se := sop.Error{Err: errors.New("y"), UserData: "not a lock key"}
	if err := tx.handleRegistrySectorLockTimeout(ctx, se); err == nil {
		t.Fatalf("expected original error returned")
	}

	// Case 2: valid *sop.LockKey and priorityRollback returns nil
	lk := &sop.LockKey{Key: "k", LockID: sop.NewUUID()}
	se2 := sop.Error{Err: errors.New("z"), UserData: lk}
	if err := tx.handleRegistrySectorLockTimeout(ctx, se2); err != nil {
		t.Fatalf("expected nil after successful priority rollback path; got %v", err)
	}
}

func Test_Transaction_onIdle_DoesNotPanic_WithBackendAndDisabledPriorityLog(t *testing.T) {
	ctx := context.Background()
	tx := &Transaction{l2Cache: mocks.NewMockClient(), logger: newTransactionLogger(mocks.NewMockTransactionLog(), true)}
	// Prepare a backend slice to pass early len>0 check
	tx.btreesBackend = []btreeBackend{{}}
	// Run twice to cover both intervals logic without actual sleeping
	tx.onIdle(ctx)
	// Force second branch inside onIdle (cleanup interval)
	lastOnIdleRunTime = sop.Now().Add(time.Duration(-10) * time.Minute).UnixMilli()
	tx.onIdle(ctx)
}
