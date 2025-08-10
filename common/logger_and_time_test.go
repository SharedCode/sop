package common

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	cas "github.com/sharedcode/sop/cassandra"
	"github.com/sharedcode/sop/common/mocks"
)

// Ensures processExpiredTransactionLogs processes an older-than-1h log and calls rollback.
func Test_TransactionLogger_ProcessExpired_WithEntry(t *testing.T) {
	ctx := context.Background()
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	// Create a log from yesterday so mock GetOne returns it.
	prevCasNow := cas.Now
	defer func() { cas.Now = prevCasNow }()
	cas.Now = func() time.Time { return time.Now().Add(-24 * time.Hour) }

	tid := tl.NewUUID()
	if err := tl.Add(ctx, tid, int(finalizeCommit), nil); err != nil {
		t.Fatalf("Add log err: %v", err)
	}

	// Restore time to current so process can advance hourBeingProcessed
	cas.Now = prevCasNow
	tx := &Transaction{}
	if err := tl.processExpiredTransactionLogs(ctx, tx); err != nil {
		t.Fatalf("processExpiredTransactionLogs err: %v", err)
	}
}

// Covers handleRegistrySectorLockTimeout passthrough when error doesn't carry a *LockKey.
func Test_Transaction_HandleRegistrySectorLockTimeout_Passthrough(t *testing.T) {
	ctx := context.Background()
	tx := &Transaction{l2Cache: mocks.NewMockClient(), logger: newTransactionLogger(mocks.NewMockTransactionLog(), true)}
	// Error with wrong user data type should be returned as-is.
	in := sop.Error{Code: sop.RestoreRegistryFileSectorFailure, UserData: 123}
	if out := tx.handleRegistrySectorLockTimeout(ctx, in); out == nil {
		t.Fatalf("expected same error back, got nil")
	}
}

// Covers Transaction.timedOut helper.
func Test_Transaction_TimedOut(t *testing.T) {
	ctx := context.Background()
	tx := &Transaction{maxTime: 1 * time.Millisecond}
	start := sop.Now().Add(-2 * time.Millisecond)
	if err := tx.timedOut(ctx, start); err == nil {
		t.Fatalf("expected timeout error")
	}
}
