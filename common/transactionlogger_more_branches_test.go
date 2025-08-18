package common

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// Covers acquireLocks branch where a different owner holds a lock, returning a failover error.
func Test_TransactionLogger_AcquireLocks_PartialLockByOther_ReturnsFailoverError(t *testing.T) {
	ctx := context.Background()
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	l2 := mocks.NewMockClient()
	tx := &Transaction{l2Cache: l2}
	// Build two logical IDs and seed one lock as owned by another transaction.
	id1, id2 := sop.NewUUID(), sop.NewUUID()
	owner := sop.NewUUID()
	// Seed the mock cache so Lock() sees a conflicting owner on id1.
	key := l2.FormatLockKey(id1.String())
	_ = l2.Set(ctx, key, owner.String(), time.Minute)

	// Compose registry payload with these logical IDs.
	uhAndrh := []sop.RegistryPayload[sop.Handle]{{IDs: []sop.Handle{sop.NewHandle(id1), sop.NewHandle(id2)}}}
	tid := sop.NewUUID()
	_, err := tl.acquireLocks(ctx, tx, tid, uhAndrh)
	if err == nil {
		t.Fatalf("expected error when another owner holds a lock")
	}
	if se, ok := err.(sop.Error); !ok || se.Code != sop.RestoreRegistryFileSectorFailure {
		t.Fatalf("expected RestoreRegistryFileSectorFailure, got %v", err)
	}
}
