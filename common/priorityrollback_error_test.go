package common

import (
	"context"
	"fmt"
	"testing"

	"github.com/sharedcode/sop"
)

// errRegistry is a stub sop.Registry that forces UpdateNoLocks to return an error.
type errRegistry struct{}

func (e errRegistry) Add(ctx context.Context, storesHandles []sop.RegistryPayload[sop.Handle]) error {
	return nil
}
func (e errRegistry) Update(ctx context.Context, storesHandles []sop.RegistryPayload[sop.Handle]) error {
	return nil
}
func (e errRegistry) UpdateNoLocks(ctx context.Context, allOrNothing bool, storesHandles []sop.RegistryPayload[sop.Handle]) error {
	return fmt.Errorf("forced error")
}
func (e errRegistry) Get(ctx context.Context, storesLids []sop.RegistryPayload[sop.UUID]) ([]sop.RegistryPayload[sop.Handle], error) {
	return nil, nil
}
func (e errRegistry) Remove(ctx context.Context, storesLids []sop.RegistryPayload[sop.UUID]) error {
	return nil
}
func (e errRegistry) Replicate(ctx context.Context, newRootNodeHandles, addedNodeHandles, updatedNodeHandles, removedNodeHandles []sop.RegistryPayload[sop.Handle]) error {
	return nil
}

// Ensures the error branch in priorityRollback returns a sop.Error when UpdateNoLocks fails.
func Test_TransactionLogger_PriorityRollback_ErrorBranch(t *testing.T) {
	ctx := context.Background()
	// Registry that always errors on UpdateNoLocks
	tx := &Transaction{registry: errRegistry{}}
	// Priority log returns a payload for the same tid
	tid := sop.NewUUID()
	pl := &stubPriorityLog{batch: []sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]]{{Key: tid, Value: []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rt", IDs: []sop.Handle{sop.NewHandle(sop.NewUUID())}}}}}}
	tl := newTransactionLogger(stubTLog{pl: pl}, true)

	err := tl.priorityRollback(ctx, tx, tid)
	if err == nil {
		t.Fatalf("expected error from priorityRollback")
	}
	// Expect a wrapped sop.Error carrying failover code
	if se, ok := err.(sop.Error); !ok || se.Code != sop.RestoreRegistryFileSectorFailure {
		t.Fatalf("expected sop.RestoreRegistryFileSectorFailure, got %v", err)
	}
}
