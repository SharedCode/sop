package common

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// Fake PriorityLog to drive doPriorityRollbacks deterministically.
type fakePriorityLog struct {
	enabled bool
	batch   []sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]]
}

func (f *fakePriorityLog) IsEnabled() bool                                             { return f.enabled }
func (f *fakePriorityLog) Add(ctx context.Context, tid sop.UUID, payload []byte) error { return nil }
func (f *fakePriorityLog) Remove(ctx context.Context, tid sop.UUID) error              { return nil }
func (f *fakePriorityLog) Get(ctx context.Context, tid sop.UUID) ([]sop.RegistryPayload[sop.Handle], error) {
	return nil, nil
}
func (f *fakePriorityLog) GetBatch(ctx context.Context, batchSize int) ([]sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]], error) {
	if len(f.batch) == 0 {
		return nil, nil
	}
	out := f.batch
	f.batch = nil
	return out, nil
}
func (f *fakePriorityLog) LogCommitChanges(ctx context.Context, stores []sop.StoreInfo, newRootNodeHandles, addedNodeHandles, updatedNodeHandles, removedNodeHandles []sop.RegistryPayload[sop.Handle]) error {
	return nil
}
func (f *fakePriorityLog) WriteBackup(ctx context.Context, tid sop.UUID, payload []byte) error {
	return nil
}
func (f *fakePriorityLog) RemoveBackup(ctx context.Context, tid sop.UUID) error { return nil }

// Fake TransactionLog delegating to the mock transaction log but exposing custom PriorityLog.
type fakeTransactionLog struct {
	base sop.TransactionLog
	pl   *fakePriorityLog
}

func (ftl *fakeTransactionLog) GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, []byte], error) {
	return ftl.base.GetOne(ctx)
}
func (ftl *fakeTransactionLog) GetOneOfHour(ctx context.Context, hour string) (sop.UUID, []sop.KeyValuePair[int, []byte], error) {
	return ftl.base.GetOneOfHour(ctx, hour)
}
func (ftl *fakeTransactionLog) GetTIDLogs(tid sop.UUID) []sop.KeyValuePair[int, []byte] { return nil }
func (ftl *fakeTransactionLog) Add(ctx context.Context, tid sop.UUID, commitFunction int, payload []byte) error {
	return ftl.base.Add(ctx, tid, commitFunction, payload)
}
func (ftl *fakeTransactionLog) Remove(ctx context.Context, tid sop.UUID) error {
	return ftl.base.Remove(ctx, tid)
}
func (ftl *fakeTransactionLog) NewUUID() sop.UUID                       { return ftl.base.NewUUID() }
func (ftl *fakeTransactionLog) PriorityLog() sop.TransactionPriorityLog { return ftl.pl }

func Test_Transaction_OnIdle_ProcessesExpired_WhenHourSet(t *testing.T) {
	ctx := context.Background()
	tx := &Transaction{btreesBackend: []btreeBackend{{}}}
	tx.logger = newTransactionLogger(mocks.NewMockTransactionLog(), true)

	prevHour := hourBeingProcessed
	prevLast := lastOnIdleRunTime
	hourBeingProcessed = "2022010112"
	lastOnIdleRunTime = 0
	defer func() { hourBeingProcessed = prevHour; lastOnIdleRunTime = prevLast }()

	tx.onIdle(ctx)
	if hourBeingProcessed != "" {
		t.Fatalf("expected hourBeingProcessed reset to empty, got %q", hourBeingProcessed)
	}
}

func Test_TransactionLogger_DoPriorityRollbacks_FailoverOnVersionMismatch(t *testing.T) {
	ctx := context.Background()
	// Prepare a handle in the priority log with old version
	lid := sop.NewUUID()
	h := sop.NewHandle(lid)
	h.Version = 1
	pr := sop.RegistryPayload[sop.Handle]{IDs: []sop.Handle{h}}

	fpl := &fakePriorityLog{enabled: true, batch: []sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]]{{Key: sop.NewUUID(), Value: []sop.RegistryPayload[sop.Handle]{pr}}}}
	ftl := &fakeTransactionLog{base: mocks.NewMockTransactionLog(), pl: fpl}
	tl := &transactionLog{TransactionLog: ftl, logging: true, transactionID: sop.NewUUID()}

	// Seed registry with progressed version to trigger failover condition
	reg := mocks.NewMockRegistry(false)
	h2 := sop.NewHandle(lid)
	h2.Version = 10
	_ = reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{IDs: []sop.Handle{h2}}})

	// Transaction with l2 cache and registry in place
	tx := &Transaction{l2Cache: mocks.NewMockClient(), registry: reg}

	ok, err := tl.doPriorityRollbacks(ctx, tx)
	if err == nil {
		t.Fatalf("expected failover error, got nil")
	}
	if se, ok2 := err.(sop.Error); !ok2 || se.Code != sop.RestoreRegistryFileSectorFailure {
		t.Fatalf("expected RestoreRegistryFileSectorFailure, got %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false due to early exit, got true")
	}
}
