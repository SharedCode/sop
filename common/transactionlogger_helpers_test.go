package common

import (
	"context"
	"reflect"
	"testing"

	"github.com/sharedcode/sop"
)

// tlRecorder is a minimal TransactionLog test double that captures Add/Remove calls
// and returns a stable UUID from NewUUID so we can assert removeLogs uses it.
type tlRecorder struct {
	added   []sop.KeyValuePair[int, []byte]
	removed []sop.UUID
	tid     sop.UUID
}

// noOpPrioLog is a no-op implementation of sop.TransactionPriorityLog for unit tests.
type noOpPrioLog struct{}

func (noOpPrioLog) IsEnabled() bool                             { return false }
func (noOpPrioLog) Add(context.Context, sop.UUID, []byte) error { return nil }
func (noOpPrioLog) Remove(context.Context, sop.UUID) error      { return nil }
func (noOpPrioLog) Get(context.Context, sop.UUID) ([]sop.RegistryPayload[sop.Handle], error) {
	return nil, nil
}
func (noOpPrioLog) GetBatch(context.Context, int) ([]sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]], error) {
	return nil, nil
}
func (noOpPrioLog) LogCommitChanges(context.Context, []sop.StoreInfo, []sop.RegistryPayload[sop.Handle], []sop.RegistryPayload[sop.Handle], []sop.RegistryPayload[sop.Handle], []sop.RegistryPayload[sop.Handle]) error {
	return nil
}
func (noOpPrioLog) WriteBackup(context.Context, sop.UUID, []byte) error { return nil }
func (noOpPrioLog) RemoveBackup(context.Context, sop.UUID) error        { return nil }

func (t *tlRecorder) PriorityLog() sop.TransactionPriorityLog { return noOpPrioLog{} }
func (t *tlRecorder) Add(ctx context.Context, tid sop.UUID, commitFunction int, payload []byte) error {
	t.added = append(t.added, sop.KeyValuePair[int, []byte]{Key: commitFunction, Value: payload})
	return nil
}
func (t *tlRecorder) Remove(ctx context.Context, tid sop.UUID) error {
	t.removed = append(t.removed, tid)
	return nil
}
func (t *tlRecorder) GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, []byte], error) {
	return sop.NilUUID, "", nil, nil
}
func (t *tlRecorder) GetOneOfHour(ctx context.Context, hour string) (sop.UUID, []sop.KeyValuePair[int, []byte], error) {
	return sop.NilUUID, nil, nil
}
func (t *tlRecorder) NewUUID() sop.UUID { return t.tid }

func Test_TransactionLogger_Log_And_RemoveLogs(t *testing.T) {
	ctx := context.Background()
	fixedTID := sop.NewUUID()
	rec := &tlRecorder{tid: fixedTID}

	// logging disabled -> no Add, but committedState should record last function
	tl := newTransactionLogger(rec, false)
	if err := tl.log(ctx, commitAddedNodes, []byte{1, 2, 3}); err != nil {
		t.Fatalf("log returned error: %v", err)
	}
	if tl.committedState != commitAddedNodes {
		t.Fatalf("committedState not set, got %v", tl.committedState)
	}
	if len(rec.added) != 0 {
		t.Fatalf("expected no Add calls when logging disabled; got %d", len(rec.added))
	}

	// logging enabled -> Add should be recorded and removeLogs should remove by the fixed TID
	tl2 := newTransactionLogger(rec, true)
	if err := tl2.log(ctx, commitUpdatedNodes, []byte{9}); err != nil {
		t.Fatalf("log (enabled) error: %v", err)
	}
	if len(rec.added) == 0 || rec.added[len(rec.added)-1].Key != int(commitUpdatedNodes) {
		t.Fatalf("expected Add of commitUpdatedNodes recorded")
	}
	if err := tl2.removeLogs(ctx); err != nil {
		t.Fatalf("removeLogs error: %v", err)
	}
	if len(rec.removed) == 0 || rec.removed[len(rec.removed)-1] != fixedTID {
		t.Fatalf("expected Remove called with fixed tid %s, got %v", fixedTID.String(), rec.removed)
	}
}

func Test_ToStruct_ToByteArray_RoundTrip_And_Nil(t *testing.T) {
	// nil -> zero value
	var z sop.Tuple[int, string]
	got := toStruct[sop.Tuple[int, string]](nil)
	if !reflect.DeepEqual(got, z) {
		t.Fatalf("expected zero value on nil input; got %+v", got)
	}

	// roundtrip a composite payload used elsewhere in code paths
	original := sop.Tuple[sop.Tuple[int, string], []sop.UUID]{
		First:  sop.Tuple[int, string]{First: 42, Second: "hello"},
		Second: []sop.UUID{sop.NewUUID(), sop.NewUUID()},
	}
	ba := toByteArray(original)
	round := toStruct[sop.Tuple[sop.Tuple[int, string], []sop.UUID]](ba)
	if !reflect.DeepEqual(original, round) {
		t.Fatalf("roundtrip mismatch: want %+v; got %+v", original, round)
	}
}
