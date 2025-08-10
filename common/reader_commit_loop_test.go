package common

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
)

// seqRegistry toggles version returned by Get on successive calls to simulate a
// concurrent update that is resolved after a refetch cycle.
type seqRegistry struct {
	versions map[sop.UUID][]int32 // [initial, final]
	calls    int
}

func (s *seqRegistry) Add(ctx context.Context, storesHandles []sop.RegistryPayload[sop.Handle]) error {
	return nil
}
func (s *seqRegistry) Update(ctx context.Context, storesHandles []sop.RegistryPayload[sop.Handle]) error {
	return nil
}
func (s *seqRegistry) UpdateNoLocks(ctx context.Context, allOrNothing bool, storesHandles []sop.RegistryPayload[sop.Handle]) error {
	return nil
}
func (s *seqRegistry) Get(ctx context.Context, storesLids []sop.RegistryPayload[sop.UUID]) ([]sop.RegistryPayload[sop.Handle], error) {
	s.calls++
	out := make([]sop.RegistryPayload[sop.Handle], len(storesLids))
	for i := range storesLids {
		out[i].RegistryTable = storesLids[i].RegistryTable
		out[i].IDs = make([]sop.Handle, len(storesLids[i].IDs))
		for ii := range storesLids[i].IDs {
			lid := storesLids[i].IDs[ii]
			h := sop.NewHandle(lid)
			v := s.versions[lid]
			if len(v) == 0 {
				out[i].IDs[ii] = h
				continue
			}
			if s.calls == 1 {
				h.Version = v[0]
			} else {
				h.Version = v[1]
			}
			out[i].IDs[ii] = h
		}
	}
	return out, nil
}
func (s *seqRegistry) Remove(ctx context.Context, storesLids []sop.RegistryPayload[sop.UUID]) error {
	return nil
}
func (s *seqRegistry) Replicate(ctx context.Context, a, b, c, d []sop.RegistryPayload[sop.Handle]) error {
	return nil
}

func Test_ReaderCommit_RefetchLoop_Converges(t *testing.T) {
	ctx := context.Background()
	name := "reader_loop"
	pk, p := newPerson("r", "loop", "x", "e", "p")
	// Seed committed store with one record.
	seedStoreWithOne(t, name, true, pk, p)

	// Reader transaction fetches the item to track it.
	trans, err := newMockTransaction(t, sop.ForReading, -1)
	if err != nil {
		t.Fatal(err)
	}
	if err := trans.Begin(); err != nil {
		t.Fatal(err)
	}
	b3, err := OpenBtree[PersonKey, Person](ctx, name, trans, Compare)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := b3.Find(ctx, pk, false)
	if !ok || err != nil {
		t.Fatalf("Find failed: ok=%v err=%v", ok, err)
	}
	it, err := b3.GetCurrentItem(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Swap in a registry that first reports a mismatching version then a matching one.
	t2 := trans.GetPhasedTransaction().(*Transaction)
	sr := &seqRegistry{versions: map[sop.UUID][]int32{it.ID: {it.Version + 1, it.Version}}}
	t2.registry = sr

	if err := t2.commitForReaderTransaction(ctx); err != nil {
		t.Fatalf("commitForReaderTransaction error: %v", err)
	}
}
