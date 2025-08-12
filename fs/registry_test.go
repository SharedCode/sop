package fs

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// No external Redis needed; tests use an in-memory mock client.

var uuid, _ = sop.ParseUUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
var hashMod = MinimumModValue

// Consolidated, table-driven tests for Registry core behavior to reduce duplication.
func TestRegistry_Scenarios(t *testing.T) {
	ctx := context.Background()

	t.Run("add_then_get", func(t *testing.T) {
		l2cache := mocks.NewMockClient()
		base := t.TempDir()
		rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2cache)
		r := NewRegistry(true, hashMod, rt, l2cache)
		defer r.Close()

		h := sop.NewHandle(uuid)
		if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{
			{RegistryTable: "regtest", BlobTable: "regtest", IDs: []sop.Handle{h}},
		}); err != nil {
			t.Fatalf("add: %v", err)
		}
		if res, err := r.Get(ctx, []sop.RegistryPayload[sop.UUID]{{
			RegistryTable: "regtest", BlobTable: "regtest", IDs: []sop.UUID{h.LogicalID},
		}}); err != nil {
			t.Fatalf("get: %v", err)
		} else if res[0].IDs[0].LogicalID != h.LogicalID {
			t.Fatalf("want %v got %v", h.LogicalID, res[0].IDs[0].LogicalID)
		}
	})

	t.Run("update_variants_and_remove", func(t *testing.T) {
		l2 := mocks.NewMockClient()
		base := t.TempDir()
		rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
		r := NewRegistry(true, hashMod, rt, l2)
		defer r.Close()

		h1 := sop.NewHandle(sop.NewUUID())
		h2 := sop.NewHandle(sop.NewUUID())
		if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{
			{RegistryTable: "regx", IDs: []sop.Handle{h1, h2}},
		}); err != nil {
			t.Fatalf("add: %v", err)
		}

		h1.Version = 1
		if err := r.Update(ctx, []sop.RegistryPayload[sop.Handle]{
			{RegistryTable: "regx", IDs: []sop.Handle{h1}},
		}); err != nil {
			t.Fatalf("update: %v", err)
		}

		h2.Version = 2
		if err := r.UpdateNoLocks(ctx, true, []sop.RegistryPayload[sop.Handle]{
			{RegistryTable: "regx", IDs: []sop.Handle{h2}},
		}); err != nil {
			t.Fatalf("update nolocks: %v", err)
		}

		if err := r.Remove(ctx, []sop.RegistryPayload[sop.UUID]{
			{RegistryTable: "regx", IDs: []sop.UUID{h1.LogicalID}},
		}); err != nil {
			t.Fatalf("remove: %v", err)
		}

		if err := r.Replicate(ctx, nil, nil, nil, nil); err != nil {
			t.Fatalf("replicate disabled: %v", err)
		}
	})

	t.Run("replicate_writes_to_passive", func(t *testing.T) {
		l2 := mocks.NewMockClient()
		active := t.TempDir()
		passive := t.TempDir()
		rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
		r := NewRegistry(true, hashMod, rt, l2)
		defer r.Close()

		hNew := sop.NewHandle(sop.NewUUID())
		hAdd := sop.NewHandle(sop.NewUUID())
		hUpd := sop.NewHandle(sop.NewUUID())
		hDel := sop.NewHandle(sop.NewUUID())

		if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{
			{RegistryTable: "regy", IDs: []sop.Handle{hAdd, hUpd, hDel}},
		}); err != nil {
			t.Fatalf("seed add: %v", err)
		}

		if err := r.Replicate(ctx,
			[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "regy", IDs: []sop.Handle{hNew}}},
			[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "regy", IDs: []sop.Handle{hAdd}}},
			[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "regy", IDs: []sop.Handle{hUpd}}},
			nil, // omit removals to avoid deleting item not yet replicated on passive
		); err != nil {
			t.Fatalf("replicate: %v", err)
		}
	})
}
