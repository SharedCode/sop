package fs

import (
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

func TestRegistryMapAdd(t *testing.T) {
	l2cache := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2cache)
	r := newRegistryMap(true, hashMod, rt, l2cache)

	h := sop.NewHandle(uuid)

	if err := r.add(ctx, []sop.RegistryPayload[sop.Handle]{{
		RegistryTable: "regtest",
		IDs:           []sop.Handle{h},
	}}); err != nil {
		t.Error(err.Error())
	}

	r.close()
}

func TestRegistryMapSet(t *testing.T) {
	l2cache := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2cache)
	r := newRegistryMap(true, hashMod, rt, l2cache)

	h := sop.NewHandle(uuid)

	if err := r.set(ctx, []sop.RegistryPayload[sop.Handle]{{
		RegistryTable: "regtest",
		IDs:           []sop.Handle{h},
	}}); err != nil {
		t.Error(err.Error())
	}

	r.close()
}

func TestRegistryMapGet(t *testing.T) {
	l2cache := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2cache)
	r := newRegistryMap(true, hashMod, rt, l2cache)

	// Write a handle first so fetch has something to return.
	h := sop.NewHandle(uuid)
	if err := r.add(ctx, []sop.RegistryPayload[sop.Handle]{
		{
			RegistryTable: "regtest",
			IDs:           []sop.Handle{h},
		},
	}); err != nil {
		t.Fatalf("add failed: %v", err)
	}

	if res, err := r.fetch(ctx, []sop.RegistryPayload[sop.UUID]{{
		RegistryTable: "regtest",
		IDs:           []sop.UUID{uuid},
	}}); err != nil {
		t.Error(err.Error())
	} else {
		if len(res) == 0 || len(res[0].IDs) == 0 {
			t.Fatalf("expected non-empty result, got: %+v", res)
		}
		if res[0].RegistryTable != "regtest" || res[0].IDs[0].LogicalID != uuid {
			t.Errorf("Expected: First='regtest', Second='%v', got: First: %s, Second=%v", uuid, res[0].RegistryTable, res[0].IDs[0].LogicalID)
		}
	}

	r.close()
}

func TestRegistryMapRemove(t *testing.T) {
	l2cache := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2cache)
	r := newRegistryMap(true, hashMod, rt, l2cache)

	// Add first so remove has a record to delete.
	h := sop.NewHandle(uuid)
	if err := r.add(ctx, []sop.RegistryPayload[sop.Handle]{
		{
			RegistryTable: "regtest",
			IDs:           []sop.Handle{h},
		},
	}); err != nil {
		t.Fatalf("add failed: %v", err)
	}

	if err := r.remove(ctx, []sop.RegistryPayload[sop.UUID]{{
		RegistryTable: "regtest",
		IDs:           []sop.UUID{uuid},
	}}); err != nil {
		t.Error(err.Error())
	}

	r.close()
}

func TestRegistryMapSetAfterRemove(t *testing.T) {
	l2cache := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2cache)
	r := newRegistryMap(true, hashMod, rt, l2cache)

	h := sop.NewHandle(uuid)

	if err := r.add(ctx, []sop.RegistryPayload[sop.Handle]{{
		RegistryTable: "regtest",
		IDs:           []sop.Handle{h},
	}}); err != nil {
		t.Error(err.Error())
	}

	if err := r.remove(ctx, []sop.RegistryPayload[sop.UUID]{{
		RegistryTable: "regtest",
		IDs:           []sop.UUID{uuid},
	}}); err != nil {
		t.Error(err.Error())
	}

	if err := r.set(ctx, []sop.RegistryPayload[sop.Handle]{
		{
			RegistryTable: "regtest",
			IDs:           []sop.Handle{h},
		},
	}); err != nil {
		t.Errorf("r.set failed after remove, expected success: %v", err)
	}

	r.close()
}

func TestRegistryMapRecyAddRemoveAdd(t *testing.T) {
	l2cache := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2cache)
	r := newRegistryMap(true, hashMod, rt, l2cache)

	h := sop.NewHandle(uuid)

	if err := r.add(ctx, []sop.RegistryPayload[sop.Handle]{{
		RegistryTable: "regtest",
		IDs:           []sop.Handle{h},
	}}); err != nil {
		t.Error(err.Error())
	}

	if err := r.remove(ctx, []sop.RegistryPayload[sop.UUID]{{
		RegistryTable: "regtest",
		IDs:           []sop.UUID{uuid},
	}}); err != nil {
		t.Error(err.Error())
	}

	if err := r.add(ctx, []sop.RegistryPayload[sop.Handle]{{
		RegistryTable: "regtest",
		IDs:           []sop.Handle{h},
	}}); err != nil {
		t.Error(err.Error())
	}

	r.close()
}
