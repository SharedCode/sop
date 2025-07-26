package fs

import (
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/redis"
)

func TestRegistryMapAdd(t *testing.T) {
	l2cache := redis.NewClient()
	rt, _ := NewReplicationTracker(ctx, []string{"/Users/grecinto/sop_data/"}, false, l2cache)
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
	l2cache := redis.NewClient()
	rt, _ := NewReplicationTracker(ctx, []string{"/Users/grecinto/sop_data/"}, false, l2cache)
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
	l2cache := redis.NewClient()
	rt, _ := NewReplicationTracker(ctx, []string{"/Users/grecinto/sop_data/"}, false, l2cache)
	r := newRegistryMap(true, hashMod, rt, redis.NewClient())

	if res, err := r.fetch(ctx, []sop.RegistryPayload[sop.UUID]{{
		RegistryTable: "regtest",
		IDs:           []sop.UUID{uuid},
	}}); err != nil {
		t.Error(err.Error())
	} else {
		if res[0].RegistryTable != "regtest" || res[0].IDs[0].LogicalID != uuid {
			t.Errorf("Expected: First='regtest', Second='%v', got: First: %s, Second=%v", uuid, res[0].RegistryTable, res[0].IDs[0].LogicalID)
		}
	}

	r.close()
}

func TestRegistryMapRemove(t *testing.T) {
	l2cache := redis.NewClient()
	rt, _ := NewReplicationTracker(ctx, []string{"/Users/grecinto/sop_data/"}, false, l2cache)
	r := newRegistryMap(true, hashMod, rt, redis.NewClient())

	if err := r.remove(ctx, []sop.RegistryPayload[sop.UUID]{{
		RegistryTable: "regtest",
		IDs:           []sop.UUID{uuid},
	}}); err != nil {
		t.Error(err.Error())
	}

	r.close()
}

func TestRegistryMapFailedSet(t *testing.T) {
	l2cache := redis.NewClient()
	rt, _ := NewReplicationTracker(ctx, []string{"/Users/grecinto/sop_data/"}, false, l2cache)
	r := newRegistryMap(true, hashMod, rt, redis.NewClient())

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

	if err := r.set(ctx, []sop.RegistryPayload[sop.Handle]{{
		RegistryTable: "regtest",
		IDs:           []sop.Handle{h},
	}}); err == nil {
		t.Errorf("r.set succeeded, expected to fail")
	}

	r.close()
}

func TestRegistryMapRecyAddRemoveAdd(t *testing.T) {
	l2cache := redis.NewClient()
	rt, _ := NewReplicationTracker(ctx, []string{"/Users/grecinto/sop_data/"}, false, l2cache)
	r := newRegistryMap(true, hashMod, rt, redis.NewClient())

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
