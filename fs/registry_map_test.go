package fs

import (
	"testing"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/redis"
)

func TestRegistryMapAdd(t *testing.T) {
	rt, _ := NewReplicationTracker([]string{"/Users/grecinto/sop_data/"}, false)
	r := newRegistryMap(true, hashMod, rt, redis.NewClient())

	h := sop.NewHandle(uuid)

	if err := r.add(ctx, sop.Tuple[string, []sop.Handle]{
		First:  "regtest",
		Second: []sop.Handle{h},
	}); err != nil {
		t.Error(err.Error())
	}

	r.close()
}

func TestRegistryMapSet(t *testing.T) {
	rt, _ := NewReplicationTracker([]string{"/Users/grecinto/sop_data/"}, false)
	r := newRegistryMap(true, hashMod, rt, redis.NewClient())

	h := sop.NewHandle(uuid)

	if err := r.set(ctx, sop.Tuple[string, []sop.Handle]{
		First:  "regtest",
		Second: []sop.Handle{h},
	}); err != nil {
		t.Error(err.Error())
	}

	r.close()
}

func TestRegistryMapGet(t *testing.T) {
	rt, _ := NewReplicationTracker([]string{"/Users/grecinto/sop_data/"}, false)
	r := newRegistryMap(true, hashMod, rt, redis.NewClient())

	if res, err := r.fetch(ctx, sop.Tuple[string, []sop.UUID]{
		First:  "regtest",
		Second: []sop.UUID{uuid},
	}); err != nil {
		t.Error(err.Error())
	} else {
		if res[0].First != "regtest" || res[0].Second[0].LogicalID != uuid {
			t.Errorf("Expected: First='regtest', Second='%v', got: First: %s, Second=%v", uuid, res[0].First, res[0].Second[0].LogicalID)
		}
	}

	r.close()
}

func TestRegistryMapRemove(t *testing.T) {
	rt, _ := NewReplicationTracker([]string{"/Users/grecinto/sop_data/"}, false)
	r := newRegistryMap(true, hashMod, rt, redis.NewClient())

	if err := r.remove(ctx, sop.Tuple[string, []sop.UUID]{
		First:  "regtest",
		Second: []sop.UUID{uuid},
	}); err != nil {
		t.Error(err.Error())
	}

	r.close()
}

func TestRegistryMapFailedSet(t *testing.T) {
	rt, _ := NewReplicationTracker([]string{"/Users/grecinto/sop_data/"}, false)
	r := newRegistryMap(true, hashMod, rt, redis.NewClient())

	h := sop.NewHandle(uuid)

	if err := r.add(ctx, sop.Tuple[string, []sop.Handle]{
		First:  "regtest",
		Second: []sop.Handle{h},
	}); err != nil {
		t.Error(err.Error())
	}

	if err := r.remove(ctx, sop.Tuple[string, []sop.UUID]{
		First:  "regtest",
		Second: []sop.UUID{uuid},
	}); err != nil {
		t.Error(err.Error())
	}

	if err := r.set(ctx, sop.Tuple[string, []sop.Handle]{
		First:  "regtest",
		Second: []sop.Handle{h},
	}); err == nil {
		t.Errorf("r.set succeeded, expected to fail")
	}

	r.close()
}

func TestRegistryMapRecyAddRemoveAdd(t *testing.T) {
	rt, _ := NewReplicationTracker([]string{"/Users/grecinto/sop_data/"}, false)
	r := newRegistryMap(true, hashMod, rt, redis.NewClient())

	h := sop.NewHandle(uuid)

	if err := r.add(ctx, sop.Tuple[string, []sop.Handle]{
		First:  "regtest",
		Second: []sop.Handle{h},
	}); err != nil {
		t.Error(err.Error())
	}

	if err := r.remove(ctx, sop.Tuple[string, []sop.UUID]{
		First:  "regtest",
		Second: []sop.UUID{uuid},
	}); err != nil {
		t.Error(err.Error())
	}

	if err := r.add(ctx, sop.Tuple[string, []sop.Handle]{
		First:  "regtest",
		Second: []sop.Handle{h},
	}); err != nil {
		t.Error(err.Error())
	}

	r.close()
}
