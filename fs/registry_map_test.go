package fs

import (
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

func TestRegistryMap_BasicOps_TableDriven(t *testing.T) {
	type op string
	const (
		add op = "add"
		set op = "set"
		fetch op = "fetch"
		remove op = "remove"
	)

	cases := []struct{
		name   string
		ops    []op
		verify func(t *testing.T, r *registryMap)
	}{
		{name: "add", ops: []op{add}},
		{name: "set", ops: []op{set}},
		{name: "add_fetch", ops: []op{add, fetch}},
		{name: "add_remove", ops: []op{add, remove}},
		{name: "add_remove_set", ops: []op{add, remove, set}},
		{name: "cycle_add_remove_add", ops: []op{add, remove, add}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l2cache := mocks.NewMockClient()
			base := t.TempDir()
			rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2cache)
			r := newRegistryMap(true, hashMod, rt, l2cache)
			defer r.close()

			h := sop.NewHandle(uuid)

			for _, o := range tc.ops {
				switch o {
				case add:
					if err := r.add(ctx, []sop.RegistryPayload[sop.Handle]{{
						RegistryTable: "regtest",
						IDs:           []sop.Handle{h},
					}}); err != nil { t.Fatalf("add: %v", err) }
				case set:
					if err := r.set(ctx, []sop.RegistryPayload[sop.Handle]{{
						RegistryTable: "regtest",
						IDs:           []sop.Handle{h},
					}}); err != nil { t.Fatalf("set: %v", err) }
				case fetch:
					if res, err := r.fetch(ctx, []sop.RegistryPayload[sop.UUID]{{
						RegistryTable: "regtest",
						IDs:           []sop.UUID{uuid},
					}}); err != nil {
						t.Fatalf("fetch: %v", err)
					} else if len(res) == 0 || len(res[0].IDs) == 0 {
						t.Fatalf("expected non-empty result, got: %+v", res)
					}
				case remove:
					if err := r.remove(ctx, []sop.RegistryPayload[sop.UUID]{{
						RegistryTable: "regtest",
						IDs:           []sop.UUID{uuid},
					}}); err != nil { t.Fatalf("remove: %v", err) }
				}
			}
			if tc.verify != nil { tc.verify(t, r) }
		})
	}
}
