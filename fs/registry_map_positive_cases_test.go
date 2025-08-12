package fs

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestRegistryMap_PositiveMultiItem exercises multi-item add/update/get/remove paths to cover
// looping logic in registryMap.{add,set,fetch,remove} and registry.Close.
func TestRegistryMap_PositiveMultiItem(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
	r := NewRegistry(true, MinimumModValue, rt, l2)
	defer r.Close()

	// Prepare multiple handles across two payloads to drive inner loops.
	h1 := sop.NewHandle(sop.NewUUID())
	h2 := sop.NewHandle(sop.NewUUID())
	h3 := sop.NewHandle(sop.NewUUID())
	h4 := sop.NewHandle(sop.NewUUID())

	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{
		{RegistryTable: "regmulti", IDs: []sop.Handle{h1, h2}},
		{RegistryTable: "regmulti", IDs: []sop.Handle{h3, h4}},
	}); err != nil {
		t.Fatalf("multi add: %v", err)
	}

	// Update versions simultaneously to exercise set loop over multiple frds.
	h1.Version, h2.Version, h3.Version, h4.Version = 1, 2, 3, 4
	if err := r.Update(ctx, []sop.RegistryPayload[sop.Handle]{
		{RegistryTable: "regmulti", IDs: []sop.Handle{h1, h2, h3, h4}},
	}); err != nil {
		t.Fatalf("multi update: %v", err)
	}

	// Fetch all individually to exercise fetch path multiple times.
	res, err := r.Get(ctx, []sop.RegistryPayload[sop.UUID]{
		{RegistryTable: "regmulti", IDs: []sop.UUID{h1.LogicalID, h2.LogicalID, h3.LogicalID, h4.LogicalID}},
	})
	if err != nil || len(res) != 1 || len(res[0].IDs) != 4 {
		t.Fatalf("get multi unexpected: %v %+v", err, res)
	}
	// Basic sanity on versions.
	wantVersions := []int32{1, 2, 3, 4}
	for i, hv := range wantVersions {
		if res[0].IDs[i].Version != hv {
			t.Fatalf("version mismatch idx=%d got=%d want=%d", i, res[0].IDs[i].Version, hv)
		}
	}

	// Remove two handles in one call to exercise multi remove loop.
	if err := r.Remove(ctx, []sop.RegistryPayload[sop.UUID]{
		{RegistryTable: "regmulti", IDs: []sop.UUID{h1.LogicalID, h3.LogicalID}},
	}); err != nil {
		t.Fatalf("multi remove: %v", err)
	}
}
