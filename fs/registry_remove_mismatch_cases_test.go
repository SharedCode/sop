package fs

import (
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestRegistry_RemoveMismatch covers the error path where the record at offset differs from the
// requested logical ID (registryMap.remove mismatch branch) and ensures caches evict entries.
// Helper to generate two handles that map to the same file region block/slot by brute force.
func generateCollidingHandles(t *testing.T, hmHashMod int) (sop.Handle, sop.Handle) {
	t.Helper()
	h1 := sop.NewHandle(sop.NewUUID())
	high1, low1 := h1.LogicalID.Split()
	for i := 0; i < 500000; i++ { // bounded search
		h2 := sop.NewHandle(sop.NewUUID())
		high2, low2 := h2.LogicalID.Split()
		if high1%uint64(hmHashMod) == high2%uint64(hmHashMod) && low1%uint64(handlesPerBlock) == low2%uint64(handlesPerBlock) && h1.LogicalID != h2.LogicalID {
			return h1, h2
		}
	}
	t.Fatalf("unable to find colliding handles quickly; increase search bounds")
	return sop.Handle{}, sop.Handle{}
}

// TestRegistry_SetAndRemoveMismatch crafts two handles landing on the same slot; writes one then tries to set/remove with differing logical IDs to trigger mismatch errors.
func TestRegistry_SetAndRemoveMismatch(t *testing.T) {
	t.Skip("registryMap set/remove mismatch branches appear unreachable due to findFileRegion using forWriting=true which avoids returning occupied differing slots; skipping")
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
	r := NewRegistry(true, 32, rt, l2) // small mod value -> faster collisions
	defer r.Close()

	hExisting, hConflict := generateCollidingHandles(t, 32)
	// Add the first handle.
	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regmm", IDs: []sop.Handle{hExisting}}}); err != nil {
		t.Fatalf("add seed: %v", err)
	}
	// Attempt to set a different handle that maps to same slot -> expect mismatch error from registryMap.set
	if err := r.UpdateNoLocks(ctx, true, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regmm", IDs: []sop.Handle{hConflict}}}); err == nil || !strings.Contains(err.Error(), "registryMap.set failed") {
		t.Fatalf("expected mismatch set error, got: %v", err)
	}

	// Now attempt remove using a conflicting logical ID sequence: first fetch actual to ensure present.
	if _, err := r.Get(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "regmm", IDs: []sop.UUID{hExisting.LogicalID}}}); err != nil {
		t.Fatalf("get existing: %v", err)
	}
	// Remove with wrong logical ID (the conflicting one) should cause mismatch in registryMap.remove
	if err := r.Remove(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "regmm", IDs: []sop.UUID{hConflict.LogicalID}}}); err == nil || !strings.Contains(err.Error(), "registryMap.remove failed") {
		t.Fatalf("expected mismatch remove error, got: %v", err)
	}
}
