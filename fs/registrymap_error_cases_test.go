package fs

import (
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// Tests specific error branches in registryMap add/remove operations.
func TestRegistryMapAddAndRemoveErrorBranches(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	rm := newRegistryMap(true, 32, rt, mocks.NewMockClient())

	h := sop.NewHandle(sop.NewUUID())
	if err := rm.add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regerr", IDs: []sop.Handle{h}}}); err != nil {
		t.Fatalf("initial add: %v", err)
	}
	// Duplicate add should error.
	if err := rm.add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regerr", IDs: []sop.Handle{h}}}); err == nil || !strings.Contains(err.Error(), "can't overwrite") {
		t.Fatalf("expected duplicate add error, got %v", err)
	}

	// Remove missing handle should error.
	missing := sop.NewUUID()
	if err := rm.remove(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "regerr", IDs: []sop.UUID{missing}}}); err == nil || !strings.Contains(err.Error(), "was not found, can't delete") {
		t.Fatalf("expected remove missing error, got %v", err)
	}
}
