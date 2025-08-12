package fs

import (
	"context"
	"os"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestRegistry_ReplicateErrorBranches forces errors in each replicate phase by pointing passive folder to a file.
func TestRegistry_ReplicateErrorBranches(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	active := t.TempDir()
	passive := t.TempDir()
	// Replace passive folder with a file to induce errors (add/set/remove) when rm.add/set/remove attempt directory ops.
	badPassiveFile := passive + string(os.PathSeparator) + "passive_as_file"
	if err := os.WriteFile(badPassiveFile, []byte("x"), 0o600); err != nil {
		t.Fatalf("setup file: %v", err)
	}

	rt, _ := NewReplicationTracker(ctx, []string{active, badPassiveFile}, true, l2)
	r := NewRegistry(true, MinimumModValue, rt, l2)
	defer r.Close()

	// Seed active with an item so update/remove have something.
	hAdd := sop.NewHandle(sop.NewUUID())
	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{hAdd}}}); err != nil {
		t.Fatalf("seed add: %v", err)
	}

	// Prepare payloads for replicate call covering all non-nil slices.
	newRoot := sop.NewHandle(sop.NewUUID())
	upd := hAdd
	upd.Version = 2
	del := sop.NewHandle(sop.NewUUID()) // create then attempt remove replicate path

	// Add del so removal slice has valid original.
	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{del}}}); err != nil {
		t.Fatalf("seed del add: %v", err)
	}

	err := r.Replicate(ctx,
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{newRoot}}},
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{hAdd}}},
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{upd}}},
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{del}}},
	)
	if err == nil {
		t.Fatalf("expected replicate error due to passive file path")
	}
	if !rt.FailedToReplicate {
		t.Fatalf("expected FailedToReplicate set after replication errors")
	}
}
