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

	// Create a file inside the passive directory that we'll incorrectly treat as the passive base path.
	badPassiveFile := passive + string(os.PathSeparator) + "passive_as_file"
	if err := os.WriteFile(badPassiveFile, []byte("x"), 0o600); err != nil {
		t.Fatalf("setup file: %v", err)
	}

	// Pass the file path (not the directory) as the second replication root to trigger errors.
	rt, _ := NewReplicationTracker(ctx, []string{active, badPassiveFile}, true, l2)
	r := NewRegistry(true, MinimumModValue, rt, l2)
	defer r.Close()

	// Seed active with handles used in added/updated/removed slices.
	hAdd := sop.NewHandle(sop.NewUUID())
	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{hAdd}}}); err != nil {
		t.Fatalf("seed add: %v", err)
	}
	del := sop.NewHandle(sop.NewUUID())
	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{del}}}); err != nil {
		t.Fatalf("seed del add: %v", err)
	}
	upd := hAdd
	upd.Version = 2
	newRoot := sop.NewHandle(sop.NewUUID())

	// Attempt replicate; if failure flag wasn't set yet, this first call should error.
	firstErr := r.Replicate(ctx,
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{newRoot}}},
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{hAdd}}},
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{upd}}},
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{del}}},
	)
	if firstErr == nil && !rt.FailedToReplicate {
		// We expected either an error (before flag) or short-circuit nil (after flag). Having neither flag set nor error is unexpected.
		t.Fatalf("expected replicate error on first call or FailedToReplicate already set")
	}

	// Ensure flag is set now.
	if !rt.FailedToReplicate {
		t.Fatalf("expected FailedToReplicate set after replicate attempt")
	}

	// Second replicate should short-circuit returning nil.
	if err := r.Replicate(ctx,
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{newRoot}}},
		nil, nil, nil,
	); err != nil {
		t.Fatalf("expected nil replicate (short-circuit) on second call, got %v", err)
	}
}
