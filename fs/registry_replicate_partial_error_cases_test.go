package fs

import (
	"context"
	"os"
	"runtime"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// failingFileIO simulates a write failure on passive replication path (.reg files) to trigger handleFailedToReplicate.
// (Removed failingFileIO; using passive permission restriction instead)

// TestRegistryReplicatePartialErrors causes replication failure then ensures subsequent replicates early-return.
func TestRegistryReplicatePartialErrors(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := t.TempDir()
	passive := t.TempDir()
	GlobalReplicationDetails = nil
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}

	// Seed baseline store & registry table by adding one handle (non-replicated scenario).
	reg := NewRegistry(true, MinimumModValue, rt, cache)
	h1 := sop.NewHandle(sop.NewUUID())
	if err := reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "pr", IDs: []sop.Handle{h1}}}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := reg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Re-open registry for replication attempts.
	reg2 := NewRegistry(true, MinimumModValue, rt, cache)
	// Make passive folder read-only to trigger write failure on replication attempt (skip on Windows where perms differ).
	if runtime.GOOS != "windows" {
		if err := os.Chmod(passive, 0o500); err != nil {
			t.Fatalf("chmod passive: %v", err)
		}
	}

	h2 := sop.NewHandle(sop.NewUUID())
	err = reg2.Replicate(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "pr", IDs: []sop.Handle{h2}}}, nil, nil, nil)
	if err == nil {
		t.Fatalf("expected replication failure error")
	}
	if !rt.FailedToReplicate {
		t.Fatalf("expected FailedToReplicate set true")
	}

	// Second replicate should early-return with nil error.
	if err := reg2.Replicate(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "pr", IDs: []sop.Handle{h2}}}, nil, nil, nil); err != nil {
		t.Fatalf("expected early-return nil error, got %v", err)
	}
}
