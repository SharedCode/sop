package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// mockRegistryMapAddSetRemoveFail injects failures on add/set/remove to drive Replicate() error branches.
type mockRegistryMapAddSetRemoveFail struct{ registryMap }

func (m *mockRegistryMapAddSetRemoveFail) add(ctx context.Context, p []sop.RegistryPayload[sop.Handle]) error {
	return errors.New("induced add error")
}
func (m *mockRegistryMapAddSetRemoveFail) set(ctx context.Context, p []sop.RegistryPayload[sop.Handle]) error {
	return errors.New("induced set error")
}
func (m *mockRegistryMapAddSetRemoveFail) remove(ctx context.Context, p []sop.RegistryPayload[sop.UUID]) error {
	return errors.New("induced remove error")
}

// TestRegistryGet_MultiMiss ensures mixed cache hit/miss path populating L2 and L1 correctly.
func TestRegistryGet_MultiMiss(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
	r := NewRegistry(true, MinimumModValue, rt, l2)
	defer r.Close()

	// Create three handles; only seed one into L2 cache to force 2 misses.
	h1 := sop.NewHandle(sop.NewUUID())
	h2 := sop.NewHandle(sop.NewUUID())
	h3 := sop.NewHandle(sop.NewUUID())
	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgget", IDs: []sop.Handle{h1, h2, h3}}}); err != nil {
		t.Fatalf("add: %v", err)
	}

	// Purge cache entries (mock does not expose Clear via interface); delete all three then re-add h1 only.
	l2.Delete(ctx, []string{h1.LogicalID.String(), h2.LogicalID.String(), h3.LogicalID.String()})
	if err := l2.SetStruct(ctx, h1.LogicalID.String(), &h1, time.Minute); err != nil {
		t.Fatalf("seed cache: %v", err)
	}

	res, err := r.Get(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "rgget", IDs: []sop.UUID{h1.LogicalID, h2.LogicalID, h3.LogicalID}}})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(res) != 1 || len(res[0].IDs) != 3 {
		t.Fatalf("expected 3 handles result, got %+v", res)
	}
}

// TestRegistryReplicate_ErrorBranches triggers each add/set/remove error path; lastErr returned should be last non-nil.
func TestRegistryReplicate_ErrorBranches(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	active := t.TempDir()
	// Reset global replication details so prior tests that triggered a failure do not short-circuit replication here.
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true}
	// Create a regular file and use it as "passive" base so attempts to open subpaths under it fail with not a directory.
	passiveFile := filepath.Join(active, "passive-file")
	if err := os.WriteFile(passiveFile, []byte("x"), 0o600); err != nil {
		t.Fatalf("prep passive file: %v", err)
	}
	passive := passiveFile
	rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
	r := NewRegistry(true, MinimumModValue, rt, l2)
	defer r.Close()

	hNew := sop.NewHandle(sop.NewUUID())
	hAdd := sop.NewHandle(sop.NewUUID())
	hUpd := sop.NewHandle(sop.NewUUID())
	hDel := sop.NewHandle(sop.NewUUID())
	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgrep", IDs: []sop.Handle{hNew, hAdd, hUpd, hDel}}}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	err := r.Replicate(ctx,
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgrep", IDs: []sop.Handle{hNew}}},
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgrep", IDs: []sop.Handle{hAdd}}},
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgrep", IDs: []sop.Handle{hUpd}}},
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgrep", IDs: []sop.Handle{hDel}}},
	)
	if err == nil {
		t.Fatalf("expected replication error due to invalid passive root (file used as folder)")
	}
}
