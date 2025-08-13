package fs

import (
    "context"
    "os"
    "testing"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/common/mocks"
)

// Consolidated scenario restoring coverage for registry Replicate failure + short-circuit behavior
// previously in replicate_error_branches_cases_test.go.
func TestRegistry_ReplicateErrorBranches_Scenario(t *testing.T) {
    ctx := context.Background()
    cache := mocks.NewMockClient()
    active := t.TempDir()
    passiveDir := t.TempDir()
    // Create a file inside passiveDir and then use that file path as passive root to induce errors.
    passiveFile := passiveDir + string(os.PathSeparator) + "passive_as_file"
    if err := os.WriteFile(passiveFile, []byte("x"), 0o600); err != nil { t.Fatalf("setup file: %v", err) }

    rt, _ := NewReplicationTracker(ctx, []string{active, passiveFile}, true, cache)
    r := NewRegistry(true, MinimumModValue, rt, cache)
    defer r.Close()

    // Seed active with a handle used in added/updated/removed slices.
    hAdd := sop.NewHandle(sop.NewUUID())
    if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{hAdd}}}); err != nil {
        t.Fatalf("seed add: %v", err)
    }
    del := sop.NewHandle(sop.NewUUID())
    if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{del}}}); err != nil {
        t.Fatalf("seed del add: %v", err)
    }
    upd := hAdd; upd.Version = 2
    newRoot := sop.NewHandle(sop.NewUUID())

    firstErr := r.Replicate(ctx,
        []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{newRoot}}},
        []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{hAdd}}},
        []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{upd}}},
        []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{del}}},
    )
    if firstErr == nil && !rt.FailedToReplicate {
        t.Fatalf("expected replicate error or failure flag set on first call")
    }
    if !rt.FailedToReplicate { t.Fatalf("expected FailedToReplicate flag set") }

    // Second replicate should short-circuit returning nil.
    if err := r.Replicate(ctx,
        []sop.RegistryPayload[sop.Handle]{{RegistryTable: "regrep", IDs: []sop.Handle{newRoot}}},
        nil, nil, nil,
    ); err != nil { t.Fatalf("expected nil on second replicate, got %v", err) }
}
