package fs

import (
    "context"
    "os"
    "path/filepath"
    "testing"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/common/mocks"
)

// End-to-end: CopyToPassiveFolders writes store list, storeinfo, and .reg files to passive.
func TestStoreRepositoryCopyToPassiveFolders_E2E(t *testing.T) {
    ctx := context.Background()
    active := t.TempDir()
    passive := t.TempDir()

    // Seed a store in the active side with one registry segment file
    rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
    if err != nil { t.Fatalf("rt: %v", err) }
    rt.ActiveFolderToggler = true

    sr, err := NewStoreRepository(ctx, rt, nil, mocks.NewMockClient(), MinimumModValue)
    if err != nil { t.Fatalf("NewStoreRepository: %v", err) }

    si := sop.NewStoreInfo(sop.StoreOptions{Name: "c1", SlotLength: 10})
    if err := sr.Add(ctx, *si); err != nil { t.Fatalf("Add: %v", err) }

    // Create a registry folder and a dummy .reg segment under active
    regDir := filepath.Join(active, si.RegistryTable)
    if err := os.MkdirAll(regDir, 0o755); err != nil { t.Fatalf("mkdir reg: %v", err) }
    seg := filepath.Join(regDir, "0000-0000.reg")
    if err := os.WriteFile(seg, []byte("x"), 0o644); err != nil { t.Fatalf("seed .reg: %v", err) }

    if err := sr.CopyToPassiveFolders(ctx); err != nil { t.Fatalf("CopyToPassiveFolders: %v", err) }

    // Verify passive has store list, storeinfo, and copied .reg
    if _, err := os.Stat(filepath.Join(passive, storeListFilename)); err != nil {
        t.Fatalf("store list not copied: %v", err)
    }
    if _, err := os.Stat(filepath.Join(passive, si.Name, storeInfoFilename)); err != nil {
        t.Fatalf("storeinfo not copied: %v", err)
    }
    if _, err := os.Stat(filepath.Join(passive, si.RegistryTable, "0000-0000.reg")); err != nil {
        t.Fatalf("registry segment not copied: %v", err)
    }
}
