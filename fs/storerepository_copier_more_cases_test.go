package fs

import (
    "context"
    "os"
    "path/filepath"
    "testing"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/common/mocks"
)

func TestCopyToPassiveFoldersCopiesRegistrySegments(t *testing.T) {
    ctx := context.Background()
    l2 := mocks.NewMockClient()

    active := t.TempDir()
    passive := t.TempDir()
    rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, l2)

    sr, err := NewStoreRepository(ctx, rt, nil, l2, MinimumModValue)
    if err != nil { t.Fatalf("NewStoreRepository: %v", err) }

    si := sop.NewStoreInfo(sop.StoreOptions{Name: "c1", SlotLength: 10})
    if err := sr.Add(ctx, *si); err != nil { t.Fatalf("Add: %v", err) }

    // Create a registry file in active for this store.
    reg := NewRegistry(true, MinimumModValue, rt, l2)
    lid := sop.NewUUID()
    if err := reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: si.RegistryTable, IDs: []sop.Handle{{LogicalID: lid}}}}); err != nil {
        t.Fatalf("registry add: %v", err)
    }
    reg.Close()

    if err := sr.CopyToPassiveFolders(ctx); err != nil { t.Fatalf("CopyToPassiveFolders: %v", err) }

    // Verify store list + info copied
    if _, err := os.Stat(filepath.Join(passive, storeListFilename)); err != nil {
        t.Fatalf("store list missing in passive: %v", err)
    }
    if _, err := os.Stat(filepath.Join(passive, "c1", storeInfoFilename)); err != nil {
        t.Fatalf("storeinfo missing in passive: %v", err)
    }
    // Verify at least one registry segment exists for this store in passive
    // Registry files are placed under the registry table folder (e.g., c1_r), not under the store name.
    entries, err := os.ReadDir(filepath.Join(passive, sop.FormatRegistryTable("c1")))
    if err != nil { t.Fatalf("readdir: %v", err) }
    foundReg := false
    for _, e := range entries {
        if !e.IsDir() && filepath.Ext(e.Name()) == registryFileExtension {
            foundReg = true
            break
        }
    }
    if !foundReg {
        t.Fatalf("expected a registry segment file to be copied to passive")
    }
}
