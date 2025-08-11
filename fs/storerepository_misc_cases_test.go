package fs

import (
    "context"
    "testing"
    "path/filepath"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/common/mocks"
)

func TestStoreRepository_GetStoresBaseFolderAndGetFromCache(t *testing.T) {
    ctx := context.Background()
    active := filepath.Join(t.TempDir(), "a")
    passive := filepath.Join(t.TempDir(), "p")
    rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
    if err != nil { t.Fatalf("rt: %v", err) }
    rt.ActiveFolderToggler = true

    cache := mocks.NewMockClient()
    sr, err := NewStoreRepository(ctx, rt, nil, cache, MinimumModValue)
    if err != nil { t.Fatalf("NewStoreRepository: %v", err) }

    // Verify GetStoresBaseFolder reflects active folder
    if got := sr.GetStoresBaseFolder(); got != active {
        t.Fatalf("GetStoresBaseFolder: got %q want %q", got, active)
    }

    // Populate cache with a StoreInfo and ensure getFromCache returns it, while ignoring missing keys.
    s1 := sop.NewStoreInfo(sop.StoreOptions{Name: "s1", SlotLength: 10})
    if err := cache.SetStruct(ctx, s1.Name, s1, 0); err != nil {
        t.Fatalf("cache.SetStruct: %v", err)
    }
    res, err := sr.getFromCache(ctx, s1.Name, "does-not-exist")
    if err != nil { t.Fatalf("getFromCache: %v", err) }
    if len(res) != 1 || res[0].Name != s1.Name {
        t.Fatalf("getFromCache mismatch: %+v", res)
    }
}
