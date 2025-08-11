package fs

import (
    "context"
    "os"
    "path/filepath"
    "testing"
)

func TestManageStoreFolderCreateRemove(t *testing.T) {
    ctx := context.Background()
    base := filepath.Join(t.TempDir(), "stores", "a", "b")

    ms := NewManageStoreFolder(nil)
    if err := ms.CreateStore(ctx, base); err != nil {
        t.Fatalf("CreateStore: %v", err)
    }
    if _, err := os.Stat(base); err != nil {
        t.Fatalf("expected folder to exist: %v", err)
    }

    if err := ms.RemoveStore(ctx, filepath.Dir(base)); err != nil {
        t.Fatalf("RemoveStore: %v", err)
    }
}
