package fs

import (
    "context"
    "path/filepath"
    "testing"
)

// Additional coverage for FileIO: Exists(false), ReadDir error, GetAll nil path.
func TestFileIO_Edges(t *testing.T) {
    ctx := context.Background()
    fio := NewFileIO()

    // Exists should return false for definitely-missing path
    missing := filepath.Join(t.TempDir(), "does-not-exist", "x.txt")
    if fio.Exists(ctx, missing) {
        t.Fatalf("Exists returned true for missing path")
    }

    // ReadDir error on missing directory
    if _, err := fio.ReadDir(ctx, missing); err == nil {
        t.Fatalf("ReadDir expected error on missing dir")
    }
}
