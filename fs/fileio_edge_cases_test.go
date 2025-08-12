package fs

import (
	"context"
	"path/filepath"
	"testing"
)

// Additional coverage for FileIO: Exists(false) and ReadDir error paths.
func TestFileIO_Edges(t *testing.T) {
	ctx := context.Background()
	fio := NewFileIO()
	missing := filepath.Join(t.TempDir(), "does-not-exist", "x.txt")
	if fio.Exists(ctx, missing) {
		t.Fatalf("Exists returned true for missing path")
	}
	if _, err := fio.ReadDir(ctx, missing); err == nil {
		t.Fatalf("expected ReadDir error on missing dir")
	}
}
