package fs

import (
    "context"
    "os"
    "path/filepath"
    "testing"
)

// Covers defaultFileIO.WriteFile path where initial write fails (missing parent directory),
// MkdirAll succeeds, then retryIO performs the write.
func TestDefaultFileIO_WriteFile_CreatesParentAndRetries(t *testing.T) {
    ctx := context.Background()
    base := t.TempDir()
    target := filepath.Join(base, "nested", "deep", "file.txt")
    dio := NewFileIO()
    if err := dio.WriteFile(ctx, target, []byte("hello"), 0o644); err != nil {
        t.Fatalf("WriteFile: %v", err)
    }
    if _, err := os.Stat(target); err != nil {
        t.Fatalf("file not created: %v", err)
    }
}

// Covers branch where MkdirAll fails (parent path collision) and original write error is returned.
func TestDefaultFileIO_WriteFile_ParentCreateFails(t *testing.T) {
    ctx := context.Background()
    base := t.TempDir()
    // Create a file that will act as a conflicting path component.
    parentFile := filepath.Join(base, "parent")
    if err := os.WriteFile(parentFile, []byte("x"), 0o644); err != nil { t.Fatalf("prep parent file: %v", err) }
    target := filepath.Join(parentFile, "child", "file.txt") // parentFile is a file, so MkdirAll on its subpath fails
    dio := NewFileIO()
    if err := dio.WriteFile(ctx, target, []byte("data"), 0o644); err == nil {
        t.Fatalf("expected error due to parent create failure")
    }
}
