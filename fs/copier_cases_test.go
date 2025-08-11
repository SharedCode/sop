package fs

import (
    "context"
    "os"
    "path/filepath"
    "testing"
)

func TestCopyFilesByExtensionCopiesExpected(t *testing.T) {
    ctx := context.Background()
    src := t.TempDir()
    dst := t.TempDir()

    // Create files in source: one with .reg, one with .txt
    regFile := filepath.Join(src, "a.reg")
    txtFile := filepath.Join(src, "b.txt")

    data := []byte("hello")
    if err := os.WriteFile(regFile, data, 0o644); err != nil { t.Fatalf("write reg: %v", err) }
    if err := os.WriteFile(txtFile, []byte("nope"), 0o644); err != nil { t.Fatalf("write txt: %v", err) }

    if err := copyFilesByExtension(ctx, src, dst, ".reg"); err != nil {
        t.Fatalf("copyFilesByExtension: %v", err)
    }

    // Only .reg should be copied
    if _, err := os.Stat(filepath.Join(dst, "a.reg")); err != nil {
        t.Fatalf("expected a.reg to be copied: %v", err)
    }
    if _, err := os.Stat(filepath.Join(dst, "b.txt")); err == nil {
        t.Fatalf("did not expect b.txt to be copied")
    }
}
