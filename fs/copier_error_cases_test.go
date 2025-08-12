package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestCopyFilesByExtensionErrorBranches covers source read error, mkdir failure, and file copy error.
func TestCopyFilesByExtensionErrorBranches(t *testing.T) {
	ctx := context.Background()

	// 1. Source directory read error (directory doesn't exist)
	if err := copyFilesByExtension(ctx, filepath.Join(t.TempDir(), "missing"), t.TempDir(), ".x"); err == nil {
		t.Fatalf("expected error for missing source directory")
	}

	// 2. Mkdir failure: create a file at target path so MkdirAll sees path existing as file
	src := t.TempDir()
	// create a .reg file to try copying later
	if err := os.WriteFile(filepath.Join(src, "z.reg"), []byte("data"), 0o644); err != nil {
		t.Fatalf("prep file: %v", err)
	}
	targetParent := t.TempDir()
	target := filepath.Join(targetParent, "subdir")
	// create file with same name instead of directory
	if err := os.WriteFile(target, []byte("file"), 0o644); err != nil {
		t.Fatalf("prep target file: %v", err)
	}
	if err := copyFilesByExtension(ctx, src, target, ".reg"); err == nil {
		t.Fatalf("expected mkdir failure due to existing file at target path")
	}

	// 3. Copy file content error: make target dir exist but remove source file after listing
	src2 := t.TempDir()
	if err := os.WriteFile(filepath.Join(src2, "k.reg"), []byte("data"), 0o644); err != nil {
		t.Fatalf("prep k.reg: %v", err)
	}
	dst2 := t.TempDir()
	// Start copy in a goroutine? Not needed; we can simulate by removing after listing: rename file to directory after listing
	// Simplify: override permissions on dst2 to read-only to force create failure
	if err := os.Chmod(dst2, 0o500); err != nil {
		t.Fatalf("chmod dst2: %v", err)
	}
	if err := copyFilesByExtension(ctx, src2, dst2, ".reg"); err == nil {
		t.Fatalf("expected error creating target file due to permissions")
	}
}
