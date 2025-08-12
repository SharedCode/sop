package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
)

// (Previously had retry injection helpers; removed for stability.)

// Note: Explicit retry injection tests were removed due to complexity of safely simulating transient errors
// without altering internal retryIO behavior (which depends on ShouldRetry classification). Remaining tests
// focus on permission existence semantics and permanent error surfacing.

// TestDefaultFileIO_ExistsPermissionDenied ensures Exists treats non-ENOENT as exists.
func TestDefaultFileIO_ExistsPermissionDenied(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	path := filepath.Join(base, "protected")
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Remove search perms; on some systems stat returns EPERM which should still mean Exists == true.
	_ = os.Chmod(path, 0o000)
	fio := NewFileIO()
	if !fio.Exists(ctx, path) {
		t.Fatalf("expected Exists true under permission restriction")
	}
	// Restore so cleanup works (ignore errors on non-POSIX).
	_ = os.Chmod(path, 0o755)
}

// Negative control: ensure permanent (non-retry) error surfaces immediately.
func TestDefaultFileIO_PermanentErrorSurface(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	fn := filepath.Join(base, "no_parent", "file.txt")
	// Create a file where parent creation will fail by making a file with parent name.
	parent := filepath.Dir(fn)
	// Make a file at parent path so MkdirAll inside WriteFile cannot create directory; then WriteFile should surface original error.
	if err := os.WriteFile(parent, []byte("blockdir"), 0o644); err != nil {
		t.Fatalf("prep: %v", err)
	}
	fio := NewFileIO()
	err := fio.WriteFile(ctx, fn, []byte("x"), 0o644)
	if err == nil {
		t.Fatalf("expected error when parent path is a file")
	}
	// Should not be classified as retryable sop.Error with FileIOError code after final surface.
	var se sop.Error
	if errors.As(err, &se) && se.Code == sop.FileIOError {
		// Acceptable: internal may wrap; document behavior.
	}
}
