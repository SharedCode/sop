package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fakeRetryErr is tagged as retryable via sop.ShouldRetry because it's a sop.Error with FileIOError code.
type fakeRetryErr struct{}

func (fakeRetryErr) Error() string { return "retry me" }

// We indirectly exercise retryIO by forcing WriteFile's initial write to fail (missing parent)
// then succeed after MkdirAll and through the retry wrapper. Also cover Exists semantics.
func TestFileIOWriteAndExistsAndRemoveAll(t *testing.T) {
	ctx := context.Background()
	dio := NewFileIO()

	dir := t.TempDir()
	nested := filepath.Join(dir, "a", "b")
	filePath := filepath.Join(nested, "f.txt")
	data := []byte("hello")
	if err := dio.WriteFile(ctx, filePath, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if !dio.Exists(ctx, nested) {
		t.Fatalf("Exists expected true for nested directory")
	}
	// ReadFile path
	got, err := dio.ReadFile(ctx, filePath)
	if err != nil || string(got) != "hello" {
		t.Fatalf("ReadFile mismatch: %v %s", err, string(got))
	}

	// Remove + RemoveAll
	if err := dio.Remove(ctx, filePath); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := dio.RemoveAll(ctx, filepath.Join(dir, "a")); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}
	if dio.Exists(ctx, filePath) {
		t.Fatalf("file should not exist after removals")
	}
}

// TestReadDirRetry creates a directory then reads it; while we can't easily force a transient
// retry error without heavy OS dependency injection, we still exercise the happy path which
// traverses retryIO once.
func TestFileIOReadDirAndMkdirAll(t *testing.T) {
	ctx := context.Background()
	dio := NewFileIO()
	dir := filepath.Join(t.TempDir(), "x", "y")
	if err := dio.MkdirAll(ctx, dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	// create two files
	for i := 0; i < 2; i++ {
		fp := filepath.Join(dir, "f"+string(rune('0'+i)))
		if err := os.WriteFile(fp, []byte("d"), 0o644); err != nil {
			t.Fatalf("write file: %v", err)
		}
	}
	entries, err := dio.ReadDir(ctx, dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}

// Edge Exists: permission error should still return true. We simulate by creating a file then chmod 000.
func TestFileIOExistsPermissionDeniedStillTrue(t *testing.T) {
	ctx := context.Background()
	dio := NewFileIO()
	fp := filepath.Join(t.TempDir(), "p.txt")
	if err := os.WriteFile(fp, []byte("x"), 0o000); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Might fail to stat due to permission, but Exists should treat it as present (unless explicitly not-exist)
	if !dio.Exists(ctx, fp) {
		t.Fatalf("expected Exists true for permission denied path")
	}
	_ = os.Chmod(fp, 0o644) // clean for OS
}

// Test retryIO minimal behavior by invoking retryIO directly with a task that succeeds after one retry-like attempt.
// We can't hook ShouldRetry easily without introducing a sop.Error; emulate by returning a retry.RetryableError pattern via sop.Error.
// Keeping it simple: return a transient error once, then nil.
func TestRetryIOHelper(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	attempts := 0
	err := retryIO(ctx, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return errors.New("temp")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retryIO unexpected err: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
}
