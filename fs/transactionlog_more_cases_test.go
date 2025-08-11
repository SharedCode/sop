package fs

import (
    "os"
    "path/filepath"
    "testing"
)

// Covers TransactionLog.NewUUID and the error branch of getFilesSortedDescByModifiedTime
// when the provided path is a file (not a directory).
func TestTransactionLogNewUUIDAndSortedFilesError(t *testing.T) {
    // NewUUID should return a non-nil UUID.
    tl := &TransactionLog{}
    id := tl.NewUUID()
    if id.IsNil() {
        t.Fatalf("NewUUID returned nil uuid")
    }

    // Create a file and pass its path as a directory to force ReadDir error.
    dir := t.TempDir()
    filePath := filepath.Join(dir, "not_a_dir.txt")
    if err := os.WriteFile(filePath, []byte("x"), 0o644); err != nil {
        t.Fatalf("setup write: %v", err)
    }
    if _, err := getFilesSortedDescByModifiedTime(ctx, filePath, ".xyz", nil); err == nil {
        t.Fatalf("expected error when scanning a file path, got nil")
    }
}
