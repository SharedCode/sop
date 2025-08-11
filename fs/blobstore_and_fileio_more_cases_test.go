package fs

import (
    "bytes"
    "context"
    "os"
    "path/filepath"
    "testing"

    "github.com/sharedcode/sop"
)

// Test the happy path for Add, GetOne, Update, and Remove using the default FileIO.
func TestBlobStore_AddGetUpdateRemove(t *testing.T) {
    t.Parallel()
    ctx := context.Background()

    base := t.TempDir()
    bs := NewBlobStore(nil, nil)

    // Two blobs across two stores
    id1 := sop.NewUUID()
    id2 := sop.NewUUID()
    storeA := filepath.Join(base, "storeA")
    storeB := filepath.Join(base, "storeB")

    // Add should create parent directories as needed and write files.
    if err := bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
        {BlobTable: storeA, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id1, Value: []byte("hello")}}},
        {BlobTable: storeB, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id2, Value: []byte("world")}}},
    }); err != nil {
        t.Fatalf("Add failed: %v", err)
    }

    // GetOne should read back the exact data.
    got1, err := bs.GetOne(ctx, storeA, id1)
    if err != nil || !bytes.Equal(got1, []byte("hello")) {
        t.Fatalf("GetOne storeA/id1 mismatch, err=%v got=%q", err, string(got1))
    }
    got2, err := bs.GetOne(ctx, storeB, id2)
    if err != nil || !bytes.Equal(got2, []byte("world")) {
        t.Fatalf("GetOne storeB/id2 mismatch, err=%v got=%q", err, string(got2))
    }

    // Update should overwrite existing data (delegates to Add).
    if err := bs.Update(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
        {BlobTable: storeA, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id1, Value: []byte("HELLO")}}},
    }); err != nil {
        t.Fatalf("Update failed: %v", err)
    }
    got1b, err := bs.GetOne(ctx, storeA, id1)
    if err != nil || !bytes.Equal(got1b, []byte("HELLO")) {
        t.Fatalf("GetOne after Update mismatch, err=%v got=%q", err, string(got1b))
    }

    // Remove should delete existing files and ignore non-existent ones.
    if err := bs.Remove(ctx, []sop.BlobsPayload[sop.UUID]{
        {BlobTable: storeA, Blobs: []sop.UUID{id1}},
        {BlobTable: storeB, Blobs: []sop.UUID{id2}},
    }); err != nil {
        t.Fatalf("Remove failed: %v", err)
    }
    if _, err := bs.GetOne(ctx, storeA, id1); err == nil {
        t.Fatalf("expected error reading removed blob id1")
    }
    if _, err := bs.GetOne(ctx, storeB, id2); err == nil {
        t.Fatalf("expected error reading removed blob id2")
    }

    // Removing again should be tolerated.
    if err := bs.Remove(ctx, []sop.BlobsPayload[sop.UUID]{{BlobTable: storeA, Blobs: []sop.UUID{id1}}}); err != nil {
        t.Fatalf("Remove non-existent should not fail: %v", err)
    }
}

// Test defaultFileIO branches directly: parent creation on WriteFile, Exists, ReadDir error, and RemoveAll.
func TestDefaultFileIO_Primitives(t *testing.T) {
    t.Parallel()
    ctx := context.Background()
    dio := NewFileIO()

    base := t.TempDir()
    // Write into a nested path whose parents do not yet exist to exercise MkdirAll path in WriteFile.
    nestedDir := filepath.Join(base, "a", "b", "c")
    filename := filepath.Join(nestedDir, "file.txt")
    if err := dio.WriteFile(ctx, filename, []byte("x"), 0o644); err != nil {
        t.Fatalf("WriteFile with parent creation failed: %v", err)
    }

    // Exists for both file and directory should be true.
    if !dio.Exists(ctx, nestedDir) {
        t.Fatalf("Exists should be true for directory: %s", nestedDir)
    }
    if !dio.Exists(ctx, filename) {
        t.Fatalf("Exists should be true for file: %s", filename)
    }
    // Non-existent path should be false.
    if dio.Exists(ctx, filepath.Join(base, "doesnot", "exist")) {
        t.Fatalf("Exists should be false for a non-existent path")
    }

    // ReadDir on a file path should return an error (not a directory) and cover non-retry branch.
    if _, err := dio.ReadDir(ctx, filename); err == nil {
        t.Fatalf("ReadDir on file should error")
    }

    // Remove and RemoveAll should succeed.
    if err := dio.Remove(ctx, filename); err != nil {
        t.Fatalf("Remove failed: %v", err)
    }
    if err := dio.RemoveAll(ctx, filepath.Join(base, "a")); err != nil {
        t.Fatalf("RemoveAll failed: %v", err)
    }
    // After removal, the directory should no longer exist.
    if _, err := os.Stat(filepath.Join(base, "a")); !os.IsNotExist(err) {
        t.Fatalf("expected removed directory to be absent, got err=%v", err)
    }
}
