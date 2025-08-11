package fs

import (
    "bytes"
    "context"
    "os"
    "path/filepath"
    "testing"

    "github.com/sharedcode/sop"
)

func TestBlobStoreAddGetRemove(t *testing.T) {
    ctx := context.Background()
    base := filepath.Join(t.TempDir(), "blobs")

    bs := NewBlobStore(DefaultToFilePath, NewFileIO())

    id1 := sop.NewUUID()
    id2 := sop.NewUUID()
    b1 := []byte("alpha")
    b2 := []byte("bravo")

    // add two blobs
    if err := bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
        {BlobTable: base, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id1, Value: b1}, {Key: id2, Value: b2}}},
    }); err != nil {
        t.Fatalf("add failed: %v", err)
    }

    // get first
    got, err := bs.GetOne(ctx, base, id1)
    if err != nil {
        t.Fatalf("get failed: %v", err)
    }
    if !bytes.Equal(got, b1) {
        t.Fatalf("mismatch: got %q want %q", string(got), string(b1))
    }

    // remove first and ensure subsequent get errors
    if err := bs.Remove(ctx, []sop.BlobsPayload[sop.UUID]{{BlobTable: base, Blobs: []sop.UUID{id1}}}); err != nil {
        t.Fatalf("remove failed: %v", err)
    }
    if _, err := bs.GetOne(ctx, base, id1); err == nil {
        t.Fatalf("expected error on removed blob")
    }

    // removing again should be a no-op
    if err := bs.Remove(ctx, []sop.BlobsPayload[sop.UUID]{{BlobTable: base, Blobs: []sop.UUID{id1}}}); err != nil {
        t.Fatalf("2nd remove failed: %v", err)
    }

    // sanity: file for id2 still exists on disk
    dir := DefaultToFilePath(base, id2)
    fn := filepath.Join(dir, id2.String())
    if _, err := os.Stat(fn); err != nil {
        t.Fatalf("expected %s to exist: %v", fn, err)
    }
}

func TestBlobStoreUpdateOverwrite(t *testing.T) {
    ctx := context.Background()
    base := filepath.Join(t.TempDir(), "blobs")
    bs := NewBlobStore(nil, nil) // exercise defaults

    id := sop.NewUUID()
    v1 := []byte("one")
    v2 := []byte("two-updated")

    if err := bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
        {BlobTable: base, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: v1}}},
    }); err != nil {
        t.Fatalf("add failed: %v", err)
    }

    if err := bs.Update(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
        {BlobTable: base, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: v2}}},
    }); err != nil {
        t.Fatalf("update failed: %v", err)
    }

    got, err := bs.GetOne(ctx, base, id)
    if err != nil {
        t.Fatalf("get failed: %v", err)
    }
    if !bytes.Equal(got, v2) {
        t.Fatalf("mismatch: got %q want %q", string(got), string(v2))
    }
}
