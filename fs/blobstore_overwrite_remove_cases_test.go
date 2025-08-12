package fs

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
)

// TestBlobStore_OverwriteAndRemoveNonExistent covers overwrite in Add (directory already exists) and Remove ignoring missing files.
func TestBlobStore_OverwriteAndRemoveNonExistent(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	bs := NewBlobStore(nil, nil)

	id := sop.NewUUID()
	payload := []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: base, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: []byte("one")}}}}
	if err := bs.Add(ctx, payload); err != nil {
		t.Fatalf("add first: %v", err)
	}
	// Overwrite same id with different bytes to exercise existing dir path.
	payload[0].Blobs[0].Value = []byte("two")
	if err := bs.Add(ctx, payload); err != nil {
		t.Fatalf("add overwrite: %v", err)
	}
	// Fetch to confirm overwrite.
	got, err := bs.GetOne(ctx, base, id)
	if err != nil || string(got) != "two" {
		t.Fatalf("get after overwrite: %v %s", err, string(got))
	}

	// Remove non-existent id should be ignored (coverage for exists==false branch).
	missing := sop.NewUUID()
	if err := bs.Remove(ctx, []sop.BlobsPayload[sop.UUID]{{BlobTable: base, Blobs: []sop.UUID{missing}}}); err != nil {
		t.Fatalf("remove missing: %v", err)
	}

	// Remove the real one.
	if err := bs.Remove(ctx, []sop.BlobsPayload[sop.UUID]{{BlobTable: base, Blobs: []sop.UUID{id}}}); err != nil {
		t.Fatalf("remove existing: %v", err)
	}
	// Second removal again ignored.
	_ = bs.Remove(ctx, []sop.BlobsPayload[sop.UUID]{{BlobTable: base, Blobs: []sop.UUID{id}}})
}
