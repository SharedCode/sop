package fs

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
)

// TestBlobStoreBasicExercises NewBlobStore, Add, GetOne, Update (alias), Remove happy paths.
func TestBlobStoreBasicExercises(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	bs := NewBlobStore(nil, nil)

	// Create two blobs in one table.
	id1, id2 := sop.NewUUID(), sop.NewUUID()
	addPayload := []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{
		BlobTable: base,
		Blobs: []sop.KeyValuePair[sop.UUID, []byte]{
			{Key: id1, Value: []byte("v1")},
			{Key: id2, Value: []byte("v2")},
		},
	}}
	if err := bs.Add(ctx, addPayload); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Update replaces contents (call Update which calls Add internally) for id1.
	addPayload[0].Blobs[0].Value = []byte("v1b")
	if err := bs.Update(ctx, addPayload); err != nil {
		t.Fatalf("Update: %v", err)
	}

	// GetOne both ids.
	if b, err := bs.GetOne(ctx, base, id1); err != nil || string(b) != "v1b" {
		t.Fatalf("GetOne id1: %v %s", err, string(b))
	}
	if b, err := bs.GetOne(ctx, base, id2); err != nil || string(b) != "v2" {
		t.Fatalf("GetOne id2: %v %s", err, string(b))
	}

	// Remove id1 only, ensure id2 still present.
	if err := bs.Remove(ctx, []sop.BlobsPayload[sop.UUID]{{BlobTable: base, Blobs: []sop.UUID{id1}}}); err != nil {
		t.Fatalf("Remove id1: %v", err)
	}
	if _, err := bs.GetOne(ctx, base, id1); err == nil {
		t.Fatalf("expected error reading removed blob")
	}
	if b, err := bs.GetOne(ctx, base, id2); err != nil || string(b) != "v2" {
		t.Fatalf("GetOne id2 after remove: %v %s", err, string(b))
	}
}
