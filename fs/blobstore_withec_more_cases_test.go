package fs

import (
    "context"
    "testing"

    "github.com/sharedcode/sop"
)

// Covers GetGlobalErasureConfig non-nil path and Update delegating to Add.
func TestBlobStoreWithECGetGlobalAndUpdate(t *testing.T) {
    ctx := context.Background()
    // Ensure global config is set via init() in blobstore.withec_test.go
    if GetGlobalErasureConfig() == nil {
        t.Fatalf("expected global EC config to be set")
    }
    fileIO := newFileIOSim()
    bs, err := NewBlobStoreWithEC(DefaultToFilePath, fileIO, nil)
    if err != nil { t.Fatalf("NewBlobStoreWithEC: %v", err) }

    id := sop.NewUUID()
    data := []byte{9,8,7}

    // Update should behave like Add
    if err := bs.Update(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
        {BlobTable: "b1", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: data}}},
    }); err != nil { t.Fatalf("update(Add): %v", err) }

    // Sanity: read back
    if got, err := bs.GetOne(ctx, "b1", id); err != nil || len(got) == 0 {
        t.Fatalf("get after update: %v, %v", got, err)
    }
}
