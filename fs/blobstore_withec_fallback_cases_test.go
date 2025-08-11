package fs

import (
    "context"
    "os"
    "path/filepath"
    "testing"

    "github.com/sharedcode/sop"
)

// When a table-specific EC config is missing, getBaseFolderPathsAndErasureConfig should
// fall back to the global ("") mapping.
func TestBlobStoreWithEC_GlobalFallback(t *testing.T) {
    ctx := context.Background()
    root := t.TempDir()
    p1 := filepath.Join(root, "d1")
    p2 := filepath.Join(root, "d2")
    p3 := filepath.Join(root, "d3")
    for _, d := range []string{p1, p2, p3} {
        if err := os.MkdirAll(d, 0o755); err != nil { t.Fatalf("mkdir: %v", err) }
    }

    prev := GetGlobalErasureConfig()
    defer SetGlobalErasureConfig(prev)
    SetGlobalErasureConfig(map[string]ErasureCodingConfig{
        "": { // default fallback
            DataShardsCount:   2,
            ParityShardsCount: 1,
            BaseFolderPathsAcrossDrives: []string{p1, p2, p3},
            RepairCorruptedShards: true,
        },
    })

    bs, err := NewBlobStoreWithEC(nil, nil, nil)
    if err != nil { t.Fatalf("NewBlobStoreWithEC: %v", err) }

    tbl := "b_fallback"
    id := sop.NewUUID()
    payload := []byte("data")
    if err := bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
        {BlobTable: tbl, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: payload}}},
    }); err != nil { t.Fatalf("Add: %v", err) }

    if got, err := bs.GetOne(ctx, tbl, id); err != nil || string(got) != string(payload) {
        t.Fatalf("GetOne mismatch: %v, %q", err, string(got))
    }
}
