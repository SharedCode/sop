package fs

import (
    "context"
    "os"
    "path/filepath"
    "testing"

    "github.com/sharedcode/sop"
)

// Ensure Add returns an error when write errors exceed parity tolerance.
func TestBlobStoreWithEC_AddFailsWhenErrorsExceedParity(t *testing.T) {
    ctx := context.Background()

    // Create three base folders under a temp root; make two of them read-only
    root := t.TempDir()
    p1 := filepath.Join(root, "disk1")
    p2 := filepath.Join(root, "disk2")
    p3 := filepath.Join(root, "disk3")
    for _, d := range []string{p1, p2, p3} {
        if err := os.MkdirAll(d, 0o755); err != nil { t.Fatalf("mkdir: %v", err) }
    }
    // Make two drives read-only to force write failures on shard creation
    if err := os.Chmod(p1, 0o555); err != nil { t.Fatalf("chmod p1: %v", err) }
    if err := os.Chmod(p2, 0o555); err != nil { t.Fatalf("chmod p2: %v", err) }

    // Configure EC: 2 data + 1 parity => tolerate 1 failure; we'll induce 2
    table := "tbl_ec_err"
    // Save and restore global config to avoid cross-test interference.
    prev := GetGlobalErasureConfig()
    defer SetGlobalErasureConfig(prev)
    SetGlobalErasureConfig(map[string]ErasureCodingConfig{
        table: {
            DataShardsCount:   2,
            ParityShardsCount: 1,
            BaseFolderPathsAcrossDrives: []string{p1, p2, p3},
            RepairCorruptedShards: true,
        },
    })

    bsIntf, err := NewBlobStoreWithEC(DefaultToFilePath, nil, nil)
    if err != nil { t.Fatalf("NewBlobStoreWithEC: %v", err) }
    bs := bsIntf

    id := sop.NewUUID()
    payload := []byte("abcdefg")
    err = bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
        {BlobTable: table, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: payload}}},
    })
    if err == nil {
        t.Fatalf("expected Add to fail when errors > parity, got nil")
    }
}
