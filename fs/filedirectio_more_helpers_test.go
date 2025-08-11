package fs

import (
    "testing"
)

func TestFileDirectIOHelpers(t *testing.T) {
    d := newFileDirectIO()
    // Aligned block creators
    if b := d.createAlignedBlock(); len(b) == 0 {
        t.Fatalf("createAlignedBlock returned empty buffer")
    }
    if b := d.createAlignedBlockOfSize(4096); len(b) != 4096 {
        t.Fatalf("createAlignedBlockOfSize size mismatch: %d", len(b))
    }
}
