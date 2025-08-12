package fs

import (
	"context"
	"os"
	"testing"
)

// TestDirectIOBasic covers open->write->read->close happy path using aligned blocks.
func TestDirectIOBasic(t *testing.T) {
	ctx := context.Background()
	dio := NewDirectIO()
	dir := t.TempDir()
	fn := dir + string(os.PathSeparator) + "dblk.dat"
	f, err := dio.Open(ctx, fn, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer dio.Close(f)

	// Allocate aligned block and write pattern.
	blk := make([]byte, blockSize)
	for i := range blk {
		blk[i] = byte(i % 251)
	}
	if n, err := dio.WriteAt(ctx, f, blk, 0); err != nil || n != len(blk) {
		t.Fatalf("write: %v n=%d", err, n)
	}

	// Read back into new buffer.
	rb := make([]byte, blockSize)
	if n, err := dio.ReadAt(ctx, f, rb, 0); err != nil || n != len(rb) {
		t.Fatalf("read: %v n=%d", err, n)
	}
	for i := range rb {
		if rb[i] != blk[i] {
			t.Fatalf("mismatch at %d", i)
		}
	}
}
