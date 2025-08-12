package fs

import (
	"context"
	"os"
	"testing"

	"github.com/ncw/directio"
)

// TestDirectIORealBasic covers the concrete directIO implementation functions with aligned buffers.
func TestDirectIORealBasic(t *testing.T) {
	ctx := context.Background()
	dio := NewDirectIO()
	dir := t.TempDir()
	fn := dir + string(os.PathSeparator) + "seg.dat"
	f, err := dio.Open(ctx, fn, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	block := directio.AlignedBlock(blockSize)
	copy(block, []byte("hello"))
	if n, err := dio.WriteAt(ctx, f, block, 0); err != nil || n != len(block) {
		t.Fatalf("WriteAt: %v n=%d", err, n)
	}

	// Zero buffer then read back.
	for i := range block {
		block[i] = 0
	}
	if n, err := dio.ReadAt(ctx, f, block, 0); err != nil || n != len(block) {
		t.Fatalf("ReadAt: %v n=%d", err, n)
	}
	if string(block[:5]) != "hello" {
		t.Fatalf("unexpected data prefix: %q", string(block[:5]))
	}

	if err := dio.Close(f); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
