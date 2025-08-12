package fs

import (
	"context"
	"os"
	"testing"
)

// TestDirectIOForceCoverage instantiates the concrete directIO type directly to ensure its methods are executed under coverage.
func TestDirectIOForceCoverage(t *testing.T) {
	ctx := context.Background()
	d := directIO{}
	dir := t.TempDir()
	fn := dir + string(os.PathSeparator) + "force.dat"
	f, err := d.Open(ctx, fn, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	blk := make([]byte, blockSize)
	blk[0] = 1
	if _, err := d.WriteAt(ctx, f, blk, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	rb := make([]byte, blockSize)
	if _, err := d.ReadAt(ctx, f, rb, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if rb[0] != 1 {
		t.Fatalf("unexpected read data")
	}
	if err := d.Close(f); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
