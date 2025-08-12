package fs

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestUpdateFileBlockRegion_PartialRead forces a short read (n < blockSize, err == nil) to exercise the partial read branch.
func TestUpdateFileBlockRegion_PartialRead(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	rt, _ := NewReplicationTracker(ctx, []string{t.TempDir()}, false, l2)
	hm := newHashmap(true, 8, rt, l2) // small hashModValue sufficient

	// Prepare a direct I/O file smaller than one block.
	fn := t.TempDir() + string(os.PathSeparator) + "seg-1.reg"
	dio := newFileDirectIO()
	if err := dio.open(ctx, fn, os.O_CREATE|os.O_RDWR, permission); err != nil {
		t.Fatalf("open: %v", err)
	}
	// Write less than a block so a full block read returns partial length without error.
	half := blockSize / 2
	ba := make([]byte, half)
	if n, err := dio.file.WriteAt(ba, 0); err != nil || n != half {
		t.Fatalf("seed write: n=%d err=%v", n, err)
	}
	if err := dio.file.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// Call updateFileBlockRegion expecting partial read error.
	handleData := make([]byte, sop.HandleSizeInBytes)
	err := hm.updateFileBlockRegion(ctx, dio, 0, 0, handleData)
	if err == nil || !(strings.Contains(err.Error(), "only partially (n=") || strings.Contains(err.Error(), "EOF")) {
		t.Fatalf("expected partial read error/EOF, got %v", err)
	}
}

// TestUpdateFileBlockRegion_WriteError opens file read-only to trigger writeAt error branch (err != nil case).
func TestUpdateFileBlockRegion_WriteError(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	rt, _ := NewReplicationTracker(ctx, []string{t.TempDir()}, false, l2)
	hm := newHashmap(true, 8, rt, l2)

	fn := t.TempDir() + string(os.PathSeparator) + "seg-1.reg"
	dio := newFileDirectIO()
	// Create a full block-sized file to avoid partial read error.
	if err := dio.open(ctx, fn, os.O_CREATE|os.O_RDWR, permission); err != nil {
		t.Fatalf("open: %v", err)
	}
	full := blockSize
	ba := make([]byte, full)
	if n, err := dio.file.WriteAt(ba, 0); err != nil || n != full {
		t.Fatalf("seed write full block: n=%d err=%v", n, err)
	}
	dio.file.Close()
	// Use a fresh fileDirectIO so open succeeds read-only.
	dioRO := newFileDirectIO()
	if err := dioRO.open(ctx, fn, os.O_RDONLY, permission); err != nil {
		t.Fatalf("ro open: %v", err)
	}

	handleData := make([]byte, sop.HandleSizeInBytes)
	err := hm.updateFileBlockRegion(ctx, dioRO, 0, 0, handleData)
	if err == nil {
		t.Fatalf("expected write error, got nil")
	}
}
