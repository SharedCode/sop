package fs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
)

// fakeDirectIOErrOpen simulates DirectIO always failing open & write/read after open.
type fakeDirectIOErrOpen struct{ DirectIO }

func (f fakeDirectIOErrOpen) Open(ctx context.Context, filename string, flag int, perm os.FileMode) (*os.File, error) {
	return nil, os.ErrPermission
}

// fakeDirectIOSimple implements minimal DirectIO behavior via real file handles.
type fakeDirectIOSimple struct{}

func (f fakeDirectIOSimple) Open(ctx context.Context, filename string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(filename, flag|os.O_CREATE, perm)
}
func (f fakeDirectIOSimple) Close(file *os.File) error { return file.Close() }
func (f fakeDirectIOSimple) WriteAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
	return file.WriteAt(block, offset)
}
func (f fakeDirectIOSimple) ReadAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
	return file.ReadAt(block, offset)
}

// Covers: double open error, writeAt/readAt before open, close w/out open (no-op), open error branch.
func TestFileDirectIOEdgeCases(t *testing.T) {
	ctx := context.Background()
	// Preserve global and restore after.
	prev := DirectIOSim
	defer func() { DirectIOSim = prev }()

	// Case: open failure path (inject failing impl just for this sub-scope).
	DirectIOSim = fakeDirectIOErrOpen{}
	fio := newFileDirectIO()
	if err := fio.open(ctx, filepath.Join(t.TempDir(), "file.dat"), os.O_RDWR, 0o644); err == nil {
		t.Fatalf("expected open error")
	}
	if _, err := fio.writeAt(ctx, []byte("abc"), 0); err == nil {
		t.Fatalf("expected writeAt error when not opened")
	}
	if _, err := fio.readAt(ctx, make([]byte, 4), 0); err == nil {
		t.Fatalf("expected readAt error when not opened")
	}
	if err := fio.close(); err != nil {
		t.Fatalf("close not opened should be nil")
	}

	// Case: normal open -> double open prevented (switch to simple impl).
	DirectIOSim = fakeDirectIOSimple{}
	fio2 := newFileDirectIO()
	name := filepath.Join(t.TempDir(), "f2.dat")
	if err := fio2.open(ctx, name, os.O_RDWR, 0o644); err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := fio2.open(ctx, name, os.O_RDWR, 0o644); err == nil {
		t.Fatalf("expected error on double open")
	}
	data := []byte("hello")
	if _, err := fio2.writeAt(ctx, data, 0); err != nil {
		t.Fatalf("writeAt: %v", err)
	}
	buf := make([]byte, len(data))
	if _, err := fio2.readAt(ctx, buf, 0); err != nil {
		t.Fatalf("readAt: %v", err)
	}
	if string(buf) != string(data) {
		t.Fatalf("read mismatch got %s", string(buf))
	}
	if err := fio2.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Cover constructor path where DirectIOSim is nil (default NewDirectIO) and isEOF helper.
	DirectIOSim = nil
	fio3 := newFileDirectIO()
	if fio3.directIO == nil {
		t.Fatalf("expected default directIO implementation")
	}
	if !fio3.isEOF(io.EOF) {
		t.Fatalf("isEOF should return true for io.EOF")
	}
	if fio3.isEOF(fmt.Errorf("x")) {
		t.Fatalf("isEOF should be false for non EOF")
	}
	// fileExists false branch and getFileSize error path.
	missing := filepath.Join(t.TempDir(), "does_not_exist.bin")
	if fio3.fileExists(missing) {
		t.Fatalf("expected fileExists false for missing path")
	}
	if _, err := fio3.getFileSize(missing); err == nil {
		t.Fatalf("expected error from getFileSize on missing file")
	}
}

// Cover blobStore.Update simple delegation + Add early return when empty slice.
func TestBlobStoreUpdateDelegatesAndAddEarlyReturn(t *testing.T) {
	ctx := context.Background()
	bs := NewBlobStore(nil, nil)
	// Add early return on empty.
	if err := bs.Add(ctx, nil); err != nil {
		t.Fatalf("Add empty should return nil: %v", err)
	}
	id := sop.NewUUID()
	dir := t.TempDir()
	payload := []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: dir, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: []byte("x")}}}}
	if err := bs.Update(ctx, payload); err != nil {
		t.Fatalf("Update delegates to Add: %v", err)
	}
}
