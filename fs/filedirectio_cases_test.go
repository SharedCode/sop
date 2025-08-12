package fs

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ncw/directio"
)

// errorDirectIO simulates failures on specific operations.
type errorDirectIO struct {
	failOpen  bool
	failWrite bool
	failRead  bool
}

func (e errorDirectIO) Open(ctx context.Context, filename string, flag int, permission os.FileMode) (*os.File, error) {
	if e.failOpen {
		return nil, errors.New("open failed")
	}
	// Ensure parent exists to avoid unrelated errors
	_ = os.MkdirAll(filepath.Dir(filename), 0o755)
	return os.OpenFile(filename, flag|os.O_CREATE, permission)
}

func (e errorDirectIO) WriteAt(ctx context.Context, f *os.File, block []byte, offset int64) (int, error) {
	if e.failWrite {
		return 0, errors.New("write failed")
	}
	return f.WriteAt(block, offset)
}

func (e errorDirectIO) ReadAt(ctx context.Context, f *os.File, block []byte, offset int64) (int, error) {
	if e.failRead {
		return 0, errors.New("read failed")
	}
	return f.ReadAt(block, offset)
}

func (e errorDirectIO) Close(f *os.File) error { return f.Close() }

// TestFileDirectIO_Coverage consolidates key path coverage for fileDirectIO.
func TestFileDirectIO_Coverage(t *testing.T) {
	// Inject failing writer first.
	old := DirectIOSim
	DirectIOSim = errorDirectIO{failWrite: true}
	defer func() { DirectIOSim = old }()
	d := newFileDirectIO()
	tmp := filepath.Join(t.TempDir(), "seg.dat")
	if _, err := d.writeAt(context.Background(), []byte("x"), 0); err == nil {
		t.Fatalf("expected write before open error")
	}
	if _, err := d.readAt(context.Background(), []byte("x"), 0); err == nil {
		t.Fatalf("expected read before open error")
	}
	if err := d.open(context.Background(), tmp, os.O_RDWR|os.O_CREATE, 0o600); err != nil {
		t.Fatalf("open: %v", err)
	}
	// second open error
	if err := d.open(context.Background(), tmp, os.O_RDWR, 0o600); err == nil {
		t.Fatalf("expected second open error")
	}
	// injected write failure
	if _, err := d.writeAt(context.Background(), []byte("abc"), 0); err == nil {
		t.Fatalf("expected injected write error")
	}
	_ = d.close()

	// Success path
	DirectIOSim = errorDirectIO{} // no failures
	d2 := newFileDirectIO()
	if err := d2.open(context.Background(), tmp, os.O_RDWR, 0o600); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	blk := d2.createAlignedBlock()
	for i := range blk {
		blk[i] = byte(i)
	}
	if n, err := d2.writeAt(context.Background(), blk, 0); err != nil || n != len(blk) {
		t.Fatalf("writeAt: %v n=%d", err, n)
	}
	rb := make([]byte, len(blk))
	if n, err := d2.readAt(context.Background(), rb, 0); err != nil || n != len(rb) {
		t.Fatalf("readAt: %v n=%d", err, n)
	}
	if !d2.fileExists(tmp) {
		t.Fatalf("file should exist")
	}
	if sz, err := d2.getFileSize(tmp); err != nil || sz == 0 {
		t.Fatalf("size err=%v sz=%d", err, sz)
	}
	if !d2.isEOF(io.EOF) {
		t.Fatalf("isEOF expected true")
	}
	if d2.isEOF(errors.New("x")) {
		t.Fatalf("isEOF expected false")
	}
	if err := d2.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := d2.close(); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}

	b1 := d2.createAlignedBlock()
	if len(b1) != directio.BlockSize {
		t.Fatalf("aligned block len=%d", len(b1))
	}
	b2 := d2.createAlignedBlockOfSize(2 * directio.BlockSize)
	if len(b2) != 2*directio.BlockSize {
		t.Fatalf("aligned block2 len=%d", len(b2))
	}
}
