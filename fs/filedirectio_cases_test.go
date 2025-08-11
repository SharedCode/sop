package fs

import (
    "context"
    "errors"
    "os"
    "path/filepath"
    "testing"
)

// errorDirectIO simulates failures on specific operations.
type errorDirectIO struct{
    failOpen bool
    failWrite bool
    failRead bool
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

func TestFileDirectIOOpenGuard(t *testing.T) {
    old := DirectIOSim
    DirectIOSim = stdDirectIO{}
    defer func(){ DirectIOSim = old }()

    d := newFileDirectIO()
    tmp := filepath.Join(t.TempDir(), "f1.dat")
    if err := d.open(context.Background(), tmp, os.O_RDWR|os.O_CREATE, 0o644); err != nil {
        t.Fatalf("first open failed: %v", err)
    }
    if err := d.open(context.Background(), tmp, os.O_RDWR, 0o644); err == nil {
        t.Fatalf("expected error on second open, got nil")
    }
    _ = d.close()
}

func TestFileDirectIOReadWriteErrors(t *testing.T) {
    old := DirectIOSim
    DirectIOSim = errorDirectIO{failWrite:true}
    defer func(){ DirectIOSim = old }()

    d := newFileDirectIO()
    tmp := filepath.Join(t.TempDir(), "f2.dat")
    if err := d.open(context.Background(), tmp, os.O_RDWR|os.O_CREATE, 0o644); err != nil {
        t.Fatalf("open failed: %v", err)
    }
    // Unaligned small buffer is fine with std files in tests; we test error path from our shim
    if _, err := d.writeAt(context.Background(), []byte("abc"), 0); err == nil {
        t.Fatalf("expected write error, got nil")
    }
    _ = d.close()
}

func TestFileDirectIOEOFAndHelpers(t *testing.T) {
    old := DirectIOSim
    DirectIOSim = stdDirectIO{}
    defer func(){ DirectIOSim = old }()

    d := newFileDirectIO()
    tmp := filepath.Join(t.TempDir(), "f3.dat")
    if err := d.open(context.Background(), tmp, os.O_RDWR|os.O_CREATE, 0o644); err != nil {
        t.Fatalf("open failed: %v", err)
    }
    // fileExists should see the file
    if !d.fileExists(tmp) {
        t.Fatalf("fileExists returned false for existing file")
    }
    // getFileSize should be 0 for a new file
    if sz, err := d.getFileSize(tmp); err != nil || sz != 0 {
        t.Fatalf("getFileSize got (%d,%v), want (0,nil)", sz, err)
    }
    // readAt on empty file returns 0, EOF from std library; check isEOF helper
    buf := make([]byte, 4)
    n, err := d.readAt(context.Background(), buf, 0)
    if err == nil {
        t.Fatalf("expected EOF on empty read, got nil")
    }
    if n != 0 || !d.isEOF(err) {
        t.Fatalf("expected (0, EOF), got (%d, %v)", n, err)
    }
    _ = d.close()
}
