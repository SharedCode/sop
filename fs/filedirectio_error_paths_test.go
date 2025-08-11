package fs

import (
    "context"
    "os"
    "path/filepath"
    "testing"
)

// testDIO is a minimal DirectIO fake backed by os.* to exercise fileDirectIO paths.
type testDIO struct{}

func (d testDIO) Open(_ context.Context, filename string, flag int, perm os.FileMode) (*os.File, error) {
    return os.OpenFile(filename, flag, perm)
}
func (d testDIO) WriteAt(_ context.Context, f *os.File, b []byte, off int64) (int, error) { return f.WriteAt(b, off) }
func (d testDIO) ReadAt(_ context.Context, f *os.File, b []byte, off int64) (int, error)  { return f.ReadAt(b, off) }
func (d testDIO) Close(f *os.File) error { return f.Close() }

func TestFileDirectIO_ErrorPaths(t *testing.T) {
    ctx := context.Background()
    dir := t.TempDir()
    path := filepath.Join(dir, "f.dat")

    // Inject fake DirectIO
    old := DirectIOSim
    DirectIOSim = testDIO{}
    defer func(){ DirectIOSim = old }()

    // write/read without open should error
    d := newFileDirectIO()
    if _, err := d.writeAt(ctx, []byte("x"), 0); err == nil {
        t.Fatalf("expected writeAt error without open")
    }
    buf := make([]byte, 1)
    if _, err := d.readAt(ctx, buf, 0); err == nil {
        t.Fatalf("expected readAt error without open")
    }

    // open twice should error
    if err := d.open(ctx, path, os.O_CREATE|os.O_RDWR, 0o644); err != nil {
        t.Fatalf("open: %v", err)
    }
    if err := d.open(ctx, path, os.O_CREATE|os.O_RDWR, 0o644); err == nil {
        t.Fatalf("expected second open to fail")
    }
    if err := d.close(); err != nil { t.Fatalf("close: %v", err) }
}
