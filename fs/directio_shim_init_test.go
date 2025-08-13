package fs

import (
    "context"
    "os"
    "path/filepath"
)

// directIOShim reintroduces the removed test shim behavior: ensure parent
// directories exist on Open so registry / replication tests that rely on
// nested segment paths succeed without each test manually creating them.
// This keeps consolidation of DirectIO tests while preserving expected setup.
type directIOShim struct{}

func (dio directIOShim) Open(ctx context.Context, filename string, flag int, perm os.FileMode) (*os.File, error) {
    if dir := filepath.Dir(filename); dir != "." { _ = os.MkdirAll(dir, perm) }
    return os.OpenFile(filename, flag, perm)
}
func (dio directIOShim) WriteAt(ctx context.Context, f *os.File, b []byte, off int64) (int, error) { return f.WriteAt(b, off) }
func (dio directIOShim) ReadAt(ctx context.Context, f *os.File, b []byte, off int64) (int, error)  { return f.ReadAt(b, off) }
func (dio directIOShim) Close(f *os.File) error { return f.Close() }

// Initialize once for the test process; don't override if a test already set a custom DirectIOSim.
func init() {
    if DirectIOSim == nil {
        DirectIOSim = directIOShim{}
    }
}
