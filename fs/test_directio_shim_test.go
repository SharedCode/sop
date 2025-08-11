package fs

import (
    "context"
    "os"
    "path/filepath"
)

// stdDirectIO is a minimal DirectIO implementation for tests that uses the
// standard library without special direct I/O flags. It also ensures parent
// directories exist on Open.
type stdDirectIO struct{}

func (dio stdDirectIO) Open(ctx context.Context, filename string, flag int, permission os.FileMode) (*os.File, error) {
    // Ensure parent directories exist for nested paths used by registry segments.
    if dir := filepath.Dir(filename); dir != "." { // tolerate relative names too
        _ = os.MkdirAll(dir, permission)
    }
    return os.OpenFile(filename, flag, permission)
}

func (dio stdDirectIO) WriteAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
    return file.WriteAt(block, offset)
}

func (dio stdDirectIO) ReadAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
    return file.ReadAt(block, offset)
}

func (dio stdDirectIO) Close(file *os.File) error { return file.Close() }

// Initialize the test shim: force filesystem code to use stdDirectIO during tests.
func init() {
    DirectIOSim = stdDirectIO{}
}
