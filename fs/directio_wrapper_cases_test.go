package fs

import (
    "context"
    "os"
    "testing"

    "github.com/ncw/directio"
)

func TestDirectIOOpenWriteReadClose(t *testing.T) {
    ctx := context.Background()
    dio := NewDirectIO()

    // create aligned buffer
    buf := directio.AlignedBlock(blockSize)
    copy(buf, []byte("hello"))

    // use a temp file path
    f, err := dio.Open(ctx, os.TempDir()+string(os.PathSeparator)+"directio_test.bin", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
    if err != nil {
        t.Fatalf("Open: %v", err)
    }
    defer func() { _ = os.Remove(f.Name()) }()

    // write and read back
    if _, err := dio.WriteAt(ctx, f, buf, 0); err != nil {
        t.Fatalf("WriteAt: %v", err)
    }

    rbuf := directio.AlignedBlock(blockSize)
    if _, err := dio.ReadAt(ctx, f, rbuf, 0); err != nil {
        t.Fatalf("ReadAt: %v", err)
    }

    if string(rbuf[:5]) != "hello" {
        t.Fatalf("unexpected content: %q", string(rbuf[:5]))
    }

    if err := dio.Close(f); err != nil {
        t.Fatalf("Close: %v", err)
    }
}
