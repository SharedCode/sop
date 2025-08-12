package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
)

// errFileIO is a stub FileIO used to force specific failure branches inside blobstore.go.
type errFileIO struct {
	failMkdir    bool
	failWrite    bool
	failRead     bool
	failRemove   bool
	existsAlways bool
}

func (e *errFileIO) WriteFile(ctx context.Context, name string, data []byte, perm os.FileMode) error {
	if e.failWrite {
		return errors.New("write failure")
	}
	// emulate directory existing by creating parent silently when not failing
	_ = os.MkdirAll(filepath.Dir(name), 0o755)
	return os.WriteFile(name, data, perm)
}
func (e *errFileIO) ReadFile(ctx context.Context, name string) ([]byte, error) {
	if e.failRead {
		return nil, errors.New("read failure")
	}
	return os.ReadFile(name)
}
func (e *errFileIO) Remove(ctx context.Context, name string) error {
	if e.failRemove {
		return errors.New("remove failure")
	}
	return os.Remove(name)
}
func (e *errFileIO) Exists(ctx context.Context, path string) bool     { return e.existsAlways }
func (e *errFileIO) RemoveAll(ctx context.Context, path string) error { return os.RemoveAll(path) }
func (e *errFileIO) MkdirAll(ctx context.Context, path string, perm os.FileMode) error {
	if e.failMkdir {
		return errors.New("mkdir failure")
	}
	return os.MkdirAll(path, perm)
}
func (e *errFileIO) ReadDir(ctx context.Context, sourceDir string) ([]os.DirEntry, error) {
	return nil, nil
}

// TestBlobStore_ErrorBranches exercises error return paths in Add, GetOne and Remove.
func TestBlobStore_ErrorBranches(t *testing.T) {
	ctx := context.Background()
	id := sop.NewUUID()
	base := t.TempDir()
	table := filepath.Join(base, "tbl")

	tests := []struct {
		name    string
		setupIO func() FileIO
		op      func(ctx context.Context, bs sop.BlobStore) error
		wantErr string
	}{
		{ // MkdirAll failure branch
			name:    "add mkdir error",
			setupIO: func() FileIO { return &errFileIO{failMkdir: true} },
			op: func(ctx context.Context, bs sop.BlobStore) error {
				return bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: table, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: []byte("x")}}}})
			},
			wantErr: "mkdir failure",
		},
		{ // WriteFile failure branch after successful MkdirAll
			name:    "add write error",
			setupIO: func() FileIO { return &errFileIO{failWrite: true} },
			op: func(ctx context.Context, bs sop.BlobStore) error {
				return bs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: table, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: []byte("x")}}}})
			},
			wantErr: "write failure",
		},
		{ // GetOne read error path
			name:    "getone read error",
			setupIO: func() FileIO { return &errFileIO{failRead: true} },
			op: func(ctx context.Context, bs sop.BlobStore) error {
				_, err := bs.GetOne(ctx, table, id)
				return err
			},
			wantErr: "read failure",
		},
		{ // Remove error path when Remove fails (Exists must be true to reach Remove branch)
			name:    "remove error",
			setupIO: func() FileIO { return &errFileIO{failRemove: true, existsAlways: true} },
			op: func(ctx context.Context, bs sop.BlobStore) error {
				return bs.Remove(ctx, []sop.BlobsPayload[sop.UUID]{{BlobTable: table, Blobs: []sop.UUID{id}}})
			},
			wantErr: "remove failure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bs := NewBlobStore(nil, tt.setupIO())
			err := tt.op(ctx, bs)
			if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("expected error %q got %v", tt.wantErr, err)
			}
		})
	}
}
