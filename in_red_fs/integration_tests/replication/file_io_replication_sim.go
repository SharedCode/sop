package replication

import (
	"context"
	"os"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/fs"
)

type fileIOReplicationSimulator struct {
}

func newFileIOReplicationSim() fs.FileIO {
	return &fileIOReplicationSimulator{}
}

func (sim *fileIOReplicationSimulator) ToFilePath(basePath string, id sop.UUID) string {
	return ""
}

func (sim *fileIOReplicationSimulator) WriteFile(ctx context.Context, name string, data []byte, perm os.FileMode) error {
	return nil
}
func (sim *fileIOReplicationSimulator) ReadFile(ctx context.Context, name string) ([]byte, error) {
	return nil, nil
}
func (sim *fileIOReplicationSimulator) Remove(ctx context.Context, name string) error {
	return nil
}
func (sim *fileIOReplicationSimulator) Exists(ctx context.Context, path string) bool {
	return true
}

// Directory API.
func (sim *fileIOReplicationSimulator) RemoveAll(ctx context.Context, path string) error {
	return nil
}
func (sim *fileIOReplicationSimulator) MkdirAll(ctx context.Context, path string, perm os.FileMode) error {
	return nil
}

func (sim *fileIOReplicationSimulator) ReadDir(ctx context.Context, sourceDir string) ([]os.DirEntry, error) {
	return nil, nil
}
