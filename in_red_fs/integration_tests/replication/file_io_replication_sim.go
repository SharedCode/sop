package replication

import (
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

func (sim *fileIOReplicationSimulator) WriteFile(name string, data []byte, perm os.FileMode) error {
	return nil
}
func (sim *fileIOReplicationSimulator) ReadFile(name string) ([]byte, error) {
	return nil, nil
}
func (sim *fileIOReplicationSimulator) Remove(name string) error {
	return nil
}
func (sim *fileIOReplicationSimulator) Exists(path string) bool {
	return true
}

// Directory API.
func (sim *fileIOReplicationSimulator) RemoveAll(path string) error {
	return nil
}
func (sim *fileIOReplicationSimulator) MkdirAll(path string, perm os.FileMode) error {
	return nil
}
