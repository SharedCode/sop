package fs

import (
	"fmt"
	"os"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_red_cfs/fs/erasure"
)

// ErasureCoding contains the Erasure object used for EC encoding & decoding and the base folder
// paths to be used where to store data & parity shards files.
type ErasureCoding struct {
	erasure                     *erasure.Erasure
	baseFolderPathsAcrossDrives []string
	repairCorruptedShards       bool
}

// NewErasureCoding instantiates a new Erasure Coding object for use in providing data replication.
func NewErasureCoding(baseFolderPaths []string, dataShardsCount int, parityShardsCount int, repairCorruptedShards bool) (*ErasureCoding, error) {
	if len(baseFolderPaths) != dataShardsCount+parityShardsCount {
		return nil, fmt.Errorf("baseFolderPaths array elements count should match the sum of dataShardsCount & parityShardsCount")
	}

	ec, err := erasure.NewErasure(dataShardsCount, parityShardsCount)
	if err != nil {
		return nil, err
	}
	return &ErasureCoding{
		baseFolderPathsAcrossDrives: baseFolderPaths,
		erasure:                     ec,
		repairCorruptedShards:       repairCorruptedShards,
	}, nil
}

func (ec *ErasureCoding) ToFilePath(basePath string, id sop.UUID) string {
	return ""
}

func (ec *ErasureCoding) WriteFile(filename string, contents []byte, perm os.FileMode) error {
	return nil
}
func (ec *ErasureCoding) ReadFile(filename string) ([]byte, error) {

	return nil, nil
}

func (ec *ErasureCoding) Remove(name string) error {
	return nil
}
func (ec *ErasureCoding) Exists(path string) bool {
	return false
}

// Directory API.
func (ec *ErasureCoding) RemoveAll(path string) error {
	return nil
}
func (ec *ErasureCoding) MkdirAll(path string, perm os.FileMode) error {
	return nil
}
