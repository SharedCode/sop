package fs

import (
	"context"

	"github.com/SharedCode/sop"
)

type manageStoreFolder struct {
	fileIO FileIO
}

// Manage store(s) folder.
func NewManageStoreFolder(fileIO FileIO) sop.ManageStore {
	return &manageStoreFolder{
		fileIO: fileIO,
	}
}

// Create a new store(s) base folder.
func (bf *manageStoreFolder) CreateStore(ctx context.Context, blobStoreBaseFolderPath string) error {
	return bf.fileIO.MkdirAll(blobStoreBaseFolderPath, permission)
}

// Remove the store(s) base folder all sub-directories & their files will be removed.
func (bf *manageStoreFolder) RemoveStore(ctx context.Context, blobStoreBaseFolderPath string) error {
	return bf.fileIO.RemoveAll(blobStoreBaseFolderPath)
}
