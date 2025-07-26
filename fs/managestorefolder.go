package fs

import (
	"context"

	"github.com/sharedcode/sop"
)

type manageStoreFolder struct {
	fileIO FileIO
}

// Manage store(s) folder.
func NewManageStoreFolder(fileIO FileIO) sop.ManageStore {
	if fileIO == nil {
		fileIO = NewFileIO()
	}
	return &manageStoreFolder{
		fileIO: fileIO,
	}
}

// Create a new store(s) base folder.
func (bf *manageStoreFolder) CreateStore(ctx context.Context, blobStoreBaseFolderPath string) error {
	return bf.fileIO.MkdirAll(ctx, blobStoreBaseFolderPath, permission)
}

// Remove the store(s) base folder all sub-directories & their files will be removed.
func (bf *manageStoreFolder) RemoveStore(ctx context.Context, blobStoreBaseFolderPath string) error {
	return bf.fileIO.RemoveAll(ctx, blobStoreBaseFolderPath)
}
