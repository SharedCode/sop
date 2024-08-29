package fs

import (
	"context"

	"github.com/SharedCode/sop"
)

type manageBlobStoreFolder struct {
	fileIO FileIO
}

func NewManageBlobStoreFolder(fileIO FileIO) sop.ManageBlobStore {
	return &manageBlobStoreFolder{
		fileIO: fileIO,
	}
}

var umaskCalled bool

func (bf *manageBlobStoreFolder) CreateBlobStore(ctx context.Context, blobStoreBaseFolderPath string) error {
	// Create a new Blob store base folder.
	return bf.fileIO.MkdirAll(blobStoreBaseFolderPath, permission)
}

func (bf *manageBlobStoreFolder) RemoveBlobStore(ctx context.Context, blobStoreBaseFolderPath string) error {
	// Remove the Blob store base folder all sub-directories & their files will be removed.
	return bf.fileIO.RemoveAll(blobStoreBaseFolderPath)
}
