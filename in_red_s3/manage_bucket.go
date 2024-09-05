package in_red_s3

import (
	"context"

	"github.com/SharedCode/sop"
)

type manageBlobStoreFolder struct {
}

func NewManageBlobStoreFolder() sop.ManageBlobStore {
	return &manageBlobStoreFolder{
	}
}

func (bf *manageBlobStoreFolder) CreateBlobStore(ctx context.Context, blobStoreBaseFolderPath string) error {
	return nil
}

func (bf *manageBlobStoreFolder) RemoveBlobStore(ctx context.Context, blobStoreBaseFolderPath string) error {
	return nil
}
