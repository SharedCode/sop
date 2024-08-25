package fs

import (
	"context"
	"os"

	"github.com/SharedCode/sop"
)

// Marshaler allows you to specify custom marshaler if needed. Defaults to the SOP default(Golang's encoding's Marshal API) marshaler.
var Marshaler sop.Marshaler = sop.NewMarshaler()

type blobStore struct {
}

// NewBlobStore instantiates a new (mocked) blobstore.
func NewBlobStore() sop.BlobStore {
	return &blobStore{}
}

func (b *blobStore) GetOne(ctx context.Context, blobFilePath string, blobID sop.UUID, target interface{}) error {
	fn:= FilenameFormatter(blobFilePath, blobID)
	ba, err := os.ReadFile(fn)
	if err != nil {
		return err
	}
	return Marshaler.Unmarshal(ba, target)
}

func (b *blobStore) Add(ctx context.Context, storesblobs ...sop.BlobsPayload[sop.KeyValuePair[sop.UUID, interface{}]]) error {
	for _, storeBlobs := range storesblobs {
		for _, blob := range storeBlobs.Blobs {
			ba, err := Marshaler.Marshal(blob.Value)
			if err != nil {
				return err
			}
			fn := FilenameFormatter(storeBlobs.BlobTable, blob.Key)
			// WriteFile will add or replace existing file.
			err = os.WriteFile(fn, ba, os.ModeAppend)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *blobStore) Update(ctx context.Context, storesblobs ...sop.BlobsPayload[sop.KeyValuePair[sop.UUID, interface{}]]) error {
	return b.Add(ctx, storesblobs...)
}

func (b *blobStore) Remove(ctx context.Context, storesBlobsIDs ...sop.BlobsPayload[sop.UUID]) error {
	for _, storeBlobIDs := range storesBlobsIDs {
		for _, blobID := range storeBlobIDs.Blobs {
			fn := FilenameFormatter(storeBlobIDs.BlobTable, blobID)
			err := os.Remove(fn)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
