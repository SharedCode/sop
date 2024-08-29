package fs

import (
	"context"

	"github.com/SharedCode/sop"
)

// BlobStore has no caching built in because blobs are huge, caller code can apply caching on top of it.
type blobStore struct {
	toFilePath ToFilePathFunc
	fileIO     FileIO
	marshaler  sop.Marshaler
}

// NewBlobStoreUsingDefaults is synonymous to NewBlobStore but uses default implementations of
// necessary parameter interfaces like for file IO, to file path formatter & object marshaler.
func NewBlobStoreUsingDefaults() sop.BlobStore {
	return NewBlobStore(ToFilePath, DefaultFileIO{}, sop.NewMarshaler())
}

// NewBlobStore instantiates a new blobstore for File System storage.
// Parameters are specified for abstractions to things like File IO, filename formatter for efficient storage
// and access of files on directories & marshaler.
func NewBlobStore(
	toFilePathFunc ToFilePathFunc,
	fileIO FileIO,
	marshaler sop.Marshaler) sop.BlobStore {
	return &blobStore{
		toFilePath: toFilePathFunc,
		fileIO:     fileIO,
		marshaler:  marshaler,
	}
}

func (b *blobStore) GetOne(ctx context.Context, blobFilePath string, blobID sop.UUID, target interface{}) error {
	fn := b.toFilePath(blobFilePath, blobID)
	ba, err := b.fileIO.ReadFile(fn)
	if err != nil {
		return err
	}
	return b.marshaler.Unmarshal(ba, target)
}

func (b *blobStore) Add(ctx context.Context, storesblobs ...sop.BlobsPayload[sop.KeyValuePair[sop.UUID, interface{}]]) error {
	for _, storeBlobs := range storesblobs {
		for _, blob := range storeBlobs.Blobs {
			ba, err := b.marshaler.Marshal(blob.Value)
			if err != nil {
				return err
			}
			fn := b.toFilePath(storeBlobs.BlobTable, blob.Key)
			// WriteFile will add or replace existing file. 666 - gives R/W permission to everybody.
			err = b.fileIO.WriteFile(fn, ba, 0666)
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
			fn := b.toFilePath(storeBlobIDs.BlobTable, blobID)
			err := b.fileIO.Remove(fn)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
