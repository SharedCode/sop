package fs

import (
	"context"
	"fmt"
	"os"

	"github.com/SharedCode/sop"
)

// * File size.
// * The number of data/parity shards.
// * Order of the shards.

// * HASH of each shard.

// BlobStore has no caching built in because blobs are huge, caller code can apply caching on top of it.
type blobStore struct {
	toFilePath ToFilePathFunc
	fileIO     FileIO
}

// Directory/File permission.
const permission os.FileMode = os.ModeSticky | os.ModePerm

// NewBlobStoreUsingDefaults is synonymous to NewBlobStore but uses default implementations of
// necessary parameter interfaces like for file IO, to file path formatter.
func NewBlobStore() sop.BlobStore {
	return NewBlobStoreExt(ToFilePath, DefaultFileIO{})
}

// NewBlobStore instantiates a new blobstore for File System storage.
// Parameters are specified for abstractions to things like File IO, filename formatter for efficient storage
// and access of files on directories.
func NewBlobStoreExt(
	toFilePathFunc ToFilePathFunc,
	fileIO FileIO) sop.BlobStore {
	return &blobStore{
		toFilePath: toFilePathFunc,
		fileIO:     fileIO,
	}
}

func (b *blobStore) GetOne(ctx context.Context, blobFilePath string, blobID sop.UUID) ([]byte, error) {
	fp := b.toFilePath(blobFilePath, blobID)
	fn := fmt.Sprintf("%s%c%s", fp, os.PathSeparator, blobID.String())
	ba, err := b.fileIO.ReadFile(fn)
	if err != nil {
		return nil, err
	}
	return ba, nil
}

func (b *blobStore) Add(ctx context.Context, storesblobs ...sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	for _, storeBlobs := range storesblobs {
		for _, blob := range storeBlobs.Blobs {
			ba := blob.Value
			fp := b.toFilePath(storeBlobs.BlobTable, blob.Key)
			if !b.fileIO.Exists(fp) {
				if err := b.fileIO.MkdirAll(fp, permission); err != nil {
					return err
				}
			}
			// WriteFile will add or replace existing file. 666 - gives R/W permission to everybody.
			fn := fmt.Sprintf("%s%c%s", fp, os.PathSeparator, blob.Key.String())
			if err := b.fileIO.WriteFile(fn, ba, permission); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *blobStore) Update(ctx context.Context, storesblobs ...sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	return b.Add(ctx, storesblobs...)
}

func (b *blobStore) Remove(ctx context.Context, storesBlobsIDs ...sop.BlobsPayload[sop.UUID]) error {
	for _, storeBlobIDs := range storesBlobsIDs {
		for _, blobID := range storeBlobIDs.Blobs {
			fp := b.toFilePath(storeBlobIDs.BlobTable, blobID)
			fn := fmt.Sprintf("%s%c%s", fp, os.PathSeparator, blobID.String())
			// Ok if file does not exist to return nil as it it was successfully removed.
			if !b.fileIO.Exists(fn) {
				return nil
			}
			if err := b.fileIO.Remove(fn); err != nil {
				return err
			}
		}
	}
	return nil
}
