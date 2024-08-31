package fs

import (
	"context"
	"fmt"
	"os"

	"github.com/SharedCode/sop"
)

// BlobStore has no caching built in because blobs are huge, caller code can apply caching on top of it.
type blobStore struct {
	toFilePath ToFilePathFunc
	fileIO     FileIO
	marshaler  sop.Marshaler
}

// Directory/File permission.
const permission os.FileMode = os.ModeSticky | os.ModePerm

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
	fp := b.toFilePath(blobFilePath, blobID)
	fn := fmt.Sprintf("%s%c%s", fp, os.PathSeparator, blobID.ToString())
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
			fp := b.toFilePath(storeBlobs.BlobTable, blob.Key)
			if !b.fileIO.Exists(fp) {
				if err := b.fileIO.MkdirAll(fp, permission); err != nil {
					return err
				}
			}
			// WriteFile will add or replace existing file. 666 - gives R/W permission to everybody.
			fn := fmt.Sprintf("%s%c%s", fp, os.PathSeparator, blob.Key.ToString())
			if err = b.fileIO.WriteFile(fn, ba, permission); err != nil {
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
			fp := b.toFilePath(storeBlobIDs.BlobTable, blobID)
			fn := fmt.Sprintf("%s%c%s", fp, os.PathSeparator, blobID.ToString())
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
