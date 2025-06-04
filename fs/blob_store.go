package fs

import (
	"context"
	"fmt"
	"os"

	"github.com/SharedCode/sop"
)

// BlobStore has no caching built in because blobs are huge, caller code can apply caching on top of it.
type blobStore struct {
	fileIO FileIO
	toFilePath ToFilePathFunc
}

// Directory/File permission.
const permission os.FileMode = os.ModeSticky | os.ModePerm

// NewBlobStore instantiates a new blobstore for File System storage.
func NewBlobStore(toFilePath ToFilePathFunc, fileIO FileIO) sop.BlobStore {
	if fileIO == nil {
		fileIO = NewDefaultFileIO(DefaultToFilePath)
	}
	if toFilePath == nil {
		toFilePath = DefaultToFilePath
	}
	return &blobStore{
		fileIO: fileIO,
		toFilePath: toFilePath,
	}
}

func (b blobStore) GetOne(ctx context.Context, blobFilePath string, blobID sop.UUID) ([]byte, error) {
	fp := b.toFilePath(blobFilePath, blobID)
	fn := fmt.Sprintf("%s%c%s", fp, os.PathSeparator, blobID.String())
	ba, err := b.fileIO.ReadFile(fn)
	if err != nil {
		return nil, err
	}
	return ba, nil
}

func (b blobStore) Add(ctx context.Context, storesblobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	for _, storeBlobs := range storesblobs {
		for _, blob := range storeBlobs.Blobs {
			ba := blob.Value
			fp := b.toFilePath(storeBlobs.BlobTable, blob.Key)
			if !b.fileIO.Exists(fp) {
				if err := b.fileIO.MkdirAll(fp, permission); err != nil {
					return err
				}
			}
			fn := fmt.Sprintf("%s%c%s", fp, os.PathSeparator, blob.Key.String())
			if err := b.fileIO.WriteFile(fn, ba, permission); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b blobStore) Update(ctx context.Context, storesblobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	return b.Add(ctx, storesblobs)
}

func (b blobStore) Remove(ctx context.Context, storesBlobsIDs []sop.BlobsPayload[sop.UUID]) error {
	for _, storeBlobIDs := range storesBlobsIDs {
		for _, blobID := range storeBlobIDs.Blobs {
			fp := b.toFilePath(storeBlobIDs.BlobTable, blobID)
			fn := fmt.Sprintf("%s%c%s", fp, os.PathSeparator, blobID.String())
			// Do nothing if file already not existent.
			if !b.fileIO.Exists(fn) {
				continue
			}

			if err := b.fileIO.Remove(fn); err != nil {
				return err
			}
		}
	}
	return nil
}
