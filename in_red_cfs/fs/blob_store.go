package fs

import (
	"context"
	"fmt"
	"os"

	"github.com/SharedCode/sop"
)

// BlobStore has no caching built in because blobs are huge, caller code can apply caching on top of it.
type blobStore struct {
	fileIO        FileIO
	erasureCoding *ErasureCoding
}

// Directory/File permission.
const permission os.FileMode = os.ModeSticky | os.ModePerm

// NewBlobStoreUsingDefaults is synonymous to NewBlobStore but uses default implementations of
// necessary parameter interfaces like for file IO, to file path formatter.
func NewBlobStore() sop.BlobStore {
	return NewBlobStoreExt(nil, nil)
}

// NewBlobStore instantiates a new blobstore for File System storage.
// Parameters are specified for abstractions to things like File IO, filename formatter for efficient storage
// and access of files on directories.
func NewBlobStoreExt(fileIO FileIO, ec *ErasureCoding) sop.BlobStore {
	// If ec is supplied, override with Erasure Coding (FileIO & ToFilePath) implementations.
	if ec != nil {
		fileIO = ec
	}

	return &blobStore{
		fileIO:        fileIO,
		erasureCoding: ec,
	}
}

func (b *blobStore) GetOne(ctx context.Context, blobFilePath string, blobID sop.UUID) ([]byte, error) {
	fp := b.fileIO.ToFilePath(blobFilePath, blobID)
	fn := fmt.Sprintf("%s%c%s", fp, os.PathSeparator, blobID.String())
	ba, err := b.fileIO.ReadFile(fn)
	if err != nil {
		return nil, err
	}
	return ba, nil
}

func (b *blobStore) Add(ctx context.Context, storesblobs ...sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {

	// Spin up a job processor of 5 tasks (threads) maximum.
	tc, eg := sop.JobProcessor(ctx, 5)

	for _, storeBlobs := range storesblobs {
		for _, blob := range storeBlobs.Blobs {
			ba := blob.Value
			fp := b.fileIO.ToFilePath(storeBlobs.BlobTable, blob.Key)
			if !b.fileIO.Exists(fp) {
				if err := b.fileIO.MkdirAll(fp, permission); err != nil {
					return err
				}
			}
			fn := fmt.Sprintf("%s%c%s", fp, os.PathSeparator, blob.Key.String())

			// Task WriteFile will add or replace existing file.
			task := func() error {
				if err := b.fileIO.WriteFile(fn, ba, permission); err != nil {
					return err
				}
				return nil
			}
			tc <- task
		}
	}
	close(tc)
	return eg.Wait()
}

func (b *blobStore) Update(ctx context.Context, storesblobs ...sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	return b.Add(ctx, storesblobs...)
}

func (b *blobStore) Remove(ctx context.Context, storesBlobsIDs ...sop.BlobsPayload[sop.UUID]) error {

	// Spin up a job processor of 5 tasks (threads) maximum.
	tc, eg := sop.JobProcessor(ctx, 5)

	for _, storeBlobIDs := range storesBlobsIDs {
		for _, blobID := range storeBlobIDs.Blobs {
			fp := b.fileIO.ToFilePath(storeBlobIDs.BlobTable, blobID)
			fn := fmt.Sprintf("%s%c%s", fp, os.PathSeparator, blobID.String())
			// Do nothing if file already not existent.
			if !b.fileIO.Exists(fn) {
				continue
			}

			// Task Remove will delete existing file (or files if using Erasure Coding).
			task := func() error {
				if err := b.fileIO.Remove(fn); err != nil {
					return err
				}
				return nil
			}
			tc <- task
		}
	}
	close(tc)
	return eg.Wait()
}
