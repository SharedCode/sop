package fs

import (
	"context"
	"fmt"
	"os"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_red_cfs/fs/erasure"
)

// BlobStore has no caching built in because blobs are huge, caller code can apply caching on top of it.
type blobStore struct {
	fileIO                      FileIO
	erasure                     map[string]*erasure.Erasure
	baseFolderPathsAcrossDrives map[string][]string
	repairCorruptedShards       bool
}

// Directory/File permission.
const permission os.FileMode = os.ModeSticky | os.ModePerm

// NewBlobStoreUsingDefaults is synonymous to NewBlobStore but uses default implementations of
// necessary parameter interfaces like for file IO, to file path formatter.
func NewBlobStore(fileIO FileIO) sop.BlobStore {
	bs, _ := NewBlobStoreExt(fileIO, nil)
	return bs
}

// NewBlobStore instantiates a new blobstore for File System storage.
// Parameters are specified for abstractions to things like File IO, filename formatter for efficient storage
// and access of files on directories.
func NewBlobStoreExt(fileIO FileIO, erasureConfig map[string]ErasureCodingConfig) (sop.BlobStore, error) {
	var e map[string]*erasure.Erasure
	var baseFolderPathsAcrossDrives map[string][]string
	var repairCorruptedShards bool

	if erasureConfig != nil {
		e = make(map[string]*erasure.Erasure, len(erasureConfig))
		baseFolderPathsAcrossDrives = make(map[string][]string, len(erasureConfig))
		for k,v := range erasureConfig {
			ec, err := erasure.NewErasure(v.DataShardsCount, v.ParityShardsCount)
			if err != nil {
				return nil, err
			}
			repairCorruptedShards = v.RepairCorruptedShards
			if ec.DataShardsCount()+ec.ParityShardsCount() != len(v.BaseFolderPathsAcrossDrives) {
				return nil, fmt.Errorf("baseFolderPaths array elements count should match the sum of dataShardsCount & parityShardsCount")
			}
			e[k] = ec
			baseFolderPathsAcrossDrives[k] = v.BaseFolderPathsAcrossDrives
		}
	}
	if fileIO == nil {
		fileIO = DefaultFileIO{}
	}
	return &blobStore{
		fileIO:                      fileIO,
		erasure:                     e,
		baseFolderPathsAcrossDrives: baseFolderPathsAcrossDrives,
		repairCorruptedShards:       repairCorruptedShards,
	}, nil
}

func (b *blobStore) GetOne(ctx context.Context, blobFilePath string, blobID sop.UUID) ([]byte, error) {
	if b.isErasureEncoding() {
		return b.ecGetOne(ctx, blobFilePath, blobID)
	}
	fp := b.fileIO.ToFilePath(blobFilePath, blobID)
	fn := fmt.Sprintf("%s%c%s", fp, os.PathSeparator, blobID.String())
	ba, err := b.fileIO.ReadFile(fn)
	if err != nil {
		return nil, err
	}
	return ba, nil
}

func (b *blobStore) Add(ctx context.Context, storesblobs ...sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	if b.isErasureEncoding() {
		return b.ecAdd(ctx, storesblobs...)
	}

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
	if b.isErasureEncoding() {
		return b.ecRemove(ctx, storesBlobsIDs...)
	}
	for _, storeBlobIDs := range storesBlobsIDs {
		for _, blobID := range storeBlobIDs.Blobs {
			fp := b.fileIO.ToFilePath(storeBlobIDs.BlobTable, blobID)
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

func (b *blobStore) isErasureEncoding() bool {
	return b.erasure != nil
}
