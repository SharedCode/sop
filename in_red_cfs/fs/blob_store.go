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
	bs, _ := newBlobStoreExt(fileIO, nil)
	return bs
}

// NewBlobStore instantiates a new blobstore for File System storage.
// Parameters are specified for abstractions to things like File IO & erasure config (EC) including filename
// formatter for efficient storage and access of files across directories.
//
// erasureConfig param allows code the flexibility to specify different EC & base folder paths across disk drives
// per blob Table, if needed. It is a map which has blob table name as key and EC as value.
// You can use the empty string ("") key as default, used like a fallback, if EC is not found for a given blob table name.
// SOP will use that entry's EC. Also, you are free to use the same EC for a given set of keys, thus, sharing the same
// disk drives and base folders is supported.
//
// If erasureConfig is nil, SOP will attempt to use the value assigned in 'globalErasureConfig', so, you can optionally
// pass in nil if your app had assigned the (global) erasureConfig using the SetGlobalErasureConfig helper function.
func NewBlobStoreExt(fileIO FileIO, erasureConfig map[string]ErasureCodingConfig) (sop.BlobStore, error) {
	if erasureConfig == nil {
		erasureConfig = globalErasureConfig
	}
	return newBlobStoreExt(fileIO, erasureConfig)
}

func newBlobStoreExt(fileIO FileIO, erasureConfig map[string]ErasureCodingConfig) (sop.BlobStore, error) {
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
