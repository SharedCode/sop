package fs

import (
	"context"
	"fmt"
	"os"
	log "log/slog"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_red_cfs/fs/erasure"
)

const (
	maxThreadCount = 7
)

func (b *blobStore) ecGetOne(ctx context.Context, blobFilePath string, blobID sop.UUID) ([]byte, error) {
	// Spin up a job processor of max thread count (threads) maximum.
	tr := sop.NewTaskRunner(ctx, maxThreadCount)

	// Get the blob table (blobFilePath) specific erasure configuration.
	baseFolderPathsAcrossDrives, erasure := b.ecGetBaseFolderPathsAndErasureConfig(blobFilePath)
	if baseFolderPathsAcrossDrives == nil {
		err := fmt.Errorf("can't find Erasure Config setting for file %s", blobFilePath)
		log.Error(err.Error())
		return nil, err
	}

	shards := make([][]byte, len(baseFolderPathsAcrossDrives))
	shardsWithMetadata := make([][]byte, len(baseFolderPathsAcrossDrives))
	shardsMetaData := make([][]byte, len(baseFolderPathsAcrossDrives))
	var lastErr error

	for i := range baseFolderPathsAcrossDrives {
		baseFolderPath := fmt.Sprintf("%s%c%s", baseFolderPathsAcrossDrives[i], os.PathSeparator, blobFilePath)
		blobKey := blobID

		shardIndex := i
		fp := b.fileIO.ToFilePath(baseFolderPath, blobKey)
		fn := fmt.Sprintf("%s%c%s_%d", fp, os.PathSeparator, blobKey.String(), shardIndex)

		tr.Go(func() error {
			log.Debug(fmt.Sprintf("reading from file %s", fn))

			ba, err := b.fileIO.ReadFile(fn)
			if err != nil {
				lastErr = err
				log.Error("failed reading from file %s, error: %v", fn, err)
				log.Info("if there are enough shards to reconstruct data, 'reader' may still work")
				return nil
			}
			shardsWithMetadata[shardIndex] = ba
			shardsMetaData[shardIndex] = ba[0:erasure.MetaDataSize()]
			shards[shardIndex] = ba[erasure.MetaDataSize():]
			return nil
		})
	}
	if err := tr.Wait(); err != nil {
		return nil, err
	}

	// Just return the (last) error if shards is empty.
	if isShardsEmpty(shards) && lastErr != nil {
		return nil, lastErr
	}
	dr := erasure.Decode(shards, shardsMetaData)
	if dr.Error != nil {
		return nil, dr.Error
	}

	if b.repairCorruptedShards && len(dr.ReconstructedShardsIndeces) > 0 {
		// Repair corrupted or bitrot shards (a.k.a. damaged shards). Just do sequential processing as
		// damaged shards should be typically one, residing in a drive that failed.
		for _, i := range dr.ReconstructedShardsIndeces {
			baseFolderPath := fmt.Sprintf("%s%c%s", baseFolderPathsAcrossDrives[i], os.PathSeparator, blobFilePath)
			blobKey := blobID

			fp := b.fileIO.ToFilePath(baseFolderPath, blobKey)
			fn := fmt.Sprintf("%s%c%s_%d", fp, os.PathSeparator, blobKey.String(), i)

			log.Debug(fmt.Sprintf("repairing file %s", fn))

			md := erasure.ComputeShardMetadata(len(dr.DecodedData), shards, i)
			buf := make([]byte, len(md)+len(shards[i]))

			// TODO: refactor to write metadata then write the shard data so we don't use temp variable,
			// more optimal if shard size is huge.
			copy(buf, md)
			copy(buf[len(md):], shards[i])
			shardsWithMetadata[i] = buf

			err := b.fileIO.WriteFile(fn, shardsWithMetadata[i], permission)
			// Just log warning if damaged shard can't be repaired as we have reconstructed it part of return.
			if err != nil {
				log.Warn(fmt.Sprintf("error encountered repairing a damaged shard (%s)", fn))
			}
		}
	}

	return dr.DecodedData, nil
}

func isShardsEmpty(shards [][]byte) bool {
	for i := range shards {
		if shards[i] != nil {
			return false
		}
	}
	return true
}

func (b *blobStore) ecGetBaseFolderPathsAndErasureConfig(blobTable string) ([]string, *erasure.Erasure) {
	// Get the blob table specific erasure configuration.
	baseFolderPathsAcrossDrives := b.baseFolderPathsAcrossDrives[blobTable]
	erasure := b.erasure[blobTable]

	if baseFolderPathsAcrossDrives == nil {
		baseFolderPathsAcrossDrives = b.baseFolderPathsAcrossDrives[""]
		erasure = b.erasure[""]
	}
	return baseFolderPathsAcrossDrives, erasure
}

func (b *blobStore) ecAdd(ctx context.Context, storesblobs ...sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	// Spin up a job processor of max thread count (threads) maximum.
	tr := sop.NewTaskRunner(ctx, maxThreadCount)

	for _, storeBlobs := range storesblobs {
		// Get the blob table specific erasure configuration.
		baseFolderPathsAcrossDrives, erasure := b.ecGetBaseFolderPathsAndErasureConfig(storeBlobs.BlobTable)
		if baseFolderPathsAcrossDrives == nil {
			err := fmt.Errorf("can't find Erasure Config setting for file %s", storeBlobs.BlobTable)
			log.Error(err.Error())
			return err
		}
	
		for _, blob := range storeBlobs.Blobs {
			ba := blob.Value
			contentsSize := len(ba)

			shards, err := erasure.Encode(ba)
			if err != nil {
				return err
			}

			for i := range shards {
				baseFolderPath := fmt.Sprintf("%s%c%s", baseFolderPathsAcrossDrives[i], os.PathSeparator, storeBlobs.BlobTable)
				blobKey := blob.Key
				shardIndex := i

				// Task WriteFile will add or replace existing file.
				tr.Go(func() error {
					fp := b.fileIO.ToFilePath(baseFolderPath, blobKey)
					if !b.fileIO.Exists(fp) {
						if err := b.fileIO.MkdirAll(fp, permission); err != nil {
							return err
						}
					}

					log.Debug(fmt.Sprintf("writing to file %s", fp))

					fn := fmt.Sprintf("%s%c%s_%d", fp, os.PathSeparator, blobKey.String(), shardIndex)

					// Prefix the shard w/ metadata.
					md := erasure.ComputeShardMetadata(contentsSize, shards, shardIndex)
					buf := make([]byte, len(md)+len(shards[shardIndex]))

					// TODO: refactor to write metadata then write the shard data so we don't use temp variable,
					// more optimal if shard size is huge.
					copy(buf, md)
					copy(buf[len(md):], shards[shardIndex])

					if err := b.fileIO.WriteFile(fn, buf, permission); err != nil {
						return err
					}
					return nil
				})
			}
		}
	}
	return tr.Wait()
}

func (b *blobStore) ecRemove(ctx context.Context, storesBlobsIDs ...sop.BlobsPayload[sop.UUID]) error {
	// Spin up a job processor of max thread count (threads) maximum.
	tr := sop.NewTaskRunner(ctx, maxThreadCount)

	var lastErr error
	for _, storeBlobIDs := range storesBlobsIDs {
		// Get the blob table specific erasure configuration.
		baseFolderPathsAcrossDrives, _ := b.ecGetBaseFolderPathsAndErasureConfig(storeBlobIDs.BlobTable)
		if baseFolderPathsAcrossDrives == nil {
			err := fmt.Errorf("can't find Erasure Config setting for file %s", storeBlobIDs.BlobTable)
			log.Error(err.Error())
			return err
		}
			
		for _, blobID := range storeBlobIDs.Blobs {

			for i := range baseFolderPathsAcrossDrives {
				baseFolderPath := fmt.Sprintf("%s%c%s", baseFolderPathsAcrossDrives[i], os.PathSeparator, storeBlobIDs.BlobTable)
				blobKey := blobID

				fp := b.fileIO.ToFilePath(baseFolderPath, blobKey)
				fn := fmt.Sprintf("%s%c%s_%d", fp, os.PathSeparator, blobKey.String(), i)

				// Do nothing if file already not existent.
				if !b.fileIO.Exists(fn) {
					continue
				}

				tr.Go(func() error {
					if err := b.fileIO.Remove(fn); err != nil {
						lastErr = err
					}
					return nil
				})
			}

		}
	}
	if err := tr.Wait(); err != nil {
		return err
	}
	return lastErr
}
