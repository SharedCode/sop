package fs

import (
	"context"
	"fmt"
	"os"

	log "log/slog"
	"github.com/SharedCode/sop"
)

func (b *blobStore) ecGetOne(ctx context.Context, blobFilePath string, blobID sop.UUID) ([]byte, error) {
	// Spin up a job processor of 5 tasks (threads) maximum.
	tc, eg := sop.JobProcessor(ctx, 5)

	shards := make([][]byte, len(b.baseFolderPathsAcrossDrives))
	shardsWithMetadata := make([][]byte, len(b.baseFolderPathsAcrossDrives))
	shardsMetaData := make([][]byte, len(b.baseFolderPathsAcrossDrives))
	for i := range b.baseFolderPathsAcrossDrives {
		baseFolderPath := fmt.Sprintf("%s%c%s", b.baseFolderPathsAcrossDrives[i], os.PathSeparator, blobFilePath)
		blobKey := blobID

		fp := b.fileIO.ToFilePath(baseFolderPath, blobKey)
		fn := fmt.Sprintf("%s%c%s_%d", fp, os.PathSeparator, blobKey.String(), i)

		shardIndex := i

		task := func() error {

			log.Debug(fmt.Sprintf("readinf from file %s",fn))

			ba, err := b.fileIO.ReadFile(fn)
			if err != nil {
				return err
			}
			shardsWithMetadata[shardIndex] = ba
			shardsMetaData[shardIndex] = ba[0:b.erasure.MetaDataSize()]
			shards[shardIndex] = ba[b.erasure.MetaDataSize():]
			return nil
		}
		tc <- task
	}
	close(tc)
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	dr := b.erasure.Decode(shards, shardsMetaData)
	if dr.Error != nil {
		return nil, dr.Error
	}

	if b.repairCorruptedShards && len(dr.ReconstructedShardsIndeces) > 0 {
		// Repair corrupted or bitrot shards (a.k.a. damaged shards). Just do sequential processing as
		// damaged shards should be typically one, residing in a drive that failed.
		for i, _ := range dr.ReconstructedShardsIndeces {
			baseFolderPath := fmt.Sprintf("%s%c%s", b.baseFolderPathsAcrossDrives[i], os.PathSeparator, blobFilePath)
			blobKey := blobID
	
			fp := b.fileIO.ToFilePath(baseFolderPath, blobKey)
			fn := fmt.Sprintf("%s%c%s_%d", fp, os.PathSeparator, blobKey.String(), i)
			err := b.fileIO.WriteFile(fn, shardsWithMetadata[i], permission)
			// Just log warning if damaged shard can't be repaired as we have reconstructed it part of return.
			if err != nil {
				log.Warn(fmt.Sprintf("error encountered repairing a damaged shard (%s)", fn))
			}
		}
	}

	return dr.DecodedData, nil
}

func (b *blobStore) ecAdd(ctx context.Context, storesblobs ...sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	// Spin up a job processor of 5 tasks (threads) maximum.
	tc, eg := sop.JobProcessor(ctx, 5)

	for _, storeBlobs := range storesblobs {
		for _, blob := range storeBlobs.Blobs {
			ba := blob.Value
			contentsSize := len(ba)

			shards, err := b.erasure.Encode(ba)
			if err != nil {
				return err
			}

			for i := range shards {
				baseFolderPath := fmt.Sprintf("%s%c%s", b.baseFolderPathsAcrossDrives[i], os.PathSeparator, storeBlobs.BlobTable)
				blobKey := blob.Key

				// Task WriteFile will add or replace existing file.
				task := func() error {
					fp := b.fileIO.ToFilePath(baseFolderPath, blobKey)
					if !b.fileIO.Exists(fp) {
						if err := b.fileIO.MkdirAll(fp, permission); err != nil {
							return err
						}
					}

					log.Debug(fmt.Sprintf("writing to file %s",fp))

					fn := fmt.Sprintf("%s%c%s_%d", fp, os.PathSeparator, blobKey.String(), i)

					// Prefix the shard w/ metadata.
					md := b.erasure.ComputeShardMetadata(contentsSize, shards, i)
					buf := make([]byte, len(md) + len(shards[i]))

					// TODO: refactor to write metadata then write the shard data so we don't use temp variable,
					// more optimal if shard size is huge.
					copy(buf, md)
					copy(buf[len(md):], shards[i])
		
					if err := b.fileIO.WriteFile(fn, buf, permission); err != nil {
						return err
					}
					return nil
				}
				tc <- task
			}

		}
	}
	close(tc)
	return eg.Wait()
}

func (b *blobStore) ecRemove(ctx context.Context, storesBlobsIDs ...sop.BlobsPayload[sop.UUID]) error {
	// Spin up a job processor of 5 tasks (threads) maximum.
	tc, eg := sop.JobProcessor(ctx, 5)

	for _, storeBlobIDs := range storesBlobsIDs {
		for _, blobID := range storeBlobIDs.Blobs {

			for i := range b.baseFolderPathsAcrossDrives {
				baseFolderPath := fmt.Sprintf("%s%c%s", b.baseFolderPathsAcrossDrives[i], os.PathSeparator, storeBlobIDs.BlobTable)
				blobKey := blobID

				fp := b.fileIO.ToFilePath(baseFolderPath, blobKey)
				fn := fmt.Sprintf("%s%c%s_%d", fp, os.PathSeparator, blobKey.String(), i)

				// Do nothing if file already not existent.
				if !b.fileIO.Exists(fn) {
					continue
				}

				task := func() error {
					if err := b.fileIO.Remove(fn); err != nil {
						return err
					}
					return nil	
				}
				tc <- task
			}

		}
	}
	close(tc)
	return eg.Wait()
}
