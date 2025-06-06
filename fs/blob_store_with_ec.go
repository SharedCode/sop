package fs

import (
	"context"
	"fmt"
	log "log/slog"
	"os"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/fs/erasure"
)

const (
	maxThreadCount = 7
)

// BlobStore has no caching built in because blobs are huge, caller code can apply caching on top of it.
type blobStoreWithEC struct {
	fileIO                      FileIO
	toFilePath                  ToFilePathFunc
	erasure                     map[string]*erasure.Erasure
	baseFolderPathsAcrossDrives map[string][]string
	repairCorruptedShards       bool
}

// Allows app to specify a global Erasure Coding Config once and allow code to simply don't bother
// specifying it again.
var globalErasureConfig map[string]ErasureCodingConfig

// Invoke SetGlobalErasureConfig to set the application global Erasure Coding Config lookup.
func SetGlobalErasureConfig(erasureConfig map[string]ErasureCodingConfig) {
	globalErasureConfig = erasureConfig
}

// Returns the global Erasure Coding config.
func GetGlobalErasureConfig() map[string]ErasureCodingConfig {
	return globalErasureConfig
}

// Instantiate a blob store with replication (via Erasure Coding (EC)) capabilities.
func NewBlobStoreWithEC(toFilePath ToFilePathFunc, fileIO FileIO, erasureConfig map[string]ErasureCodingConfig) (sop.BlobStore, error) {
	if erasureConfig == nil {
		erasureConfig = globalErasureConfig
	}
	if toFilePath == nil {
		toFilePath = DefaultToFilePath
	}

	var e map[string]*erasure.Erasure
	var baseFolderPathsAcrossDrives map[string][]string
	var repairCorruptedShards bool

	if erasureConfig != nil {
		e = make(map[string]*erasure.Erasure, len(erasureConfig))
		baseFolderPathsAcrossDrives = make(map[string][]string, len(erasureConfig))
		for k, v := range erasureConfig {
			ec, err := erasure.NewErasure(v.DataShardsCount, v.ParityShardsCount)
			if err != nil {
				return nil, err
			}
			repairCorruptedShards = v.RepairCorruptedShards
			if ec.DataShardsCount+ec.ParityShardsCount != len(v.BaseFolderPathsAcrossDrives) {
				return nil, fmt.Errorf("baseFolderPaths array elements count should match the sum of dataShardsCount & parityShardsCount")
			}
			e[k] = ec
			baseFolderPathsAcrossDrives[k] = v.BaseFolderPathsAcrossDrives
		}
	}
	if fileIO == nil {
		fileIO = NewDefaultFileIO()
	}
	return &blobStoreWithEC{
		fileIO:                      fileIO,
		toFilePath:                  toFilePath,
		erasure:                     e,
		baseFolderPathsAcrossDrives: baseFolderPathsAcrossDrives,
		repairCorruptedShards:       repairCorruptedShards,
	}, nil
}

func (b *blobStoreWithEC) GetOne(ctx context.Context, blobFilePath string, blobID sop.UUID) ([]byte, error) {
	// Spin up a job processor of max thread count (threads) maximum.
	tr := sop.NewTaskRunner(ctx, maxThreadCount)

	// Get the blob table (blobFilePath) specific ec configuration.
	baseFolderPathsAcrossDrives, ec := b.getBaseFolderPathsAndErasureConfig(blobFilePath)
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
		fp := b.toFilePath(baseFolderPath, blobKey)
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
			shardsMetaData[shardIndex] = ba[0:erasure.MetaDataSize]
			shards[shardIndex] = ba[erasure.MetaDataSize:]
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
	dr := ec.Decode(shards, shardsMetaData)
	if dr.Error != nil {
		return nil, dr.Error
	}

	if b.repairCorruptedShards && len(dr.ReconstructedShardsIndeces) > 0 {
		// Repair corrupted or bitrot shards (a.k.a. damaged shards). Just do sequential processing as
		// damaged shards should be typically one, residing in a drive that failed.
		for _, i := range dr.ReconstructedShardsIndeces {
			baseFolderPath := fmt.Sprintf("%s%c%s", baseFolderPathsAcrossDrives[i], os.PathSeparator, blobFilePath)
			blobKey := blobID

			fp := b.toFilePath(baseFolderPath, blobKey)
			fn := fmt.Sprintf("%s%c%s_%d", fp, os.PathSeparator, blobKey.String(), i)

			log.Debug(fmt.Sprintf("repairing file %s", fn))

			md := ec.ComputeShardMetadata(len(dr.DecodedData), shards, i)
			buf := make([]byte, len(md)+len(shards[i]))

			// Tip: consider refactor to write metadata then write the shard data so we don't use temp variable,
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

func (b *blobStoreWithEC) getBaseFolderPathsAndErasureConfig(blobTable string) ([]string, *erasure.Erasure) {
	// Get the blob table specific erasure configuration.
	baseFolderPathsAcrossDrives := b.baseFolderPathsAcrossDrives[blobTable]
	erasure := b.erasure[blobTable]

	if baseFolderPathsAcrossDrives == nil {
		baseFolderPathsAcrossDrives = b.baseFolderPathsAcrossDrives[""]
		erasure = b.erasure[""]
	}
	return baseFolderPathsAcrossDrives, erasure
}

func (b *blobStoreWithEC) Update(ctx context.Context, storesblobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	return b.Add(ctx, storesblobs)
}

func (b *blobStoreWithEC) Add(ctx context.Context, storesblobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	var lastErr error
	for _, storeBlobs := range storesblobs {
		// Get the blob table specific erasure configuration.
		baseFolderPathsAcrossDrives, erasure := b.getBaseFolderPathsAndErasureConfig(storeBlobs.BlobTable)
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

			// Spin up a job processor of shards count max threads.
			tr := sop.NewTaskRunner(ctx, len(shards))
			ch := make(chan error, len(shards))

			for i := range shards {
				baseFolderPath := fmt.Sprintf("%s%c%s", baseFolderPathsAcrossDrives[i], os.PathSeparator, storeBlobs.BlobTable)
				blobKey := blob.Key
				shardIndex := i

				// Task WriteFile will add or replace existing file.
				tr.Go(func() error {
					fp := b.toFilePath(baseFolderPath, blobKey)
					if !b.fileIO.Exists(fp) {
						if err := b.fileIO.MkdirAll(fp, permission); err != nil {
							return err
						}
					}

					log.Debug(fmt.Sprintf("writing to file %s", fp))

					fn := fmt.Sprintf("%s%c%s_%d", fp, os.PathSeparator, blobKey.String(), shardIndex)

					md := erasure.ComputeShardMetadata(contentsSize, shards, shardIndex)
					buf := make([]byte, len(md)+len(shards[shardIndex]))

					// Prefix the shard w/ metadata followed by the shard.
					copy(buf, md)
					copy(buf[len(md):], shards[shardIndex])

					if err := b.fileIO.WriteFile(fn, buf, permission); err != nil {
						ch <- err
						// Return nil so we don't generate an error, NOT until we exceed write error beyond parity count.
						return nil
					}
					return nil
				})
			}
			// Keep the last error and return that, enough to rollback the transaction.
			if err := tr.Wait(); err != nil {
				close(ch)
				lastErr = err
			} else {
				c := 0
				cont := true
				for cont {
					select {
					case err, ok := <-ch:
						if ok {
							c++
							lastErr = err
						} else {
							cont = false
						}
					default:
						cont = false
					}
				}
				close(ch)
				// Short circuit to cause transaction rollback if drive failures are untolerable.
				if c > erasure.ParityShardsCount {
					return lastErr
				} else {
					if lastErr != nil {
						log.Warn(fmt.Sprintf("error writing to a drive but EC tolerates it, details: %v", lastErr))
						lastErr = nil
					}
				}
			}
		}
	}

	return lastErr
}

func (b *blobStoreWithEC) Remove(ctx context.Context, storesBlobsIDs []sop.BlobsPayload[sop.UUID]) error {
	// Spin up a job processor of max thread count (threads) maximum.
	tr := sop.NewTaskRunner(ctx, maxThreadCount)

	var lastErr error
	for _, storeBlobIDs := range storesBlobsIDs {
		// Get the blob table specific erasure configuration.
		baseFolderPathsAcrossDrives, _ := b.getBaseFolderPathsAndErasureConfig(storeBlobIDs.BlobTable)
		if baseFolderPathsAcrossDrives == nil {
			err := fmt.Errorf("can't find Erasure Config setting for file %s", storeBlobIDs.BlobTable)
			log.Error(err.Error())
			return err
		}

		for _, blobID := range storeBlobIDs.Blobs {

			for i := range baseFolderPathsAcrossDrives {
				baseFolderPath := fmt.Sprintf("%s%c%s", baseFolderPathsAcrossDrives[i], os.PathSeparator, storeBlobIDs.BlobTable)
				blobKey := blobID

				fp := b.toFilePath(baseFolderPath, blobKey)
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
	if lastErr != nil {
		log.Error(fmt.Sprintf("error deleting from drive but ignoring it to tolerate part of EC feature, details: %v", lastErr))
		lastErr = nil
	}
	return lastErr
}
