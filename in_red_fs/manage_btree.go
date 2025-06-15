// Package in_red_ck contains SOP implementations that uses Redis for caching & Cassandra for backend data storage.
package in_red_fs

import (
	"context"
	"fmt"
	log "log/slog"
	"os"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/common"
	"github.com/SharedCode/sop/fs"
	"github.com/SharedCode/sop/redis"
	sd "github.com/SharedCode/sop/streaming_data"
)

// NewBtree will create a new B-Tree instance with data persisted to backend storage upon commit.
// If B-Tree(name) is not found in the backend, a new one will be created. Otherwise, the existing one will be opened
// and the parameters checked if matching. If you know that it exists, then it is more convenient and more readable to call
// the OpenBtree function.
func NewBtree[TK btree.Ordered, TV any](ctx context.Context, so sop.StoreOptions, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	if ct, ok := t.GetPhasedTransaction().(*common.Transaction); ok {
		if ct.HandleReplicationRelatedError != nil {
			return nil, fmt.Errorf("failed in NewBtree as transaction has replication enabled, use NewBtreeWithReplication instead")
		}
	}
	so.DisableRegistryStoreFormatting = true
	trans, _ := t.GetPhasedTransaction().(*common.Transaction)
	sr := trans.GetStoreRepository().(*fs.StoreRepository)
	so.BlobStoreBaseFolderPath = sr.GetStoresBaseFolder()
	return common.NewBtree[TK, TV](ctx, so, t, comparer)
}

// OpenBtree will open an existing B-Tree instance & prepare it for use in a transaction.
func OpenBtree[TK btree.Ordered, TV any](ctx context.Context, name string, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	if ct, ok := t.GetPhasedTransaction().(*common.Transaction); ok {
		if ct.HandleReplicationRelatedError != nil {
			return nil, fmt.Errorf("failed in OpenBtree as transaction has replication enabled, use OpenBtreeWithReplication instead")
		}
	}
	return common.OpenBtree[TK, TV](ctx, name, t, comparer)
}

// NewBtreeWithReplication will (create! &) instantiate a B-tree that has SOP's file system based replication feature.
func NewBtreeWithReplication[TK btree.Ordered, TV any](ctx context.Context, so sop.StoreOptions, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	if ct, ok := t.GetPhasedTransaction().(*common.Transaction); ok {
		if ct.HandleReplicationRelatedError == nil {
			return nil, fmt.Errorf("failed in NewBtreeWithReplication as transaction has no replication, use NewBtree instead")
		}
	}
	so.DisableRegistryStoreFormatting = true
	so.DisableBlobStoreFormatting = true
	return common.NewBtree[TK, TV](ctx, so, t, comparer)
}

// OpenBtreeWithReplication will (open &) instantiate a B-tree that has SOP's file system based replication feature.
func OpenBtreeWithReplication[TK btree.Ordered, TV any](ctx context.Context, name string, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	if ct, ok := t.GetPhasedTransaction().(*common.Transaction); ok {
		if ct.HandleReplicationRelatedError == nil {
			return nil, fmt.Errorf("failed in OpenBtreeWithReplication as transaction has no replication, use OpenBtree instead")
		}
	}
	return common.OpenBtree[TK, TV](ctx, name, t, comparer)
}

// Removes B-Tree with a given name from the backend storage. This involves dropping tables
// (registry & node blob) that are permanent action and thus, 'can't get rolled back.
//
// Use with care and only when you are sure to delete the tables. This does not flush the cache,
// you will have to call cache.Clear to do that, WHEN safe.
//
// This API & cache.Clear are both destructive, please use with care.
func RemoveBtree(ctx context.Context, storesBaseFolder string, name string) error {
	log.Info(fmt.Sprintf("Btree %s%c%s is about to be deleted", storesBaseFolder, os.PathSeparator, name))

	cache := redis.NewClient()
	replicationTracker, err := fs.NewReplicationTracker(ctx, []string{storesBaseFolder}, false, cache)
	if err != nil {
		return err
	}
	storeRepository, err := fs.NewStoreRepository(replicationTracker, nil, cache)
	if err != nil {
		return err
	}

	return storeRepository.Remove(ctx, name)
}

// Reinstate replication of the failed passive targets by delegating call to the Replication Tracker.
//
// storesFolders & erasureConfig parameters serve the same purpose as how they got used/
// values passed in in the call to NewTransactionOptionsWithReplication(..).
// storesFolders should contain the active & passive stores' base folder paths.
// erasureConfig should be nil if storesFolders is already specified.
//
// Also, if you want SOP to use the global erasure config and there is one set (in "fs" package), then these
// two can be nil. In a bit later, SOP may support caching in L2 cache the storesFolders, so,
// in that version, this function may just take it from L2 cache (Redis).
//
// If storesFolders is nil, SOP will use the 1st two drive/paths it can find from the
// erasureConfig or the global erasure config, whichever is passed in or available.
// The default erasureConfig map entry (with key "") will be tried for use and if this is not
// set or it only has one path, then the erasureConfig map will be iterated and whichever
// entry with at least two drive paths set, then will "win" as the stores base folders paths.
//
// Explicitly specifying it in storesFolders param is recommended.
func ReinstateFailedDrives(ctx context.Context, storesFolders []string, erasureConfig map[string]fs.ErasureCodingConfig, registryHashModValue int) error {
	if erasureConfig == nil {
		erasureConfig = fs.GetGlobalErasureConfig()
	}
	// Try to extract stores base folders from the erasure config
	storesFolders = pickStoresFoldersFromEC(storesFolders, erasureConfig)

	if len(storesFolders) < 2 {
		return fmt.Errorf("'storeFolders' need to be array of two strings(drive/folder paths). 'was not able to reuse anything from 'erasureConfig'")
	}

	rt, err := fs.NewReplicationTracker(ctx, storesFolders, true, redis.NewClient())
	if err != nil {
		log.Error(fmt.Sprintf("failed instantiating Replication Tracker, details: %v", err))
		return err
	}
	if err := rt.ReinstateFailedDrives(ctx, registryHashModValue); err != nil {
		log.Error(fmt.Sprintf("failed reinstating failed drives, details: %v", err))
		return err
	}
	return nil
}

// Streaming Data Store related.

// NewStreamingDataStore implements data chunking on top of a B-tree, thus, it can support very large data sets. limited by your hardware only.
// It returns JSON constructs like "encoder" (for writing) & "decoder" (for reading) which is backed by the B-tree and thus, gives your code
// the convenience to "chunkitize" a huge huge object (blob) and still, be able to easily stream it, without impacting the network, because
// B-tree stores this object in chunks and even allows you to manage its part(s). As they are stored in a B-tree in chunks, thus, you can easily
// replace or update any part of the huge huge object (blob).
func NewStreamingDataStore[TK btree.Ordered](ctx context.Context, so sop.StoreOptions, trans sop.Transaction, comparer btree.ComparerFunc[sd.StreamingDataKey[TK]]) (*sd.StreamingDataStore[TK], error) {
	if so.SlotLength < sd.MinimumStreamingStoreSlotLength {
		return nil, fmt.Errorf("streaming data store requires minimum of %d SlotLength", sd.MinimumStreamingStoreSlotLength)
	}
	if so.IsValueDataInNodeSegment {
		return nil, fmt.Errorf("streaming data store requires value data to be set for save in separate segment(IsValueDataInNodeSegment = false)")
	}
	if !so.IsUnique {
		return nil, fmt.Errorf("streaming data store requires unique key (IsUnique = true) to be set to true")
	}
	btree, err := NewBtree[sd.StreamingDataKey[TK], []byte](ctx, so, trans, comparer)
	if err != nil {
		return nil, err
	}
	return &sd.StreamingDataStore[TK]{
		BtreeInterface: btree,
	}, nil
}

// OpenStreamingDataStore opens an existing data store for use in "streaming data".
func OpenStreamingDataStore[TK btree.Ordered](ctx context.Context, name string, trans sop.Transaction, comparer btree.ComparerFunc[sd.StreamingDataKey[TK]]) (*sd.StreamingDataStore[TK], error) {
	btree, err := OpenBtree[sd.StreamingDataKey[TK], []byte](ctx, name, trans, comparer)
	if err != nil {
		return nil, err
	}
	return &sd.StreamingDataStore[TK]{
		BtreeInterface: btree,
	}, nil
}

// TODO: add NewXx/OpenXx "with replication" for streaming data stores.
