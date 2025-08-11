// Package inredfs contains SOP implementations that use Redis for caching and the filesystem for backend data storage.
package inredfs

import (
	"context"
	"fmt"
	log "log/slog"
	"os"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common"
	"github.com/sharedcode/sop/fs"
	sd "github.com/sharedcode/sop/internal/streamingdata"
	"github.com/sharedcode/sop/redis"
)

// NewBtree creates a new B-tree instance with data persisted to the backend storage upon commit.
// If the B-tree (by name) is not found, a new one is created; otherwise, the existing one is opened and parameters validated.
// If you know it exists, prefer OpenBtree for clarity.
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

// OpenBtree opens an existing B-tree instance and prepares it for use in a transaction.
func OpenBtree[TK btree.Ordered, TV any](ctx context.Context, name string, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	if ct, ok := t.GetPhasedTransaction().(*common.Transaction); ok {
		if ct.HandleReplicationRelatedError != nil {
			return nil, fmt.Errorf("failed in OpenBtree as transaction has replication enabled, use OpenBtreeWithReplication instead")
		}
	}
	return common.OpenBtree[TK, TV](ctx, name, t, comparer)
}

// NewBtreeWithReplication creates a B-tree that uses SOP's filesystem-based replication feature.
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

// OpenBtreeWithReplication opens a B-tree that uses SOP's filesystem-based replication feature.
func OpenBtreeWithReplication[TK btree.Ordered, TV any](ctx context.Context, name string, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	if ct, ok := t.GetPhasedTransaction().(*common.Transaction); ok {
		if ct.HandleReplicationRelatedError == nil {
			return nil, fmt.Errorf("failed in OpenBtreeWithReplication as transaction has no replication, use OpenBtree instead")
		}
	}
	return common.OpenBtree[TK, TV](ctx, name, t, comparer)
}

// RemoveBtree removes the B-tree with the given name from backend storage.
// This is destructive: it drops registry and node-blob data and cannot be rolled back.
func RemoveBtree(ctx context.Context, storesBaseFolder string, name string) error {
	log.Info(fmt.Sprintf("Btree %s%c%s is about to be deleted", storesBaseFolder, os.PathSeparator, name))

	cache := redis.NewClient()
	replicationTracker, err := fs.NewReplicationTracker(ctx, []string{storesBaseFolder}, false, cache)
	if err != nil {
		return err
	}
	storeRepository, err := fs.NewStoreRepository(ctx, replicationTracker, nil, cache, 0)
	if err != nil {
		return err
	}

	return storeRepository.Remove(ctx, name)
}

// ReinstateFailedDrives asks the replication tracker to reinstate failed passive targets.
// storesFolders must contain the active and passive stores' base folder paths.
func ReinstateFailedDrives(ctx context.Context, storesFolders []string) error {
	if len(storesFolders) != 2 {
		return fmt.Errorf("'storeFolders' need to be array of two strings(drive/folder paths)")
	}

	cache := redis.NewClient()
	rt, err := fs.NewReplicationTracker(ctx, storesFolders, true, cache)
	if err != nil {
		log.Error(fmt.Sprintf("failed instantiating Replication Tracker, details: %v", err))
		return err
	}

	if err := rt.ReinstateFailedDrives(ctx); err != nil {
		log.Error(fmt.Sprintf("failed reinstating failed drives, details: %v", err))
		return err
	}
	return nil
}

// Streaming Data Store related.

// NewStreamingDataStore creates a streaming data store backed by a B-tree for chunked large-object storage.
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

// OpenStreamingDataStore opens an existing streaming data store.
func OpenStreamingDataStore[TK btree.Ordered](ctx context.Context, name string, trans sop.Transaction, comparer btree.ComparerFunc[sd.StreamingDataKey[TK]]) (*sd.StreamingDataStore[TK], error) {
	btree, err := OpenBtree[sd.StreamingDataKey[TK], []byte](ctx, name, trans, comparer)
	if err != nil {
		return nil, err
	}
	return &sd.StreamingDataStore[TK]{
		BtreeInterface: btree,
	}, nil
}

// NewStreamingDataStoreWithReplication creates a streaming data store with filesystem replication enabled.
func NewStreamingDataStoreWithReplication[TK btree.Ordered](ctx context.Context, so sop.StoreOptions, trans sop.Transaction, comparer btree.ComparerFunc[sd.StreamingDataKey[TK]]) (*sd.StreamingDataStore[TK], error) {
	if so.SlotLength < sd.MinimumStreamingStoreSlotLength {
		return nil, fmt.Errorf("streaming data store requires minimum of %d SlotLength", sd.MinimumStreamingStoreSlotLength)
	}
	if so.IsValueDataInNodeSegment {
		return nil, fmt.Errorf("streaming data store requires value data to be set for save in separate segment(IsValueDataInNodeSegment = false)")
	}
	if !so.IsUnique {
		return nil, fmt.Errorf("streaming data store requires unique key (IsUnique = true) to be set to true")
	}
	btree, err := NewBtreeWithReplication[sd.StreamingDataKey[TK], []byte](ctx, so, trans, comparer)
	if err != nil {
		return nil, err
	}
	return &sd.StreamingDataStore[TK]{
		BtreeInterface: btree,
	}, nil
}

// OpenStreamingDataStoreWithReplication opens an existing streaming data store with filesystem replication enabled.
func OpenStreamingDataStoreWithReplication[TK btree.Ordered](ctx context.Context, name string, trans sop.Transaction, comparer btree.ComparerFunc[sd.StreamingDataKey[TK]]) (*sd.StreamingDataStore[TK], error) {
	btree, err := OpenBtreeWithReplication[sd.StreamingDataKey[TK], []byte](ctx, name, trans, comparer)
	if err != nil {
		return nil, err
	}
	return &sd.StreamingDataStore[TK]{
		BtreeInterface: btree,
	}, nil
}
