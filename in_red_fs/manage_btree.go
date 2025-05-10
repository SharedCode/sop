// Package in_red_ck contains SOP implementations that uses Redis for caching & Cassandra for backend data storage.
package in_red_fs

import (
	"context"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/common"
	"github.com/SharedCode/sop/fs"
	"github.com/SharedCode/sop/redis"
	sd "github.com/SharedCode/sop/streaming_data"
)

// Removes B-Tree with a given name from the backend storage. This involves dropping tables
// (registry & node blob) that are permanent action and thus, 'can't get rolled back.
//
// Use with care and only when you are sure to delete the tables.
// Also, this does NOT clear out the (Redis) cache, so, you may generate an issue. This is only meant
// to be used on special DB administration case, not to be used part of your application.
// Make sure to delete all entries in (Redis) cache if ever you delete a SOP table using this function.
func RemoveBtree(ctx context.Context, storesBaseFolder string, name string) error {
	cache := redis.NewClient()
	replicationTracker := fs.NewReplicationTracker([]string{storesBaseFolder}, false)
	storeRepository, err := fs.NewStoreRepository(replicationTracker, nil, cache)
	if err != nil {
		return err
	}
	return storeRepository.Remove(ctx, name)
}

// OpenBtree will open an existing B-Tree instance & prepare it for use in a transaction.
func OpenBtree[TK btree.Comparable, TV any](ctx context.Context, name string, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	return common.OpenBtree[TK, TV](ctx, name, t, comparer)
}

// NewBtree will create a new B-Tree instance with data persisted to backend storage upon commit.
// If B-Tree(name) is not found in the backend, a new one will be created. Otherwise, the existing one will be opened
// and the parameters checked if matching. If you know that it exists, then it is more convenient and more readable to call
// the OpenBtree function.
func NewBtree[TK btree.Comparable, TV any](ctx context.Context, si sop.StoreOptions, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	si.DisableRegistryStoreFormatting = true
	trans, _ := t.GetPhasedTransaction().(*common.Transaction)
	sr := trans.GetStoreRepository().(*fs.StoreRepository)
	si.BlobStoreBaseFolderPath = sr.GetStoresBaseFolder()
	return common.NewBtree[TK, TV](ctx, si, t, comparer)
}

// NewBtreeWithReplication is geared for enforcing the Blobs base folder path to generate good folder path that works
// with Erasure Coding I/O, a part of SOP's replication feature (for blobs replication).
func NewBtreeWithReplication[TK btree.Comparable, TV any](ctx context.Context, si sop.StoreOptions, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	si.DisableRegistryStoreFormatting = true
	si.DisableBlobStoreFormatting = true
	return common.NewBtree[TK, TV](ctx, si, t, comparer)
}

// Streaming Data Store related.

// NewStreamingDataStore is synonymous to NewStreamingDataStore but is geared for storing blobs in blob table in Cassandra.
func NewStreamingDataStore[TK btree.Comparable](ctx context.Context, name string, trans sop.Transaction, comparer btree.ComparerFunc[sd.StreamingDataKey[TK]]) (*sd.StreamingDataStore[TK], error) {
	return NewStreamingDataStoreExt[TK](ctx, name, trans, "", comparer)
}

// NewStreamingDataStoreExt instantiates a new Data Store for use in "streaming data".
// That is, the "value" is saved in separate segment(partition in Cassandra) &
// actively persisted to the backend, e.g. - call to Add method will save right away
// to the separate segment and on commit, it will be a quick action as data is already saved to the data segments.
//
// This behaviour makes this store ideal for data management of huge blobs, like movies or huge data graphs.
// Supports parameter for blobStoreBaseFolderPath which is useful in File System based blob storage.
func NewStreamingDataStoreExt[TK btree.Comparable](ctx context.Context, name string, trans sop.Transaction, blobStoreBaseFolderPath string, comparer btree.ComparerFunc[sd.StreamingDataKey[TK]]) (*sd.StreamingDataStore[TK], error) {
	btree, err := NewBtree[sd.StreamingDataKey[TK], []byte](ctx, sop.ConfigureStore(name, true, 500, "Streaming data", sop.BigData, blobStoreBaseFolderPath), trans, comparer)
	if err != nil {
		return nil, err
	}
	return &sd.StreamingDataStore[TK]{
		Btree: btree,
	}, nil
}

// Synonymous to NewStreamingDataStore but expects StoreOptions parameter.
func NewStreamingDataStoreOptions[TK btree.Comparable](ctx context.Context, options sop.StoreOptions, trans sop.Transaction, comparer btree.ComparerFunc[sd.StreamingDataKey[TK]]) (*sd.StreamingDataStore[TK], error) {
	btree, err := NewBtree[sd.StreamingDataKey[TK], []byte](ctx, options, trans, comparer)
	if err != nil {
		return nil, err
	}
	return &sd.StreamingDataStore[TK]{
		Btree: btree,
	}, nil
}

// OpenStreamingDataStore opens an existing data store for use in "streaming data".
func OpenStreamingDataStore[TK btree.Comparable](ctx context.Context, name string, trans sop.Transaction, comparer btree.ComparerFunc[sd.StreamingDataKey[TK]]) (*sd.StreamingDataStore[TK], error) {
	btree, err := OpenBtree[sd.StreamingDataKey[TK], []byte](ctx, name, trans, comparer)
	if err != nil {
		return nil, err
	}
	return &sd.StreamingDataStore[TK]{
		Btree: btree,
	}, nil
}
