// Package in_red_cfs contains SOP implementations that uses Redis for caching, Cassandra & File System for backend data storage.
// The Objects Registry is stored in Cassandra and the B-Tree Nodes & their items' value data blobs are stored in the File System.
package inredcfs

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/fs"
	cas "github.com/sharedcode/sop/internal/cassandra"
	"github.com/sharedcode/sop/internal/inredck"
	sd "github.com/sharedcode/sop/internal/streamingdata"
)

// NewBtree will create a new B-Tree instance with data persisted to backend storage upon commit.
// If B-Tree(name) is not found in the backend, a new one will be created. Otherwise, the existing one will be opened
// and the parameters checked if matching. If you know that it exists, then it is more convenient and more readable to call
// the OpenBtree function.
func NewBtree[TK btree.Ordered, TV any](ctx context.Context, si sop.StoreOptions, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	if si.BlobStoreBaseFolderPath == "" {
		return nil, fmt.Errorf("si.BlobStoreBaseFolderPath(\"\") needs to be a valid folder path")
	}
	return inredck.NewBtree[TK, TV](ctx, si, t, comparer)
}

// NewBtreeWithEC is geared for enforcing the Blobs base folder path to generate good folder path that works with Erasure Coding I/O.
func NewBtreeWithEC[TK btree.Ordered, TV any](ctx context.Context, si sop.StoreOptions, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	// Force blob base folder path to be the name of the store so we generate good folder path.
	si.BlobStoreBaseFolderPath = si.Name
	return inredck.NewBtree[TK, TV](ctx, si, t, comparer)
}

// OpenBtree will open an existing B-Tree instance & prepare it for use in a transaction.
func OpenBtree[TK btree.Ordered, TV any](ctx context.Context, name string, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	return inredck.OpenBtree[TK, TV](ctx, name, t, comparer)
}

// Removes B-Tree with a given name from the backend storage. This involves dropping tables
// (registry & node blob) that are permanent action and thus, 'can't get rolled back.
//
// Use with care and only when you are sure to delete the tables.
func RemoveBtree(ctx context.Context, name string) error {
	sr := NewStoreRepository()
	return sr.Remove(ctx, name)
}

// NewStoreRepository is a convenience function to instantiate a repository with necessary File System
// based blob store implementation.
func NewStoreRepository() sop.StoreRepository {
	fio := fs.NewFileIO()
	mbsf := fs.NewManageStoreFolder(fio)
	return cas.NewStoreRepository(mbsf)
}

// NewStreamingDataStore is a convenience function to easily instantiate a streaming data store that stores
// blobs in File System.
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
	if so.BlobStoreBaseFolderPath == "" {
		return nil, fmt.Errorf("so.BlobStoreBaseFolderPath(\"\") needs to be a valid folder path")
	}
	return sd.NewStreamingDataStore[TK](ctx, so, trans, comparer)
}

// OpenStreamingDataStore is a convenience function to open an existing data store for use in "streaming data".
func OpenStreamingDataStore[TK btree.Ordered](ctx context.Context, name string, trans sop.Transaction, comparer btree.ComparerFunc[sd.StreamingDataKey[TK]]) (*sd.StreamingDataStore[TK], error) {
	return sd.OpenStreamingDataStore[TK](ctx, name, trans, comparer)
}
