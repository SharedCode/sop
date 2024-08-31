// Package in_red_cfs contains SOP implementations that uses Redis for caching, Cassandra & File System for backend data storage.
// The Objects Registry is stored in Cassandra and the B-Tree Nodes & their items' value data blobs are stored in the File System.
package in_red_cfs

import (
	"context"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_red_cfs/fs"
	"github.com/SharedCode/sop/in_red_ck"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
	sd "github.com/SharedCode/sop/streaming_data"
)

// NewBtree will create a new B-Tree instance with data persisted to backend storage upon commit.
// If B-Tree(name) is not found in the backend, a new one will be created. Otherwise, the existing one will be opened
// and the parameters checked if matching. If you know that it exists, then it is more convenient and more readable to call
// the OpenBtree function.
func NewBtree[TK btree.Comparable, TV any](ctx context.Context, si sop.StoreOptions, t sop.Transaction) (btree.BtreeInterface[TK, TV], error) {
	return in_red_ck.NewBtree[TK, TV](ctx, si, t)
}

// OpenBtree will open an existing B-Tree instance & prepare it for use in a transaction.
func OpenBtree[TK btree.Comparable, TV any](ctx context.Context, name string, t sop.Transaction) (btree.BtreeInterface[TK, TV], error) {
	return in_red_ck.OpenBtree[TK, TV](ctx, name, t)
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
	fio := fs.DefaultFileIO{}
	mbsf := fs.NewManageBlobStoreFolder(fio)
	return cas.NewStoreRepositoryExt(mbsf)
}

// NewStreamingDataStore is a convenience function to easily instantiate a streaming data store that stores
// blobs in File System.
//
// Specify your blobStoreBaseFolderPath to an appropriate folder path that will be the base folder of blob files.
func NewStreamingDataStore[TK btree.Comparable](ctx context.Context, name string, trans sop.Transaction, blobStoreBaseFolderPath string) (*sd.StreamingDataStore[TK], error) {
	return sd.NewStreamingDataStoreExt[TK](ctx, name, trans, blobStoreBaseFolderPath)
}

// OpenStreamingDataStore is a convenience function to open an existing data store for use in "streaming data".
func OpenStreamingDataStore[TK btree.Comparable](ctx context.Context, name string, trans sop.Transaction) (*sd.StreamingDataStore[TK], error) {
	return sd.OpenStreamingDataStore[TK](ctx, name, trans)
}