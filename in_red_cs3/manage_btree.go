// Package in_red_cs3 contains SOP implementations that uses Redis for caching, Cassandra & AWS S3 for backend data storage.
// The Objects Registry is stored in Cassandra and the B-Tree Nodes & their items' value data blobs are stored in the AWS S3 bucket(s).
package in_red_cs3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/aws_s3"
	"github.com/SharedCode/sop/btree"
	cas "github.com/SharedCode/sop/cassandra"
	"github.com/SharedCode/sop/in_red_ck"
	sd "github.com/SharedCode/sop/streaming_data"
)

// NewBtree will create a new B-Tree instance with data persisted to backend storage upon commit.
// If B-Tree(name) is not found in the backend, a new one will be created. Otherwise, the existing one will be opened
// and the parameters checked if matching. If you know that it exists, then it is more convenient and more readable to call
// the OpenBtree function.
func NewBtree[TK btree.Comparable, TV any](ctx context.Context, si sop.StoreOptions, t sop.Transaction) (btree.BtreeInterface[TK, TV], error) {
	// Use the Store name as the bucket name.
	si.DisableBlobStoreFormatting = true
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
func RemoveBtree[TK btree.Comparable, TV any](ctx context.Context, s3Client *s3.Client, region string, name string) error {
	// Delete B-Tree contents.
	if err := removeBtreeContents[TK, TV](ctx, s3Client, region, name); err != nil {
		return err
	}
	// Delete the B-Tree itself including its backend bits.
	sr, err := NewStoreRepository(s3Client, region)
	if err != nil {
		return err
	}
	return sr.Remove(ctx, name)
}

// NewStoreRepository is a convenience function to instantiate a repository with necessary File System
// based blob store implementation.
func NewStoreRepository(s3Client *s3.Client, region string) (sop.StoreRepository, error) {
	mbs, err := aws_s3.NewManageBucket(s3Client, region)
	if err != nil {
		return nil, err
	}
	return cas.NewStoreRepositoryExt(mbs), nil
}

// NewStreamingDataStore is a convenience function to easily instantiate a streaming data store that stores
// blobs in AWS S3.
func NewStreamingDataStore[TK btree.Comparable](ctx context.Context, name string, trans sop.Transaction) (*sd.StreamingDataStore[TK], error) {
	si := sop.ConfigureStore(name, true, 500, "Streaming data", sop.BigData, "")
	si.DisableBlobStoreFormatting = true
	return sd.NewStreamingDataStoreOptions[TK](ctx, si, trans)
}

// OpenStreamingDataStore is a convenience function to open an existing data store for use in "streaming data".
func OpenStreamingDataStore[TK btree.Comparable](ctx context.Context, name string, trans sop.Transaction) (*sd.StreamingDataStore[TK], error) {
	return sd.OpenStreamingDataStore[TK](ctx, name, trans)
}

func removeBtreeContents[TK btree.Comparable, TV any](ctx context.Context, s3Client *s3.Client, region string, name string) error {
	const batchSize = 1000
	for {
		trans, err  := NewTransaction(s3Client, sop.ForWriting, -1, true, region)
		if err != nil {
			return err
		}
		trans.Begin()
		btree, err := OpenBtree[TK, TV](ctx, name, trans)
		if err != nil {
			return err
		}
		if btree.Count() == 0 {
			if err := trans.Commit(ctx); err != nil {
				return err
			}
			break
		}
		for i :=0; i < batchSize; i++ {
			if ok, err := btree.First(ctx); !ok || err != nil {
				if err != nil {
					return err
				}
				// Perhaps btree is empty?
				break
			}
			if ok, err := btree.RemoveCurrentItem(ctx); !ok || err != nil {
				if err != nil {
					return err
				}
				if rerr := trans.Rollback(ctx); rerr != nil {
					return fmt.Errorf("failed to RemoveCurrentItem from btree(%s) & failed to rollback, fail err: %v, rollback err: %v", name, err, rerr)
				}
				return fmt.Errorf("failed to RemoveCurrentItem from btree(%s), fail err: %v", name, err)
			}
		}
		if err := trans.Commit(ctx); err != nil {
			return err
		}
		if btree.Count() == 0 {
			break
		}
	}
	return nil
}