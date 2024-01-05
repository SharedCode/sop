package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gocql/gocql"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
)

// Manage or fetch node blobs request/response payload.
type BlobsPayload[T btree.UUID | sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]]] struct {
	// Blob store table name.
	BlobTable string
	// Blobs contains the blobs Ids and blobs data for upsert to the store or the blobs Ids to be removed.
	Blobs []T
}

// BlobStore specifies the backend blob store interface used for storing & managing data blobs.
// Blobs are data that can vary in size and is big enough that they can't be stored in database
// as it will impose performance penalties. This kind of data are typically stored in blob stores
// like AWS S3, or file system, etc...
type BlobStore interface {
	// Get or fetch a blob given an Id.
	GetOne(ctx context.Context, blobTable string, blobId btree.UUID, target *btree.Node[interface{}, interface{}]) error
	// Add blobs to store.
	Add(ctx context.Context, blobs ...BlobsPayload[sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]]]) error
	// Update blobs in store.
	Update(ctx context.Context, blobs ...BlobsPayload[sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]]]) error
	// Remove blobs in store with given Ids.
	Remove(ctx context.Context, blobsIds ...BlobsPayload[btree.UUID]) error
}

type blobStore struct {}

func NewBlobStore() blobStore {
	return blobStore{}
}

// GetOne fetches a blob from blob table.
func (b *blobStore) GetOne(ctx context.Context, blobTable string, blobId btree.UUID, target *btree.Node[interface{}, interface{}]) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it.")
	}
	selectStatement := fmt.Sprintf("SELECT node FROM %s.%s WHERE id in (?);", connection.Config.Keyspace, blobTable)
	iter := connection.Session.Query(selectStatement, gocql.UUID(blobId)).WithContext(ctx).Iter()
	var ba []byte
	for iter.Scan(&ba) {}
	if err := iter.Close(); err != nil {
		return err
	}
	return json.Unmarshal(ba, target)
}

func (b *blobStore) Add(ctx context.Context, storesblobs ...BlobsPayload[sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]]]) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it.")
	}
	for _, storeBlobs := range storesblobs {
		for _, blob := range storeBlobs.Blobs {
			ba, err := json.Marshal(blob.Value)
			if err != nil {
				return err
			}
			insertStatement := fmt.Sprintf("INSERT INTO %s.%s (id, node) VALUES(?,?);",
				connection.Config.Keyspace, storeBlobs.BlobTable)
			if err := connection.Session.Query(insertStatement, gocql.UUID(blob.Key), ba).WithContext(ctx).Exec(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *blobStore) Update(ctx context.Context, storesblobs ...BlobsPayload[sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]]]) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it.")
	}
	for _, storeBlobs := range storesblobs {
		for _, blob := range storeBlobs.Blobs {
			ba, err := json.Marshal(blob.Value)
			if err != nil {
				return err
			}
			updateStatement := fmt.Sprintf("UPDATE %s.%s SET node = ? WHERE id = ?;", connection.Config.Keyspace, storeBlobs.BlobTable)
			if err := connection.Session.Query(updateStatement, ba, gocql.UUID(blob.Key)).WithContext(ctx).Exec(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *blobStore) Remove(ctx context.Context, storesBlobsIds ...BlobsPayload[btree.UUID]) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it.")
	}
	for _, storeBlobIds := range storesBlobsIds {
		for _, blobId := range storeBlobIds.Blobs {
			dropBlobTable := fmt.Sprintf("DELETE FROM %s.%s WHERE id = ?;", connection.Config.Keyspace, storeBlobIds.BlobTable)
			if err := connection.Session.Query(dropBlobTable, gocql.UUID(blobId)).WithContext(ctx).Exec(); err != nil {
				return err
			}
		}
	}
	return nil
}
