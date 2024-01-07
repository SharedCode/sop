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

type blobStore struct{}

func NewBlobStore() BlobStore {
	return &blobStore{}
}

// GetOne fetches a blob from blob table.
func (b *blobStore) GetOne(ctx context.Context, blobTable string, blobId btree.UUID, target *btree.Node[interface{}, interface{}]) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
	}
	selectStatement := fmt.Sprintf("SELECT node FROM %s.%s WHERE id in (?);", connection.Config.Keyspace, blobTable)
	iter := connection.Session.Query(selectStatement, gocql.UUID(blobId)).WithContext(ctx).Iter()
	var s string
	for iter.Scan(&s) {
	}
	if err := iter.Close(); err != nil {
		return err
	}
	return json.Unmarshal([]byte(s), target)
}

func (b *blobStore) Add(ctx context.Context, storesblobs ...BlobsPayload[sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]]]) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
	}
	for i := range storesblobs {
		for ii := range storesblobs[i].Blobs {
			ba, err := json.Marshal(storesblobs[i].Blobs[ii].Value)
			if err != nil {
				return err
			}
			insertStatement := fmt.Sprintf("INSERT INTO %s.%s (id, node) VALUES(?,?);",
				connection.Config.Keyspace, storesblobs[i].BlobTable)
			if err := connection.Session.Query(insertStatement, gocql.UUID(storesblobs[i].Blobs[ii].Key), string(ba)).WithContext(ctx).Exec(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *blobStore) Update(ctx context.Context, storesblobs ...BlobsPayload[sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]]]) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
	}
	for i := range storesblobs {
		for ii := range storesblobs[i].Blobs {
			ba, err := json.Marshal(storesblobs[i].Blobs[ii].Value)
			if err != nil {
				return err
			}
			updateStatement := fmt.Sprintf("UPDATE %s.%s SET node = ? WHERE id = ?;", connection.Config.Keyspace, storesblobs[i].BlobTable)
			if err := connection.Session.Query(updateStatement, string(ba), gocql.UUID(storesblobs[i].Blobs[ii].Key)).WithContext(ctx).Exec(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *blobStore) Remove(ctx context.Context, storesBlobsIds ...BlobsPayload[btree.UUID]) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
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
