package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gocql/gocql"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
)

// Manage or fetch node blobs request/response payload.
type BlobsPayload[T btree.UUID | sop.KeyValuePair[btree.UUID, interface{}]] struct {
	// Blob store table name.
	BlobTable string
	// Blobs contains the blobs Ids and blobs data for upsert to the store or the blobs Ids to be removed.
	Blobs []T
}

// Returns the total number of UUIDs given a set of blobs (Id) payload.
func GetBlobPayloadCount[T btree.UUID](payloads []BlobsPayload[T]) int {
	total := 0
	for _, p := range payloads {
		total = total + len(p.Blobs)
	}
	return total
}

// BlobStore specifies the backend blob store interface used for storing & managing data blobs.
// Blobs are data that can vary in size and is big enough that they can't be stored in database
// as it will impose performance penalties. This kind of data are typically stored in blob stores
// like AWS S3, or file system, etc...
type BlobStore interface {
	// Get or fetch a blob given an Id.
	GetOne(ctx context.Context, blobTable string, blobId btree.UUID, target interface{}) error
	// Add blobs to store.
	Add(ctx context.Context, blobs ...BlobsPayload[sop.KeyValuePair[btree.UUID, interface{}]]) error
	// Update blobs in store.
	Update(ctx context.Context, blobs ...BlobsPayload[sop.KeyValuePair[btree.UUID, interface{}]]) error
	// Remove blobs in store with given Ids.
	Remove(ctx context.Context, blobsIds ...BlobsPayload[btree.UUID]) error
}

type blobStore struct{}

func NewBlobStore() BlobStore {
	return &blobStore{}
}

// GetOne fetches a blob from blob table.
func (b *blobStore) GetOne(ctx context.Context, blobTable string, blobId btree.UUID, target interface{}) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
	}
	selectStatement := fmt.Sprintf("SELECT node FROM %s.%s WHERE id in (?);", connection.Config.Keyspace, blobTable)
	qry := connection.Session.Query(selectStatement, gocql.UUID(blobId)).WithContext(ctx)
	if connection.Config.ConsistencyBook.BlobStoreGet > gocql.Any {
		qry.Consistency(connection.Config.ConsistencyBook.BlobStoreGet)
	}
	iter := qry.Iter()
	var ba []byte
	for iter.Scan(&ba) {
	}
	if err := iter.Close(); err != nil {
		return err
	}
	return json.Unmarshal(ba, target)
}

func (b *blobStore) Add(ctx context.Context, storesblobs ...BlobsPayload[sop.KeyValuePair[btree.UUID, interface{}]]) error {
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
			qry := connection.Session.Query(insertStatement, gocql.UUID(storesblobs[i].Blobs[ii].Key), ba).WithContext(ctx)
			if connection.Config.ConsistencyBook.BlobStoreAdd > gocql.Any {
				qry.Consistency(connection.Config.ConsistencyBook.BlobStoreAdd)
			}
			if err := qry.Exec(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *blobStore) Update(ctx context.Context, storesblobs ...BlobsPayload[sop.KeyValuePair[btree.UUID, interface{}]]) error {
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
			qry := connection.Session.Query(updateStatement, ba, gocql.UUID(storesblobs[i].Blobs[ii].Key)).WithContext(ctx)
			if connection.Config.ConsistencyBook.BlobStoreUpdate > gocql.Any {
				qry.Consistency(connection.Config.ConsistencyBook.BlobStoreUpdate)
			}
			if err := qry.Exec(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Remove will delete(non-logged & non-transactional) node records from different node tables.
func (b *blobStore) Remove(ctx context.Context, storesBlobsIds ...BlobsPayload[btree.UUID]) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
	}
	// Delete per blob table the Node "blobs".
	for _, storeBlobIds := range storesBlobsIds {
		paramQ := make([]string, len(storeBlobIds.Blobs))
		idsAsIntfs := make([]interface{}, len(storeBlobIds.Blobs))
		for i := range storeBlobIds.Blobs {
			paramQ[i] = "?"
			idsAsIntfs[i] = interface{}(gocql.UUID(storeBlobIds.Blobs[i]))
		}
		deleteStatement := fmt.Sprintf("DELETE FROM %s.%s WHERE id in (%v);",
			connection.Config.Keyspace, storeBlobIds.BlobTable, strings.Join(paramQ, ", "))
		qry := connection.Session.Query(deleteStatement, idsAsIntfs...).WithContext(ctx)
		if connection.Config.ConsistencyBook.BlobStoreRemove > gocql.Any {
			qry.Consistency(connection.Config.ConsistencyBook.BlobStoreRemove)
		}
		if err := qry.Exec(); err != nil {
			return err
		}
	}
	return nil
}
