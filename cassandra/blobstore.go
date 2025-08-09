package cassandra

import (
	"context"
	"fmt"
	"strings"

	"github.com/gocql/gocql"

	"github.com/sharedcode/sop"
)

type blobStore struct{}

// NewBlobStore instantiates a Cassandra-backed implementation of sop.BlobStore.
func NewBlobStore() sop.BlobStore {
	return &blobStore{}
}

// GetOne fetches a blob from the Cassandra blob table for the given ID.
func (b *blobStore) GetOne(ctx context.Context, blobTable string, blobID sop.UUID) ([]byte, error) {
	if connection == nil {
		return nil, fmt.Errorf("cassandra connection is closed; call OpenConnection(config) to open it")
	}
	selectStatement := fmt.Sprintf("SELECT node FROM %s.%s WHERE id in (?);", connection.Config.Keyspace, blobTable)
	qry := connection.Session.Query(selectStatement, gocql.UUID(blobID)).WithContext(ctx)
	if connection.Config.ConsistencyBook.BlobStoreGet > gocql.Any {
		qry.Consistency(connection.Config.ConsistencyBook.BlobStoreGet)
	}
	iter := qry.Iter()
	var ba []byte
	for iter.Scan(&ba) {
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return ba, nil
}

// Add inserts blob records into Cassandra per target blob table.
func (b *blobStore) Add(ctx context.Context, storesblobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	if connection == nil {
		return fmt.Errorf("cassandra connection is closed; call OpenConnection(config) to open it")
	}
	for i := range storesblobs {
		for ii := range storesblobs[i].Blobs {
			ba := storesblobs[i].Blobs[ii].Value
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

// Update updates blob records in Cassandra per target blob table.
func (b *blobStore) Update(ctx context.Context, storesblobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	if connection == nil {
		return fmt.Errorf("cassandra connection is closed; call OpenConnection(config) to open it")
	}
	for i := range storesblobs {
		for ii := range storesblobs[i].Blobs {
			ba := storesblobs[i].Blobs[ii].Value
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

// Remove deletes node blobs from Cassandra for each blob table and list of IDs provided.
func (b *blobStore) Remove(ctx context.Context, storesBlobsIDs []sop.BlobsPayload[sop.UUID]) error {
	if connection == nil {
		return fmt.Errorf("cassandra connection is closed; call OpenConnection(config) to open it")
	}
	// Delete per blob table the Node "blobs".
	for _, storeBlobIDs := range storesBlobsIDs {
		paramQ := make([]string, len(storeBlobIDs.Blobs))
		idsAsIntfs := make([]interface{}, len(storeBlobIDs.Blobs))
		for i := range storeBlobIDs.Blobs {
			paramQ[i] = "?"
			idsAsIntfs[i] = interface{}(gocql.UUID(storeBlobIDs.Blobs[i]))
		}
		deleteStatement := fmt.Sprintf("DELETE FROM %s.%s WHERE id in (%v);",
			connection.Config.Keyspace, storeBlobIDs.BlobTable, strings.Join(paramQ, ", "))
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
