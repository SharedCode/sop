package cassandra

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop"
)


type TransactionLog interface {
	// Add a transaction log.
	Add(ctx context.Context, tid sop.UUID, commitFunctionName string, payload interface{}) error
	// Remove all logs of a given transaciton.
	Remove(ctx context.Context, tid sop.UUID) error

	// GetOne will fetch the oldest transaction logs from the backend.
	GetOne(ctx context.Context) (sop.UUID, []sop.KeyValuePair[string, interface{}], error)
}

type transactionLog struct{}

// NewBlobStore instantiates a new BlobStore instance.
func NewTransactionLog() TransactionLog {
	return &transactionLog{}
}

// GetOne fetches a blob from blob table.
func (b *transactionLog) GetOne(ctx context.Context) (sop.UUID, []sop.KeyValuePair[string, interface{}], error) {
	if connection == nil {
		return sop.NilUUID, nil, fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
	}
	// selectStatement := fmt.Sprintf("SELECT node FROM %s.%s WHERE id in (?);", connection.Config.Keyspace, blobTable)
	// qry := connection.Session.Query(selectStatement, gocql.UUID(blobId)).WithContext(ctx)
	// if connection.Config.ConsistencyBook.BlobStoreGet > gocql.Any {
	// 	qry.Consistency(connection.Config.ConsistencyBook.BlobStoreGet)
	// }
	// iter := qry.Iter()
	// var ba []byte
	// for iter.Scan(&ba) {
	// }
	// if err := iter.Close(); err != nil {
	// 	return err
	// }
	// return Marshaler.Unmarshal(ba, target)
	return sop.NilUUID, nil, nil
}

// Add blob(s) to the Blob store.
func (b *transactionLog) Add(ctx context.Context, tid sop.UUID, commitFunctionName string, blobsIds interface{}) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
	}
	// for i := range storesblobs {
	// 	for ii := range storesblobs[i].Blobs {
	// 		ba, err := Marshaler.Marshal(storesblobs[i].Blobs[ii].Value)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		insertStatement := fmt.Sprintf("INSERT INTO %s.%s (id, node) VALUES(?,?);",
	// 			connection.Config.Keyspace, storesblobs[i].BlobTable)
	// 		qry := connection.Session.Query(insertStatement, gocql.UUID(storesblobs[i].Blobs[ii].Key), ba).WithContext(ctx)
	// 		if connection.Config.ConsistencyBook.BlobStoreAdd > gocql.Any {
	// 			qry.Consistency(connection.Config.ConsistencyBook.BlobStoreAdd)
	// 		}
	// 		if err := qry.Exec(); err != nil {
	// 			return err
	// 		}
	// 	}
	// }
	return nil
}

// Remove will delete(non-logged) node records from different Blob stores(node tables).
func (b *transactionLog) Remove(ctx context.Context, tid sop.UUID) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
	}
	// // Delete per blob table the Node "blobs".
	// for _, storeBlobIds := range storesBlobsIds {
	// 	paramQ := make([]string, len(storeBlobIds.Blobs))
	// 	idsAsIntfs := make([]interface{}, len(storeBlobIds.Blobs))
	// 	for i := range storeBlobIds.Blobs {
	// 		paramQ[i] = "?"
	// 		idsAsIntfs[i] = interface{}(gocql.UUID(storeBlobIds.Blobs[i]))
	// 	}
	// 	deleteStatement := fmt.Sprintf("DELETE FROM %s.%s WHERE id in (%v);",
	// 		connection.Config.Keyspace, storeBlobIds.BlobTable, strings.Join(paramQ, ", "))
	// 	qry := connection.Session.Query(deleteStatement, idsAsIntfs...).WithContext(ctx)
	// 	if connection.Config.ConsistencyBook.BlobStoreRemove > gocql.Any {
	// 		qry.Consistency(connection.Config.ConsistencyBook.BlobStoreRemove)
	// 	}
	// 	if err := qry.Exec(); err != nil {
	// 		return err
	// 	}
	// }
	return nil
}
