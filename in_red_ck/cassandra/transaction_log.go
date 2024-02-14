package cassandra

import (
	"context"
	"fmt"
	"time"

	"github.com/SharedCode/sop"
)

// This is a good plan, it will work optimally because we are reading entire transaction logs set
// then deleting the entire partition when done. Use consistency of LOCAL_ONE when writing logs.

type TransactionLog interface {
	// Initiate is invoked to signal start of transaction logging & to add the 1st transaction log.
	// In Cassandra backend, this should translate into adding a new transaction by day
	// record(see t_by_day table), and a call to Add method to add the 1st log.
	Initiate(ctx context.Context, tid sop.UUID, commitFunctionName string, payload interface{}) error
	// Add a transaction log.
	Add(ctx context.Context, tid sop.UUID, commitFunctionName string, payload interface{}) error
	// Remove all logs of a given transaciton.
	Remove(ctx context.Context, tid sop.UUID) error

	// GetOne will fetch the oldest transaction logs from the backend, older than 1 hour ago.
	// It is capped to an hour ago older because anything newer may still be an in-flight or ongoing transaction.
	GetOne(ctx context.Context) (sop.UUID, []sop.KeyValuePair[string, interface{}], error)
}

type transactionLog struct{}

// Now lambda to allow unit test to inject replayable time.Now.
var Now = time.Now

// NewBlobStore instantiates a new BlobStore instance.
func NewTransactionLog() TransactionLog {
	return &transactionLog{}
}

// GetOne fetches a blob from blob table.
func (tl *transactionLog) GetOne(ctx context.Context) (sop.UUID, []sop.KeyValuePair[string, interface{}], error) {
	if connection == nil {
		return sop.NilUUID, nil, fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}
	// selectStatement := fmt.Sprintf("SELECT node FROM %s.%s WHERE id in (?);", connection.Config.Keyspace, blobTable)
	// qry := connection.Session.Query(selectStatement, gocql.UUID(blobID)).WithContext(ctx)
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

func (tl *transactionLog) Initiate(ctx context.Context, tid sop.UUID, commitFunctionName string, payload interface{}) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
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

// Add blob(s) to the Blob store.
func (tl *transactionLog) Add(ctx context.Context, tid sop.UUID, commitFunctionName string, payload interface{}) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
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
func (tl *transactionLog) Remove(ctx context.Context, tid sop.UUID) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}
	// // Delete per blob table the Node "blobs".
	// for _, storeBlobIDs := range storesBlobsIDs {
	// 	paramQ := make([]string, len(storeBlobIDs.Blobs))
	// 	idsAsIntfs := make([]interface{}, len(storeBlobIDs.Blobs))
	// 	for i := range storeBlobIDs.Blobs {
	// 		paramQ[i] = "?"
	// 		idsAsIntfs[i] = interface{}(gocql.UUID(storeBlobIDs.Blobs[i]))
	// 	}
	// 	deleteStatement := fmt.Sprintf("DELETE FROM %s.%s WHERE id in (%v);",
	// 		connection.Config.Keyspace, storeBlobIDs.BlobTable, strings.Join(paramQ, ", "))
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
