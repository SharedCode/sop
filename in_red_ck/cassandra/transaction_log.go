package cassandra

import (
	"context"
	"fmt"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_red_ck/redis"
	"github.com/gocql/gocql"
)

const dateHour = "2006-01-02T15"

// This is a good plan, it will work optimally because we are reading entire transaction logs set
// then deleting the entire partition when done. Use consistency of LOCAL_ONE when writing logs.

type TransactionLog interface {
	// Initiate is invoked to signal start of transaction logging & to add the 1st transaction log.
	// In Cassandra backend, this should translate into adding a new transaction by day
	// record(see t_by_day table), and a call to Add method to add the 1st log.
	Initiate(ctx context.Context, tid sop.UUID, commitFunction int, payload interface{}) error
	// Add a transaction log.
	Add(ctx context.Context, tid sop.UUID, commitFunction int, payload interface{}) error
	// Remove all logs of a given transaciton.
	Remove(ctx context.Context, tid sop.UUID) error

	// GetOne will fetch the oldest transaction logs from the backend, older than 1 hour ago.
	// It is capped to an hour ago older because anything newer may still be an in-flight or ongoing transaction.
	GetOne(ctx context.Context) (sop.UUID, []sop.KeyValuePair[int, interface{}], error)
}

type transactionLog struct{
	// Should coordinate via Redis cache. Each date hour should get locked and for "work" by GetOne
	// to increase chances of distribution of cleanup load across machines.
	redisCache redis.Cache
}

// Now lambda to allow unit test to inject replayable time.Now.
var Now = time.Now

// NewBlobStore instantiates a new BlobStore instance.
func NewTransactionLog() TransactionLog {
	return &transactionLog{}
}

// GetOne fetches a blob from blob table.
func (tl *transactionLog) GetOne(ctx context.Context) (sop.UUID, []sop.KeyValuePair[int, interface{}], error) {
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

func (tl *transactionLog) Initiate(ctx context.Context, tid sop.UUID, commitFunction int, payload interface{}) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	date := Now().Format(dateHour)
	insertStatement := fmt.Sprintf("INSERT INTO %s.t_by_hour (date, tid) VALUES(?,?);", connection.Config.Keyspace)
	qry := connection.Session.Query(insertStatement, date, gocql.UUID(tid)).WithContext(ctx).Consistency(gocql.LocalOne)
	if err := qry.Exec(); err != nil {
		return err
	}
	return tl.Add(ctx, tid, commitFunction, payload)
}

// Add blob(s) to the Blob store.
func (tl *transactionLog) Add(ctx context.Context, tid sop.UUID, commitFunction int, payload interface{}) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	ba, err := Marshaler.Marshal(payload)
	if err != nil {
		return err
	}

	insertStatement := fmt.Sprintf("INSERT INTO %s.t_log (id, c_f, c_f_p) VALUES(?,?,?);", connection.Config.Keyspace)
	qry := connection.Session.Query(insertStatement, gocql.UUID(tid), commitFunction, ba).WithContext(ctx).Consistency(gocql.LocalOne)
	if err := qry.Exec(); err != nil {
		return err
	}
	return nil
}

// Remove will delete(non-logged) node records from different Blob stores(node tables).
func (tl *transactionLog) Remove(ctx context.Context, tid sop.UUID) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	// deleteStatement := fmt.Sprintf("DELETE FROM %s.t_log WHERE id = ?;", connection.Config.Keyspace)
	// qry := connection.Session.Query(deleteStatement, gocql.UUID(tid)).WithContext(ctx).Consistency(gocql.LocalOne)
	// if err := qry.Exec(); err != nil {
	// 	return err
	// }
	// deleteStatement = fmt.Sprintf("DELETE tid FROM %s.t_by_hour WHERE date = ? AND tid = ?;", connection.Config.Keyspace)
	// qry = connection.Session.Query(deleteStatement, hour, gocql.UUID(tid)).WithContext(ctx).Consistency(gocql.LocalOne)
	// if err := qry.Exec(); err != nil {
	// 	return err
	// }

	return nil
}
