package cassandra

import (
	"context"
	"encoding/json"
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
	Initiate(ctx context.Context, tid sop.UUID, commitFunction int, payload interface{}) (string, error)
	// Add a transaction log.
	Add(ctx context.Context, tid sop.UUID, commitFunction int, payload interface{}) error
	// Remove all logs of a given transaciton.
	Remove(ctx context.Context, tid sop.UUID, hour string) error

	// GetOne will fetch the oldest transaction logs from the backend, older than 1 hour ago, mark it so succeeding call
	// will return the next hour and so on, until no more, upon reaching the current hour.
	//
	// GetOne behaves like a job distributor by the hour. SOP uses it to sprinkle/distribute task to cleanup
	// left over resources by unfinished transactions in time. Be it due to crash or host reboot, any transaction
	// temp resource will then age and reach expiration limit, then get cleaned up. This method is used to do distribution.
	//
	// It is capped to an hour ago older because anything newer may still be an in-flight or ongoing transaction.
	GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, interface{}], error)

	// Given a date hour, returns an available for cleanup set of transaction logs with their Transaction ID.
	// Or nils if there is no more needing cleanup for this date hour.
	GetLogsDetails(ctx context.Context, hour string) (sop.UUID, []sop.KeyValuePair[int, interface{}], error)
}

type transactionLog struct {
	// Should coordinate via Redis cache. Each date hour should get locked and for "work" by GetOne
	// to increase chances of distribution of cleanup load across machines.
	redisCache redis.Cache
	hourLockKey *redis.LockKeys
}

// Now lambda to allow unit test to inject replayable time.Now.
var Now = time.Now

// NewBlobStore instantiates a new BlobStore instance.
func NewTransactionLog() TransactionLog {
	return &transactionLog{
		redisCache: redis.NewClient(),
		hourLockKey: redis.CreateLockKeys("HBP")[0],
	}
}

// GetOne fetches an expired Transaction ID(TID), the hour it was created in and transaction logs for this TID.
func (tl *transactionLog) GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, interface{}], error) {
	duration := time.Duration(1 * time.Hour)

	if err := redis.Lock(ctx, duration, tl.hourLockKey); err != nil {
		return sop.NilUUID, "", nil, nil
	}

	hour, tid, err := tl.getOne(ctx)
	if err != nil {
		redis.Unlock(ctx, tl.hourLockKey)
		return sop.NilUUID, hour, nil, err
	}
	r, err := tl.getLogsDetails(ctx, tid)
	if err != nil {
		redis.Unlock(ctx, tl.hourLockKey)
		return sop.NilUUID, hour, nil, err
	}
	// Check one more time to remove race condition issue.
	if err := redis.IsLocked(ctx, tl.hourLockKey); err != nil {
		// Just return nils as we can't attain a lock.
		return sop.NilUUID, hour, nil, nil
	}
	return tid, hour, r, nil
}

func (tl *transactionLog) GetLogsDetails(ctx context.Context, hour string) (sop.UUID, []sop.KeyValuePair[int, interface{}], error) {
	if hour == "" {
		return sop.NilUUID, nil, nil
	}
	if connection == nil {
		return sop.NilUUID, nil, fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	selectStatement := fmt.Sprintf("SELECT tid FROM %s.t_by_hour WHERE date = ? LIMIT 1 ALLOW FILTERING;", connection.Config.Keyspace)
	qry := connection.Session.Query(selectStatement, hour).WithContext(ctx).Consistency(gocql.LocalOne)

	iter := qry.Iter()
	var gtid gocql.UUID
	for iter.Scan(&gtid) {
	}
	if err := iter.Close(); err != nil {
		return sop.NilUUID, nil, err
	}

	tid := sop.UUID(gtid)
	if tid.IsNil() {
		// Unlock the hour.
		redis.Unlock(ctx, tl.hourLockKey)
		return tid, nil, nil
	}

	r, err := tl.getLogsDetails(ctx, tid)
	return tid, r, err
}

func (tl *transactionLog) getOne(ctx context.Context) (string, sop.UUID, error) {
	mh, _ := time.Parse(dateHour, Now().Format(dateHour))
	cappedHour := mh.Add(-time.Duration(1 * time.Hour)).Format(dateHour)

	if connection == nil {
		return "", sop.NilUUID, fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	selectStatement := fmt.Sprintf("SELECT date, tid FROM %s.t_by_hour WHERE date < ? LIMIT 1 ALLOW FILTERING;", connection.Config.Keyspace)
	qry := connection.Session.Query(selectStatement, cappedHour).WithContext(ctx).Consistency(gocql.LocalOne)

	iter := qry.Iter()
	var nextHour string
	var tid gocql.UUID
	for iter.Scan(&nextHour, &tid) {
	}
	if err := iter.Close(); err != nil {
		return "", sop.NilUUID, err
	}
	return nextHour, sop.UUID(tid), nil
}

func (tl *transactionLog) getLogsDetails(ctx context.Context, tid sop.UUID) ([]sop.KeyValuePair[int, interface{}], error) {
	if connection == nil {
		return nil, fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	selectStatement := fmt.Sprintf("SELECT c_f, c_f_p FROM %s.t_log WHERE id = ?;", connection.Config.Keyspace)
	qry := connection.Session.Query(selectStatement, gocql.UUID(tid)).WithContext(ctx).Consistency(gocql.LocalOne)

	iter := qry.Iter()
	r := make([]sop.KeyValuePair[int, interface{}], 0, iter.NumRows())
	var c_f int
	var c_f_p []byte
	for iter.Scan(&c_f, &c_f_p) {
		var t interface{}
		if c_f_p != nil {
			if err := json.Unmarshal(c_f_p, &t); err != nil {
				return nil, err
			}
		}
		r = append(r, sop.KeyValuePair[int, interface{}]{
			Key:   c_f,
			Value: t,
		})
	}
	if err := iter.Close(); err != nil {
		return r, err
	}
	return r, nil
}

func (tl *transactionLog) Initiate(ctx context.Context, tid sop.UUID, commitFunction int, payload interface{}) (string, error) {
	if connection == nil {
		return "", fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	date := Now().Format(dateHour)
	insertStatement := fmt.Sprintf("INSERT INTO %s.t_by_hour (date, tid) VALUES(?,?);", connection.Config.Keyspace)
	qry := connection.Session.Query(insertStatement, date, gocql.UUID(tid)).WithContext(ctx).Consistency(gocql.LocalOne)
	if err := qry.Exec(); err != nil {
		return date, err
	}
	return date, tl.Add(ctx, tid, commitFunction, payload)
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

// Remove will delete transaction log by hour(t_by_hour) & transaction log(t_log) records.
func (tl *transactionLog) Remove(ctx context.Context, tid sop.UUID, hour string) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	var deleteStatement string
	if !tid.IsNil() {
		deleteStatement = fmt.Sprintf("DELETE FROM %s.t_log WHERE id = ?;", connection.Config.Keyspace)
		qry := connection.Session.Query(deleteStatement, gocql.UUID(tid)).WithContext(ctx).Consistency(gocql.LocalOne)
		if err := qry.Exec(); err != nil {
			return err
		}
		deleteStatement = fmt.Sprintf("DELETE tid FROM %s.t_by_hour WHERE date = ? IF tid = ?;", connection.Config.Keyspace)
		qry = connection.Session.Query(deleteStatement, hour, gocql.UUID(tid)).WithContext(ctx).Consistency(gocql.LocalOne)
		if err := qry.Exec(); err != nil {
			return err
		}
		return nil
	}
	deleteStatement = fmt.Sprintf("DELETE FROM %s.t_by_hour WHERE date = ?;", connection.Config.Keyspace)
	qry := connection.Session.Query(deleteStatement, hour).WithContext(ctx).Consistency(gocql.LocalOne)
	if err := qry.Exec(); err != nil {
		return err
	}
	// This occurs when there is no more TID records, thus, safe to signal proceed to process next expired hour.
	redis.Unlock(ctx, tl.hourLockKey)

	return nil
}
