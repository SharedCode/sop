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

	// GetOne will fetch the oldest transaction logs from the backend, older than 1 hour ago.
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
}

// Now lambda to allow unit test to inject replayable time.Now.
var Now = time.Now

// NewBlobStore instantiates a new BlobStore instance.
func NewTransactionLog() TransactionLog {
	return &transactionLog{
		redisCache: redis.NewClient(),
	}
}

// GetOne fetches a blob from blob table.
func (tl *transactionLog) GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, interface{}], error) {
	const hourBeingProcessed = "HBP"
	duration := time.Duration(12 * time.Hour)

	// TODO: once items older than an hour are tapped out, after "resting"(idle) for an hour, resetting to the oldest hour seems
	// right so we can circle back and forth.

	resetterLK := redis.FormatLockKey("Rst" + hourBeingProcessed)
	var gotReset bool
	if _, err := tl.redisCache.Get(ctx, resetterLK); redis.KeyNotFound(err) {
		gotReset = true
		if err := tl.redisCache.Set(ctx, resetterLK, "fb", duration); err != nil {
			return sop.NilUUID, "", nil, err
		}
	}

	lk := redis.FormatLockKey(hourBeingProcessed)
	if gotReset {
		// Clear the "hour being processed" cache entry every 12 hours so we can ensure to process from the oldest.
		// Take care of timeout, crashed, etc... processing.
		tl.redisCache.Delete(ctx, lk)
	}

	var tid sop.UUID
	var r []sop.KeyValuePair[int, interface{}]
	var gotV sop.KeyValuePair[sop.UUID, string]
	err := tl.redisCache.GetStruct(ctx, lk, &gotV)
	if err != nil && !redis.KeyNotFound(err) {
		return sop.NilUUID, "", nil, err
	}
	ourID := sop.NewUUID()
	for {
		hour := gotV.Value
		var hr2 string
		hr2, tid, err = tl.getOne(ctx, hour)
		if err != nil || tid.IsNil() {
			return sop.NilUUID, "", nil, err
		}
		if hr2 != "" {
			hour = hr2
		}

		err = tl.redisCache.GetStruct(ctx, lk, &gotV)
		if err != nil && !redis.KeyNotFound(err) {
			return sop.NilUUID, "", nil, err
		}
		// If another SOP process already "claimed" this hour(gotV.Value, read from Redis) then loop back to get the next hour.
		if gotV.Value > hour {
			continue
		}
	
		gotV.Key = ourID
		gotV.Value = hour
		if err := tl.redisCache.SetStruct(ctx, lk, &gotV, duration); err != nil {
			return sop.NilUUID, "", nil, err
		}
		wantV := sop.KeyValuePair[sop.UUID, string]{
			Key:   gotV.Key,
			Value: gotV.Value,
		}
		err = tl.redisCache.GetStruct(ctx, lk, &gotV)
		if err != nil {
			return sop.NilUUID, "", nil, err
		}
		if gotV.Key == wantV.Key || gotV.Value == hour {
			r, err = tl.getLogsDetails(ctx, tid)
			return tid, hour, r, err
		}
		if gotV.Value < hour {
			gotV.Value = hour
			if err := tl.redisCache.SetStruct(ctx, lk, &gotV, duration); err != nil {
				return sop.NilUUID, "", nil, err
			}
			r, err = tl.getLogsDetails(ctx, tid)
			return tid, hour, r, err
		}
	}
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
		return tid, nil, nil
	}

	r, err := tl.getLogsDetails(ctx, tid)
	return tid, r, err
}

func (tl *transactionLog) getOne(ctx context.Context, lastHour string) (string, sop.UUID, error) {
	mh, _ := time.Parse(dateHour, Now().Format(dateHour))
	cappedHour := mh.Add(-time.Duration(1 * time.Hour)).Format(dateHour)

	if lastHour >= cappedHour {
		return "", sop.NilUUID, nil
	}

	if connection == nil {
		return "", sop.NilUUID, fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	selectStatement := fmt.Sprintf("SELECT date, tid FROM %s.t_by_hour WHERE date < ? AND date > ? LIMIT 1 ALLOW FILTERING;", connection.Config.Keyspace)
	qry := connection.Session.Query(selectStatement, cappedHour, lastHour).WithContext(ctx).Consistency(gocql.LocalOne)

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

// Remove will delete(non-logged) node records from different Blob stores(node tables).
func (tl *transactionLog) Remove(ctx context.Context, tid sop.UUID, hour string) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	deleteStatement := fmt.Sprintf("DELETE FROM %s.t_log WHERE id = ?;", connection.Config.Keyspace)
	qry := connection.Session.Query(deleteStatement, gocql.UUID(tid)).WithContext(ctx).Consistency(gocql.LocalOne)
	if err := qry.Exec(); err != nil {
		return err
	}
	deleteStatement = fmt.Sprintf("DELETE FROM %s.t_by_hour WHERE date = ? AND tid = ?;", connection.Config.Keyspace)
	qry = connection.Session.Query(deleteStatement, hour, gocql.UUID(tid)).WithContext(ctx).Consistency(gocql.LocalOne)
	if err := qry.Exec(); err != nil {
		return err
	}

	return nil
}
