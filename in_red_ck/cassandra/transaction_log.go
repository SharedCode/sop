package cassandra

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_red_ck/redis"
)

const dateHour = "2006-01-02T15"

// NilUUID with gocql.UUID type.
var NilUUID = gocql.UUID(sop.NilUUID)

// This is a good plan, it will work optimally because we are reading entire transaction logs set
// then deleting the entire partition when done. Use consistency of LOCAL_ONE when writing logs.

type TransactionLog interface {
	// Add a transaction log.
	Add(ctx context.Context, tid gocql.UUID, commitFunction int, payload interface{}) error
	// Remove all logs of a given transaciton.
	Remove(ctx context.Context, tid gocql.UUID) error

	// GetOne will fetch the oldest transaction logs from the backend, older than 1 hour ago, mark it so succeeding call
	// will return the next hour and so on, until no more, upon reaching the current hour.
	//
	// GetOne behaves like a job distributor by the hour. SOP uses it to sprinkle/distribute task to cleanup
	// left over resources by unfinished transactions in time. Be it due to crash or host reboot, any transaction
	// temp resource will then age and reach expiration limit, then get cleaned up. This method is used to do distribution.
	//
	// It is capped to an hour ago older because anything newer may still be an in-flight or ongoing transaction.
	GetOne(ctx context.Context) (gocql.UUID, string, []sop.KeyValuePair[int, interface{}], error)

	// Given a date hour, returns an available for cleanup set of transaction logs with their Transaction ID.
	// Or nils if there is no more needing cleanup for this date hour.
	GetLogsDetails(ctx context.Context, hour string) (gocql.UUID, []sop.KeyValuePair[int, interface{}], error)
}

type transactionLog struct {
	// Should coordinate via Redis cache. Each date hour should get locked and for "work" by GetOne
	// to increase chances of distribution of cleanup load across machines.
	redisCache  redis.Cache
	hourLockKey *redis.LockKeys
}

// Returns true if id is nil or empty UUID, otherwise false.
func IsNil(id gocql.UUID) bool {
	return sop.UUID(id).IsNil()
}

// Now lambda to allow unit test to inject replayable time.Now.
var Now = time.Now

// NewBlobStore instantiates a new BlobStore instance.
func NewTransactionLog() TransactionLog {
	return &transactionLog{
		redisCache:  redis.NewClient(),
		hourLockKey: redis.CreateLockKeys("HBP")[0],
	}
}

// GetOne fetches an expired Transaction ID(TID), the hour it was created in and transaction logs for this TID.
func (tl *transactionLog) GetOne(ctx context.Context) (gocql.UUID, string, []sop.KeyValuePair[int, interface{}], error) {
	duration := time.Duration(7 * time.Hour)

	if err := redis.Lock(ctx, duration, tl.hourLockKey); err != nil {
		return NilUUID, "", nil, nil
	}

	hour, tid, err := tl.getOne(ctx)
	if err != nil {
		redis.Unlock(ctx, tl.hourLockKey)
		return NilUUID, hour, nil, err
	}
	if IsNil(tid) {
		// Unlock the hour.
		redis.Unlock(ctx, tl.hourLockKey)
		return NilUUID, "", nil, nil
	}

	r, err := tl.getLogsDetails(ctx, tid)
	if err != nil {
		redis.Unlock(ctx, tl.hourLockKey)
		return NilUUID, "", nil, err
	}
	// Check one more time to remove race condition issue.
	if err := redis.IsLocked(ctx, tl.hourLockKey); err != nil {
		// Just return nils as we can't attain a lock.
		return NilUUID, "", nil, nil
	}
	return tid, hour, r, nil
}

func (tl *transactionLog) GetLogsDetails(ctx context.Context, hour string) (gocql.UUID, []sop.KeyValuePair[int, interface{}], error) {
	if hour == "" {
		return NilUUID, nil, nil
	}
	if connection == nil {
		return NilUUID, nil, fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	t, err := time.Parse(dateHour, hour)
	if err != nil {
		return NilUUID, nil, err
	}

	// Put a max time of three hours for a given cleanup processor.
	mh, _ := time.Parse(dateHour, Now().Format(dateHour))
	if mh.Sub(t).Hours() > 4 {
		// Unlock the hour to allow open opportunity to claim the next cleanup processing.
		// Capping to 4th hour(Redis cache is set to 7hrs) maintains only one cleaner process at a time.
		redis.Unlock(ctx, tl.hourLockKey)
		return NilUUID, nil, nil
	}

	hrid := gocql.UUIDFromTime(t)

	selectStatement := fmt.Sprintf("SELECT id FROM %s.t_log WHERE id < ? LIMIT 1 ALLOW FILTERING;", connection.Config.Keyspace)
	qry := connection.Session.Query(selectStatement, hrid).WithContext(ctx).Consistency(gocql.LocalOne)

	iter := qry.Iter()
	var tid gocql.UUID
	for iter.Scan(&tid) {
	}
	if err := iter.Close(); err != nil {
		return NilUUID, nil, err
	}

	if IsNil(tid) {
		// Unlock the hour.
		redis.Unlock(ctx, tl.hourLockKey)
		return NilUUID, nil, nil
	}

	r, err := tl.getLogsDetails(ctx, tid)
	return tid, r, err
}

func (tl *transactionLog) getOne(ctx context.Context) (string, gocql.UUID, error) {
	mh, _ := time.Parse(dateHour, Now().Format(dateHour))
	cappedHour := mh.Add(-time.Duration(1 * time.Hour))
	cappedHourTID := gocql.UUIDFromTime(cappedHour)

	if connection == nil {
		return "", NilUUID, fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	selectStatement := fmt.Sprintf("SELECT id FROM %s.t_log WHERE id < ? LIMIT 1 ALLOW FILTERING;", connection.Config.Keyspace)
	qry := connection.Session.Query(selectStatement, cappedHourTID).WithContext(ctx).Consistency(gocql.LocalOne)

	iter := qry.Iter()
	var tid gocql.UUID
	for iter.Scan(&tid) {
	}
	if err := iter.Close(); err != nil {
		return "", NilUUID, err
	}
	return cappedHour.Format(dateHour), tid, nil
}

func (tl *transactionLog) getLogsDetails(ctx context.Context, tid gocql.UUID) ([]sop.KeyValuePair[int, interface{}], error) {
	if connection == nil {
		return nil, fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	selectStatement := fmt.Sprintf("SELECT c_f, c_f_p FROM %s.t_log WHERE id = ?;", connection.Config.Keyspace)
	qry := connection.Session.Query(selectStatement, tid).WithContext(ctx).Consistency(gocql.LocalOne)

	iter := qry.Iter()
	r := make([]sop.KeyValuePair[int, interface{}], 0, iter.NumRows())
	var c_f int
	var c_f_p []byte
	for iter.Scan(&c_f, &c_f_p) {
		var t interface{}
		if c_f_p != nil {
			if err := Marshaler.Unmarshal(c_f_p, &t); err != nil {
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

// Add blob(s) to the Blob store.
func (tl *transactionLog) Add(ctx context.Context, tid gocql.UUID, commitFunction int, payload interface{}) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	ba, err := Marshaler.Marshal(payload)
	if err != nil {
		return err
	}

	insertStatement := fmt.Sprintf("INSERT INTO %s.t_log (id, c_f, c_f_p) VALUES(?,?,?);", connection.Config.Keyspace)
	qry := connection.Session.Query(insertStatement, tid, commitFunction, ba).WithContext(ctx).Consistency(gocql.LocalOne)
	if err := qry.Exec(); err != nil {
		return err
	}
	return nil
}

// Remove will delete transaction log(t_log) records given a transaction ID(tid).
func (tl *transactionLog) Remove(ctx context.Context, tid gocql.UUID) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	deleteStatement := fmt.Sprintf("DELETE FROM %s.t_log WHERE id = ?;", connection.Config.Keyspace)
	qry := connection.Session.Query(deleteStatement, tid).WithContext(ctx).Consistency(gocql.LocalOne)
	if err := qry.Exec(); err != nil {
		return err
	}

	return nil
}
