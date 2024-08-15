package cassandra

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_red_ck/redis"
)

// DateHourLayout format mask string.
const DateHourLayout = "2006-01-02T15"

// NilUUID with gocql.UUID type.
var NilUUID = gocql.UUID(sop.NilUUID)

// This is a good plan, it will work optimally because we are reading entire transaction logs set
// then deleting the entire partition when done. Use consistency of LOCAL_ONE when writing logs.

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
func NewTransactionLog() sop.TransactionLog {
	return &transactionLog{
		redisCache:  redis.NewClient(),
		hourLockKey: redis.CreateLockKeys("HBP")[0],
	}
}

// GetOne fetches an expired Transaction ID(TID), the hour it was created in and transaction logs for this TID.
func (tl *transactionLog) GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, []byte], error) {
	duration := time.Duration(7 * time.Hour)

	if err := redis.Lock(ctx, duration, tl.hourLockKey); err != nil {
		return sop.NilUUID, "", nil, nil
	}

	hour, tid, err := tl.getOne(ctx)
	if err != nil {
		redis.Unlock(ctx, tl.hourLockKey)
		return sop.NilUUID, hour, nil, err
	}
	if IsNil(tid) {
		// Unlock the hour.
		redis.Unlock(ctx, tl.hourLockKey)
		return sop.NilUUID, "", nil, nil
	}

	r, err := tl.getLogsDetails(ctx, tid)
	if err != nil {
		redis.Unlock(ctx, tl.hourLockKey)
		return sop.NilUUID, "", nil, err
	}
	// Check one more time to remove race condition issue.
	if err := redis.IsLocked(ctx, tl.hourLockKey); err != nil {
		// Just return nils as we can't attain a lock.
		return sop.NilUUID, "", nil, nil
	}
	return sop.UUID(tid), hour, r, nil
}

func (tl *transactionLog) GetLogsDetails(ctx context.Context, hour string) (sop.UUID, []sop.KeyValuePair[int, []byte], error) {
	if hour == "" {
		return sop.NilUUID, nil, nil
	}
	if connection == nil {
		return sop.NilUUID, nil, fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	t, err := time.Parse(DateHourLayout, hour)
	if err != nil {
		return sop.NilUUID, nil, err
	}

	// Put a max time of three hours for a given cleanup processor.
	mh, _ := time.Parse(DateHourLayout, Now().Format(DateHourLayout))
	if mh.Sub(t).Hours() > 4 {
		// Unlock the hour to allow open opportunity to claim the next cleanup processing.
		// Capping to 4th hour(Redis cache is set to 7hrs) maintains only one cleaner process at a time.
		redis.Unlock(ctx, tl.hourLockKey)
		return sop.NilUUID, nil, nil
	}

	hrid := gocql.UUIDFromTime(t)

	selectStatement := fmt.Sprintf("SELECT id FROM %s.t_log WHERE id < ? LIMIT 1 ALLOW FILTERING;", connection.Config.Keyspace)
	qry := connection.Session.Query(selectStatement, hrid).WithContext(ctx).Consistency(gocql.LocalOne)

	iter := qry.Iter()
	var tid gocql.UUID
	for iter.Scan(&tid) {
	}
	if err := iter.Close(); err != nil {
		return sop.NilUUID, nil, err
	}

	if IsNil(tid) {
		// Unlock the hour.
		redis.Unlock(ctx, tl.hourLockKey)
		return sop.NilUUID, nil, nil
	}

	r, err := tl.getLogsDetails(ctx, tid)
	return sop.UUID(tid), r, err
}

func (tl *transactionLog) getOne(ctx context.Context) (string, gocql.UUID, error) {
	mh, _ := time.Parse(DateHourLayout, Now().Format(DateHourLayout))
	// 70 minute capped hour as transaction has a max of 60min "commit time". 10 min
	// gap ensures no issue due to overlapping.
	cappedHour := mh.Add(-time.Duration(70 * time.Minute))
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
	return cappedHour.Format(DateHourLayout), tid, nil
}

func (tl *transactionLog) getLogsDetails(ctx context.Context, tid gocql.UUID) ([]sop.KeyValuePair[int, []byte], error) {
	if connection == nil {
		return nil, fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	selectStatement := fmt.Sprintf("SELECT c_f, c_f_p FROM %s.t_log WHERE id = ?;", connection.Config.Keyspace)
	qry := connection.Session.Query(selectStatement, tid).WithContext(ctx).Consistency(gocql.LocalOne)

	iter := qry.Iter()
	r := make([]sop.KeyValuePair[int, []byte], 0, iter.NumRows())
	var c_f int
	var c_f_p []byte
	for iter.Scan(&c_f, &c_f_p) {
		r = append(r, sop.KeyValuePair[int, []byte]{
			Key:   c_f,
			Value: c_f_p,
		})
	}
	if err := iter.Close(); err != nil {
		return r, err
	}
	return r, nil
}

// Add blob(s) to the Blob store.
func (tl *transactionLog) Add(ctx context.Context, tid sop.UUID, commitFunction int, payload []byte) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	insertStatement := fmt.Sprintf("INSERT INTO %s.t_log (id, c_f, c_f_p) VALUES(?,?,?);", connection.Config.Keyspace)
	qry := connection.Session.Query(insertStatement, gocql.UUID(tid), commitFunction, payload).WithContext(ctx).Consistency(gocql.LocalOne)
	if err := qry.Exec(); err != nil {
		return err
	}
	return nil
}

// Remove will delete transaction log(t_log) records given a transaction ID(tid).
func (tl *transactionLog) Remove(ctx context.Context, tid sop.UUID) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	deleteStatement := fmt.Sprintf("DELETE FROM %s.t_log WHERE id = ?;", connection.Config.Keyspace)
	qry := connection.Session.Query(deleteStatement, gocql.UUID(tid)).WithContext(ctx).Consistency(gocql.LocalOne)
	if err := qry.Exec(); err != nil {
		return err
	}

	return nil
}
