package fs

import (
	"context"
	"time"

	"github.com/SharedCode/sop"
)

const (
	// DateHourLayout format mask string.
	DateHourLayout = "2006-01-02T15"
)

type transactionLog struct {
	hourLockKey        *sop.LockKey
	cache              sop.Cache
	replicationTracker *replicationTracker
}

// NewTransactionLog instantiates a new TransactionLog instance.
func NewTransactionLog(cache sop.Cache, rt *replicationTracker) sop.TransactionLog {
	return &transactionLog{
		cache:              cache,
		hourLockKey:        cache.CreateLockKeys("HBP")[0],
		replicationTracker: rt,
	}
}

// Add transaction log w/ payload blob to the transaction log file.
func (tl *transactionLog) Add(ctx context.Context, tid sop.UUID, commitFunction int, payload []byte) error {

	// TODO: add an entry (using json?) to transaction log file (named after tid).

	return nil
}

// Remove will delete transaction log(t_log) records given a transaction ID(tid).
func (tl *transactionLog) Remove(ctx context.Context, tid sop.UUID) error {

	// TODO: delete the transaction log file (named after tid).

	return nil
}

// NewUUID generates a new sop UUID, currently a pass-through to google's uuid package.
func (tl *transactionLog) NewUUID() sop.UUID {
	return sop.NewUUID()
}

// GetOne fetches an expired Transaction ID(TID), the hour it was created in and transaction logs for this TID.
func (tl *transactionLog) GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, []byte], error) {
	duration := time.Duration(7 * time.Hour)

	if err := tl.cache.Lock(ctx, duration, tl.hourLockKey); err != nil {
		return sop.NilUUID, "", nil, nil
	}

	hour, tid, err := tl.getOne(ctx)
	if err != nil {
		tl.cache.Unlock(ctx, tl.hourLockKey)
		return sop.NilUUID, hour, nil, err
	}
	if tid.IsNil() {
		// Unlock the hour.
		tl.cache.Unlock(ctx, tl.hourLockKey)
		return sop.NilUUID, "", nil, nil
	}

	r, err := tl.getLogsDetails(ctx, tid)
	if err != nil {
		tl.cache.Unlock(ctx, tl.hourLockKey)
		return sop.NilUUID, "", nil, err
	}
	// Check one more time to remove race condition issue.
	if err := tl.cache.IsLocked(ctx, tl.hourLockKey); err != nil {
		tl.cache.Unlock(ctx, tl.hourLockKey)
		// Just return nils as we can't attain a lock.
		return sop.NilUUID, "", nil, nil
	}
	return sop.UUID(tid), hour, r, nil
}

func (tl *transactionLog) GetLogsDetails(ctx context.Context, hour string) (sop.UUID, []sop.KeyValuePair[int, []byte], error) {
	if hour == "" {
		return sop.NilUUID, nil, nil
	}

	t, err := time.Parse(DateHourLayout, hour)
	if err != nil {
		return sop.NilUUID, nil, err
	}

	// Put a max time of three hours for a given cleanup processor.
	mh, _ := time.Parse(DateHourLayout, sop.Now().Format(DateHourLayout))
	if mh.Sub(t).Hours() > 4 {
		// Unlock the hour to allow open opportunity to claim the next cleanup processing.
		// Capping to 4th hour(Redis cache is set to 7hrs) maintains only one cleaner process at a time.
		tl.cache.Unlock(ctx, tl.hourLockKey)
		return sop.NilUUID, nil, nil
	}

	var tid sop.UUID
	var r []sop.KeyValuePair[int, []byte]

	// hrid := gocql.UUIDFromTime(t)

	// selectStatement := fmt.Sprintf("SELECT id FROM %s.t_log WHERE id < ? LIMIT 1 ALLOW FILTERING;", connection.Config.Keyspace)
	// qry := connection.Session.Query(selectStatement, hrid).WithContext(ctx).Consistency(transactionLoggingConsistency)

	// iter := qry.Iter()
	// for iter.Scan(&tid) {
	// }
	// if err := iter.Close(); err != nil {
	// 	return sop.NilUUID, nil, err
	// }

	// if IsNil(tid) {
	// 	// Unlock the hour.
	// 	tl.cache.Unlock(ctx, tl.hourLockKey)
	// 	return sop.NilUUID, nil, nil
	// }

	// r, err := tl.getLogsDetails(ctx, tid)

	return sop.UUID(tid), r, err
}

func (tl *transactionLog) getOne(ctx context.Context) (string, sop.UUID, error) {
	mh, _ := time.Parse(DateHourLayout, sop.Now().Format(DateHourLayout))

	// 70 minute capped hour as transaction has a max of 60min "commit time". 10 min
	// gap ensures no issue due to overlapping.
	cappedHour := mh.Add(-time.Duration(70 * time.Minute))
	var tid sop.UUID
	//cappedHourTID := gocql.UUIDFromTime(cappedHour)

	// selectStatement := fmt.Sprintf("SELECT id FROM %s.t_log WHERE id < ? LIMIT 1 ALLOW FILTERING;", connection.Config.Keyspace)
	// qry := connection.Session.Query(selectStatement, cappedHourTID).WithContext(ctx).Consistency(transactionLoggingConsistency)

	// iter := qry.Iter()
	// var tid gocql.UUID
	// for iter.Scan(&tid) {
	// }
	// if err := iter.Close(); err != nil {
	// 	return "", NilUUID, err
	// }
	return cappedHour.Format(DateHourLayout), tid, nil
}

func (tl *transactionLog) getLogsDetails(ctx context.Context, tid sop.UUID) ([]sop.KeyValuePair[int, []byte], error) {

	r := make([]sop.KeyValuePair[int, []byte], 0)

	// selectStatement := fmt.Sprintf("SELECT c_f, c_f_p FROM %s.t_log WHERE id = ?;", connection.Config.Keyspace)
	// qry := connection.Session.Query(selectStatement, tid).WithContext(ctx).Consistency(transactionLoggingConsistency)

	// iter := qry.Iter()
	// r := make([]sop.KeyValuePair[int, []byte], 0, iter.NumRows())
	// var c_f int
	// var c_f_p []byte
	// for iter.Scan(&c_f, &c_f_p) {
	// 	r = append(r, sop.KeyValuePair[int, []byte]{
	// 		Key:   c_f,
	// 		Value: c_f_p,
	// 	})
	// }
	// if err := iter.Close(); err != nil {
	// 	return r, err
	// }
	return r, nil
}
