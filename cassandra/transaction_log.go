package cassandra

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/redis"
)

const (
	// DateHourLayout format mask string.
	DateHourLayout = "2006-01-02T15"

	// Transaction logging only needs the least consistency level because we only need the logs to aid in cleanup of
	// "hanged" transactions, which are rare and have no "cleanup" urgency requirement.
	//
	// Transactions are designed to auto cleanup their logs during commit or rollback.
	transactionLoggingConsistency = gocql.LocalOne
)

// Now lambda to allow unit test to inject replayable time.Now.
var Now = time.Now

// NilUUID with gocql.UUID type.
var NilUUID = gocql.UUID(sop.NilUUID)

// Returns true if id is nil or empty UUID, otherwise false.
func IsNil(id gocql.UUID) bool {
	return sop.UUID(id).IsNil()
}

type transactionLog struct {
	dummy
	hourLockKey *sop.LockKey
	cache       sop.Cache
}

// NewBlobStore instantiates a new BlobStore instance.
func NewTransactionLog() sop.TransactionLog {
	c := redis.NewClient()
	return &transactionLog{
		cache:       c,
		hourLockKey: c.CreateLockKeys([]string{"HBP"})[0],
		dummy:       dummy{},
	}
}

// Add blob(s) to the Blob store.
func (tl *transactionLog) Add(ctx context.Context, tid sop.UUID, commitFunction int, payload []byte) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	insertStatement := fmt.Sprintf("INSERT INTO %s.t_log (id, c_f, c_f_p) VALUES(?,?,?);", connection.Config.Keyspace)
	qry := connection.Session.Query(insertStatement, gocql.UUID(tid), commitFunction, payload).WithContext(ctx).Consistency(transactionLoggingConsistency)
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
	qry := connection.Session.Query(deleteStatement, gocql.UUID(tid)).WithContext(ctx).Consistency(transactionLoggingConsistency)
	if err := qry.Exec(); err != nil {
		return err
	}

	return nil
}

// Generates a new UUID based on time, pass-through to gocql.UUID function.
func (tl *transactionLog) NewUUID() sop.UUID {
	return sop.UUID(gocql.UUIDFromTime(Now().UTC()))
}

// GetOne fetches an expired Transaction ID(TID), the hour it was created in and transaction logs for this TID.
func (tl *transactionLog) GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, []byte], error) {
	duration := time.Duration(7 * time.Hour)

	if ok, _, err := tl.cache.Lock(ctx, duration, []*sop.LockKey{tl.hourLockKey}); !ok || err != nil {
		return sop.NilUUID, "", nil, nil
	}

	hour, tid, err := tl.getOne(ctx)
	if err != nil {
		tl.cache.Unlock(ctx, []*sop.LockKey{tl.hourLockKey})
		return sop.NilUUID, hour, nil, err
	}
	if IsNil(tid) {
		// Unlock the hour.
		tl.cache.Unlock(ctx, []*sop.LockKey{tl.hourLockKey})
		return sop.NilUUID, "", nil, nil
	}

	r, err := tl.getLogsDetails(ctx, tid)
	if err != nil {
		tl.cache.Unlock(ctx, []*sop.LockKey{tl.hourLockKey})
		return sop.NilUUID, "", nil, err
	}
	// Check one more time to remove race condition issue.
	if ok, err := tl.cache.IsLocked(ctx, []*sop.LockKey{tl.hourLockKey}); !ok || err != nil {
		tl.cache.Unlock(ctx, []*sop.LockKey{tl.hourLockKey})
		// Just return nils as we can't attain a lock.
		return sop.NilUUID, "", nil, nil
	}
	return sop.UUID(tid), hour, r, nil
}

func (tl *transactionLog) GetOneOfHour(ctx context.Context, hour string) (sop.UUID, []sop.KeyValuePair[int, []byte], error) {
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
		tl.cache.Unlock(ctx, []*sop.LockKey{tl.hourLockKey})
		return sop.NilUUID, nil, nil
	}

	hrid := gocql.UUIDFromTime(t)

	selectStatement := fmt.Sprintf("SELECT id FROM %s.t_log WHERE id < ? LIMIT 1 ALLOW FILTERING;", connection.Config.Keyspace)
	qry := connection.Session.Query(selectStatement, hrid).WithContext(ctx).Consistency(transactionLoggingConsistency)

	iter := qry.Iter()
	var tid gocql.UUID
	for iter.Scan(&tid) {
	}
	if err := iter.Close(); err != nil {
		return sop.NilUUID, nil, err
	}

	if IsNil(tid) {
		// Unlock the hour.
		tl.cache.Unlock(ctx, []*sop.LockKey{tl.hourLockKey})
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
	qry := connection.Session.Query(selectStatement, cappedHourTID).WithContext(ctx).Consistency(transactionLoggingConsistency)

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
	qry := connection.Session.Query(selectStatement, tid).WithContext(ctx).Consistency(transactionLoggingConsistency)

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

func (tl *transactionLog) PriorityLog() sop.TransactionPriorityLog {
	return tl.dummy
}

type dummy struct{}

func (d dummy) IsEnabled() bool {
	return false
}

// Fetch the transaction priority logs details given a tranasction ID.
func (d dummy) Get(ctx context.Context, tid sop.UUID) ([]sop.RegistryPayload[sop.Handle], error) {
	// Nothing to do here because this is only applicable/in use in File System based transaction logger.
	return nil, nil
}

// GetBatch will fetch the oldest transaction (older than 2 min) priority logs details if there are from the
// File System active home folder.
func (d dummy) GetBatch(ctx context.Context, batchSize int) ([]sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]], error) {
	return nil, nil
}

// Log commit changes to its own log file separate than the rest of transaction logs.
// This is a special log file only used during "reinstate" of drives back for replication.
func (d dummy) LogCommitChanges(ctx context.Context, stores []sop.StoreInfo, newRootNodesHandles, addedNodesHandles, updatedNodesHandles, removedNodesHandles []sop.RegistryPayload[sop.Handle]) error {
	return nil
}

func (d dummy) Add(ctx context.Context, tid sop.UUID, payload []byte) error {
	return nil
}

// Remove will delete transaction log(t_log) records given a transaction ID(tid).
func (d dummy) Remove(ctx context.Context, tid sop.UUID) error {
	return nil
}

// Write a backup file for the priority log contents (payload).
func (d dummy) WriteBackup(ctx context.Context, tid sop.UUID, payload []byte) error {
	return nil
}

func (d dummy) RemoveBackup(ctx context.Context, tid sop.UUID) error {
	return nil
}
