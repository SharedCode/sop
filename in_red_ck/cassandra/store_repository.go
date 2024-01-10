package cassandra

import (
	"context"
	"fmt"
	log "log/slog"
	"strings"
	"time"

	"github.com/gocql/gocql"
	retry "github.com/sethvargo/go-retry"

	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_memory"
	"github.com/SharedCode/sop/in_red_c/redis"
)

// TODO: when need arise, move these interfaces to a common package, but keep them for now
// in package where they are implemented, 'just because we wanted to keep changes minimal,
// and driven by needs.

// StoreRepository interface specifies the store repository.
type StoreRepository interface {
	// Fetch store info with name.
	Get(context.Context, ...string) ([]btree.StoreInfo, error)
	// Add store info.
	Add(context.Context, ...btree.StoreInfo) error
	// Update store info. Update should also merge the Count of items between the incoming store info
	// and the target store info on the backend, as they may differ. It should use StoreInfo.CountDelta to reconcile the two.
	Update(context.Context, ...btree.StoreInfo) error
	// Remove store info with name.
	Remove(context.Context, ...string) error
}

type storeRepository struct {
	redisCache redis.Cache
}

// NewStoreRepository manages the StoreInfo in Cassandra table.
func NewStoreRepository() StoreRepository {
	return &storeRepository{
		redisCache: redis.NewClient(),
	}
}

// TODO: finalize Consistency levels to use in below CRUD methods.

var storeCacheDuration = time.Duration(2 * time.Hour)

// SetStoreCacheDuration allows store repository cache duration to get set globally.
func SetStoreCacheDuration(duration time.Duration) {
	if duration < time.Minute {
		duration = time.Duration(30 * time.Minute)
	}
	storeCacheDuration = duration
}

// Add a new store record, create a new Virtual ID registry and node blob tables.
func (sr *storeRepository) Add(ctx context.Context, stores ...btree.StoreInfo) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
	}
	insertStatement := fmt.Sprintf("INSERT INTO %s.store (name, root_id, slot_count, count, unique, des, reg_tbl, blob_tbl, ts, vdins, llb) VALUES(?,?,?,?,?,?,?,?,?,?,?);", connection.Config.Keyspace)
	for _, s := range stores {
		// Add a new store record.
		if err := connection.Session.Query(insertStatement, s.Name, gocql.UUID(s.RootNodeId), s.SlotLength, s.Count, s.IsUnique, s.Description,
			s.RegistryTable, s.BlobTable, s.Timestamp, s.IsValueDataInNodeSegment, s.LeafLoadBalancing).WithContext(ctx).Exec(); err != nil {
			return err
		}
		// Create a new Blob table.
		createNewBlobTable := fmt.Sprintf("CREATE TABLE %s.%s(id UUID PRIMARY KEY, node text);", connection.Config.Keyspace, s.BlobTable)
		if err := connection.Session.Query(createNewBlobTable).WithContext(ctx).Exec(); err != nil {
			return err
		}
		// Create a new Virtual ID registry table.
		createNewRegistry := fmt.Sprintf("CREATE TABLE %s.%s(lid UUID PRIMARY KEY, is_idb boolean, p_ida UUID, p_idb UUID, ts bigint, wip_ts bigint, is_del boolean);",
			connection.Config.Keyspace, s.RegistryTable)
		if err := connection.Session.Query(createNewRegistry).WithContext(ctx).Exec(); err != nil {
			return err
		}
		// Tolerate error in Redis caching.
		if err := sr.redisCache.SetStruct(ctx, s.Name, &s, storeCacheDuration); err != nil {
			log.Error(fmt.Sprintf("StoreRepository Add failed (redis setstruct), details: %v", err))
		}
	}
	return nil
}

// Update enforces so only the Store's Count & timestamp can get updated.
func (sr *storeRepository) Update(ctx context.Context, stores ...btree.StoreInfo) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
	}

	// Sort the stores info so we can commit them in same sort order across transactions,
	// thus, reduced chance of deadlock.
	b3 := in_memory.NewBtree[string, btree.StoreInfo](true)
	for i := range stores {
		b3.Add(stores[i].Name, stores[i])
	}
	b3.First()
	keys := make([]string, len(stores))
	i := 0
	for {
		keys[i] = b3.GetCurrentKey()
		stores[i] = b3.GetCurrentValue()
		if !b3.Next() {
			break
		}
		i++
	}

	// Create lock Ids that we can use to logically lock and prevent other updates.
	lockKeys := redis.CreateLockKeys(keys)

	// 15 minutes to lock, merge/update details then unlock.
	duration := time.Duration(15 * time.Minute)
	b := retry.NewFibonacci(1 * time.Second)

	// Lock all keys.
	if err := retry.Do(ctx, retry.WithMaxRetries(3, b), func(ctx context.Context) error {
		return redis.Lock(ctx, duration, lockKeys...)
	}); err != nil {
		// Unlock all keys since we failed locking them.
		redis.Unlock(ctx, lockKeys...)
		return err
	}

	updateStatement := fmt.Sprintf("UPDATE %s.store SET count = ?, ts = ? WHERE name = ?;", connection.Config.Keyspace)
	undo := func(bus []btree.StoreInfo) {
		// Attempt to undo changes, 'ignores error as it is a last attempt to cleanup.
		for ii := 0; ii < len(bus); ii++ {
			connection.Session.Query(updateStatement, bus[ii].Count, bus[ii].Timestamp,
				bus[ii].Name).Exec()
		}
	}

	beforeUpdateStores := make([]btree.StoreInfo, 0, len(stores))
	// Unlock all keys before going out of scope.
	defer redis.Unlock(ctx, lockKeys...)

	for i := range stores {
		sis, err := sr.Get(ctx, stores[i].Name)
		if len(sis) == 0 {
			undo(beforeUpdateStores)
			return err
		}
		beforeUpdateStores = append(beforeUpdateStores, sis...)

		si := sis[0]
		if si.Timestamp > stores[i].Timestamp {
			// Merge or apply the "count delta".
			stores[i].Count = si.Count + stores[i].CountDelta
			stores[i].Timestamp = si.Timestamp
		}

		// Update store record.
		if err := connection.Session.Query(updateStatement, stores[i].Count, stores[i].Timestamp, stores[i].Name).Exec(); err != nil {
			// Undo changes.
			undo(beforeUpdateStores)
			return err
		}
	}
	// Update redis since we've successfully updated Cassandra Store table.
	for i := range stores {
		// Tolerate redis error since we've successfully updated the master table.
		if err := sr.redisCache.SetStruct(ctx, stores[i].Name, &stores[i], storeCacheDuration); err != nil {
			log.Error("StoreRepository Update (redis setstruct) failed, details: %v", err)
		}
	}

	return nil
}

func (sr *storeRepository) Get(ctx context.Context, names ...string) ([]btree.StoreInfo, error) {
	if connection == nil {
		return nil, fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
	}
	stores := make([]btree.StoreInfo, 0, len(names))
	// Format some variadic ? and convert to interface the names param.
	namesAsIntf := make([]interface{}, 0, len(names))
	paramQ := make([]string, 0, len(names))
	for i := range names {
		store := btree.StoreInfo{}
		if err := sr.redisCache.GetStruct(ctx, names[i], &store); err != nil {
			if !redis.KeyNotFound(err) {
				log.Error(fmt.Sprintf("StoreRepository Get (redis getstruct) failed, details: %v", err))
			}
			paramQ = append(paramQ, "?")
			namesAsIntf = append(namesAsIntf, interface{}(names[i]))
			continue
		}
		stores = append(stores, store)
	}
	if len(paramQ) == 0 {
		return stores, nil
	}
	selectStatement := fmt.Sprintf("SELECT name, root_id, slot_count, count, unique, des, reg_tbl, blob_tbl, ts, vdins, llb, is_del FROM %s.store  WHERE name in (%v);",
		connection.Config.Keyspace, strings.Join(paramQ, ", "))
	iter := connection.Session.Query(selectStatement, namesAsIntf...).WithContext(ctx).Iter()
	store := btree.StoreInfo{}
	var rid gocql.UUID
	for iter.Scan(&store.Name, &rid, &store.SlotLength, &store.Count, &store.IsUnique,
		&store.Description, &store.RegistryTable, &store.BlobTable, &store.Timestamp, &store.IsValueDataInNodeSegment, &store.LeafLoadBalancing, &store.IsDeleted) {
		store.RootNodeId = btree.UUID(rid)

		if err := sr.redisCache.SetStruct(ctx, store.Name, &store, storeCacheDuration); err != nil {
			log.Error(fmt.Sprintf("StoreRepository Get (redis setstruct) failed, details: %v", err))
		}

		stores = append(stores, store)
		store = btree.StoreInfo{}
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	return stores, nil
}

func (sr *storeRepository) Remove(ctx context.Context, names ...string) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
	}
	// Format some variadic ? and convert to interface the names param.
	namesAsIntf := make([]interface{}, len(names))
	paramQ := make([]string, len(names))
	for i := range names {
		paramQ[i] = "?"
		namesAsIntf[i] = interface{}(names[i])
	}
	deleteStatement := fmt.Sprintf("DELETE FROM %s.store WHERE name in (%v);", connection.Config.Keyspace, strings.Join(paramQ, ", "))
	if err := connection.Session.Query(deleteStatement, namesAsIntf...).WithContext(ctx).Exec(); err != nil {
		return err
	}

	// Delete the store "count" in Redis.
	for i := range names {
		// Tolerate Redis cache failure.
		if err := sr.redisCache.Delete(ctx, names[i]); err != nil {
			log.Error("Registry Add (redis setstruct) failed, details: %v", err)
		}
	}

	for _, n := range names {
		// Drop Blob table.
		dropBlobTable := fmt.Sprintf("DROP TABLE %s.%s;", connection.Config.Keyspace, btree.FormatBlobTable(n))
		if err := connection.Session.Query(dropBlobTable).WithContext(ctx).Exec(); err != nil {
			return err
		}
		// Drop Virtual ID registry table.
		dropRegistryTable := fmt.Sprintf("DROP TABLE %s.%s;", connection.Config.Keyspace, btree.FormatRegistryTable(n))
		if err := connection.Session.Query(dropRegistryTable).WithContext(ctx).Exec(); err != nil {
			return err
		}
	}

	return nil
}
