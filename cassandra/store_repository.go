package cassandra

import (
	"context"
	"fmt"
	log "log/slog"
	"strings"
	"time"

	"github.com/gocql/gocql"
	retry "github.com/sethvargo/go-retry"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_memory"
	"github.com/SharedCode/sop/redis"
)

type storeRepository struct {
	redisCache      redis.Cache
	manageBlobStore sop.ManageBlobStore
}

func NewStoreRepository() sop.StoreRepository {
	return NewStoreRepositoryExt(nil)
}

// NewStoreRepository manages the StoreInfo in Cassandra table.
// Passing in nil to "managedBlobStore" will use default implementation in StoreRepository itself
// for managing Blob Store table in Cassandra.
func NewStoreRepositoryExt(manageBlobStore sop.ManageBlobStore) sop.StoreRepository {
	r := &storeRepository{
		redisCache:      redis.NewClient(),
		manageBlobStore: manageBlobStore,
	}
	// Default to an implementation of this Store Repository for managing the blob table in Cassandra.
	if manageBlobStore == nil {
		r.manageBlobStore = r
	}
	return r
}

// Add a new store record, create a new Virtual ID registry and node blob tables.
func (sr *storeRepository) Add(ctx context.Context, stores ...sop.StoreInfo) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}
	insertStatement := fmt.Sprintf("INSERT INTO %s.store (name, root_id, slot_count, count, unique, des, reg_tbl, blob_tbl, ts, vdins, vdap, vdgc, llb, rcd, rc_ttl, ncd, nc_ttl, vdcd, vdc_ttl, scd, sc_ttl) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);", connection.Config.Keyspace)
	for _, s := range stores {

		// Add a new store record.
		qry := connection.Session.Query(insertStatement, s.Name, gocql.UUID(s.RootNodeID), s.SlotLength, s.Count, s.IsUnique, s.Description,
			s.RegistryTable, s.BlobTable, s.Timestamp, s.IsValueDataInNodeSegment, s.IsValueDataActivelyPersisted, s.IsValueDataGloballyCached,
			s.LeafLoadBalancing, s.CacheConfig.RegistryCacheDuration, s.CacheConfig.IsRegistryCacheTTL, s.CacheConfig.NodeCacheDuration, s.CacheConfig.IsNodeCacheTTL,
			s.CacheConfig.ValueDataCacheDuration, s.CacheConfig.IsValueDataCacheTTL, s.CacheConfig.StoreInfoCacheDuration, s.CacheConfig.IsStoreInfoCacheTTL).WithContext(ctx)
		if connection.Config.ConsistencyBook.StoreAdd > gocql.Any {
			qry.Consistency(connection.Config.ConsistencyBook.StoreAdd)
		}
		if err := qry.Exec(); err != nil {
			return err
		}

		// Create a new Blob table.
		if err := sr.manageBlobStore.CreateBlobStore(ctx, s.BlobTable); err != nil {
			return err
		}

		// Create a new Virtual ID registry table.
		createNewRegistry := fmt.Sprintf("CREATE TABLE %s.%s(lid UUID PRIMARY KEY, is_idb boolean, p_ida UUID, p_idb UUID, ver int, wip_ts bigint, is_del boolean);",
			connection.Config.Keyspace, s.RegistryTable)
		qry = connection.Session.Query(createNewRegistry).WithContext(ctx)
		if connection.Config.ConsistencyBook.StoreAdd > gocql.Any {
			qry.Consistency(connection.Config.ConsistencyBook.StoreAdd)
		}
		if err := qry.Exec(); err != nil {
			return err
		}
		// Tolerate error in Redis caching.
		if err := sr.redisCache.SetStruct(ctx, s.Name, &s, s.CacheConfig.StoreInfoCacheDuration); err != nil {
			log.Warn(fmt.Sprintf("StoreRepository Add failed (redis setstruct), details: %v", err))
		}
	}
	return nil
}

// Update enforces so only the Store's Count & timestamp can get updated.
func (sr *storeRepository) Update(ctx context.Context, stores ...sop.StoreInfo) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	// Sort the stores info so we can commit them in same sort order across transactions,
	// thus, reduced chance of deadlock.
	b3 := in_memory.NewBtree[string, sop.StoreInfo](true)
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

	// Create lock IDs that we can use to logically lock and prevent other updates.
	lockKeys := redis.CreateLockKeys(keys...)

	// 15 minutes to lock, merge/update details then unlock.
	duration := time.Duration(15 * time.Minute)
	b := retry.NewFibonacci(1 * time.Second)

	// Lock all keys.
	if err := retry.Do(ctx, retry.WithMaxRetries(5, b), func(ctx context.Context) error {
		if err := redis.Lock(ctx, duration, lockKeys...); err != nil {
			log.Warn(err.Error() + ", will retry")
			return retry.RetryableError(err)
		}
		return nil
	}); err != nil {
		// Unlock all keys since we failed locking them.
		redis.Unlock(ctx, lockKeys...)
		return err
	}

	updateStatement := fmt.Sprintf("UPDATE %s.store SET count = ?, ts = ? WHERE name = ?;", connection.Config.Keyspace)
	undo := func(bus []sop.StoreInfo) {
		// Attempt to undo changes, 'ignores error as it is a last attempt to cleanup.
		for ii := 0; ii < len(bus); ii++ {
			log.Debug(fmt.Sprintf("undo occured for store %s", bus[ii].Name))
			qry := connection.Session.Query(updateStatement, bus[ii].Count, bus[ii].Timestamp,
				bus[ii].Name)
			if connection.Config.ConsistencyBook.StoreUpdate > gocql.Any {
				qry.Consistency(connection.Config.ConsistencyBook.StoreUpdate)
			}
			qry.Exec()
		}
	}

	beforeUpdateStores := make([]sop.StoreInfo, 0, len(stores))
	// Unlock all keys before going out of scope.
	defer redis.Unlock(ctx, lockKeys...)

	for i := range stores {
		sis, err := sr.Get(ctx, stores[i].CacheConfig.IsStoreInfoCacheTTL, stores[i].CacheConfig.StoreInfoCacheDuration, stores[i].Name)
		if len(sis) == 0 {
			undo(beforeUpdateStores)
			return err
		}
		beforeUpdateStores = append(beforeUpdateStores, sis...)

		si := sis[0]
		// Merge or apply the "count delta".
		stores[i].Count = si.Count + stores[i].CountDelta
		stores[i].Timestamp = si.Timestamp

		qry := connection.Session.Query(updateStatement, stores[i].Count, stores[i].Timestamp, stores[i].Name)
		if connection.Config.ConsistencyBook.StoreUpdate > gocql.Any {
			qry.Consistency(connection.Config.ConsistencyBook.StoreUpdate)
		}

		// Update store record.
		if err := qry.Exec(); err != nil {
			// Undo changes.
			undo(beforeUpdateStores)
			return err
		}
	}
	// Update redis since we've successfully updated Cassandra Store table.
	for i := range stores {
		// Tolerate redis error since we've successfully updated the master table.
		if err := sr.redisCache.SetStruct(ctx, stores[i].Name, &stores[i], stores[i].CacheConfig.StoreInfoCacheDuration); err != nil {
			log.Warn(fmt.Sprintf("StoreRepository Update (redis setstruct) failed, details: %v", err))
		}
	}

	return nil
}

func (sr *storeRepository) Get(ctx context.Context, isCacheTTL bool, cacheDuration time.Duration, names ...string) ([]sop.StoreInfo, error) {
	if connection == nil {
		return nil, fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}
	stores := make([]sop.StoreInfo, 0, len(names))
	// Format some variadic ? and convert to interface the names param.
	namesAsIntf := make([]interface{}, 0, len(names))
	paramQ := make([]string, 0, len(names))
	for i := range names {
		store := sop.StoreInfo{}
		var err error
		if isCacheTTL {
			err = sr.redisCache.GetStructEx(ctx, names[i], &store, cacheDuration)
		} else {
			err = sr.redisCache.GetStruct(ctx, names[i], &store)
		}
		if err != nil {
			if !redis.KeyNotFound(err) {
				log.Warn(fmt.Sprintf("StoreRepository Get (redis getstruct) failed, details: %v", err))
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
	selectStatement := fmt.Sprintf("SELECT name, root_id, slot_count, count, unique, des, reg_tbl, blob_tbl, ts, vdins, vdap, vdgc, llb, rcd, rc_ttl, ncd, nc_ttl, vdcd, vdc_ttl, scd, sc_ttl FROM %s.store  WHERE name in (%v);",
		connection.Config.Keyspace, strings.Join(paramQ, ", "))

	qry := connection.Session.Query(selectStatement, namesAsIntf...).WithContext(ctx)
	if connection.Config.ConsistencyBook.StoreGet > gocql.Any {
		qry.Consistency(connection.Config.ConsistencyBook.StoreGet)
	}

	iter := qry.Iter()
	store := sop.StoreInfo{}
	var rid gocql.UUID
	for iter.Scan(&store.Name, &rid, &store.SlotLength, &store.Count, &store.IsUnique,
		&store.Description, &store.RegistryTable, &store.BlobTable, &store.Timestamp, &store.IsValueDataInNodeSegment, &store.IsValueDataActivelyPersisted, &store.IsValueDataGloballyCached,
		&store.LeafLoadBalancing, &store.CacheConfig.RegistryCacheDuration, &store.CacheConfig.IsRegistryCacheTTL, &store.CacheConfig.NodeCacheDuration, &store.CacheConfig.IsNodeCacheTTL,
		&store.CacheConfig.ValueDataCacheDuration, &store.CacheConfig.IsValueDataCacheTTL, &store.CacheConfig.StoreInfoCacheDuration, &store.CacheConfig.IsStoreInfoCacheTTL) {
		store.RootNodeID = sop.UUID(rid)

		if err := sr.redisCache.SetStruct(ctx, store.Name, &store, store.CacheConfig.StoreInfoCacheDuration); err != nil {
			log.Warn(fmt.Sprintf("StoreRepository Get (redis setstruct) failed, details: %v", err))
		}

		stores = append(stores, store)
		store = sop.StoreInfo{}
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	return stores, nil
}

func (sr *storeRepository) Remove(ctx context.Context, names ...string) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	sis, err := sr.Get(ctx, false, 0, names...)
	if err != nil {
		return err
	}

	// Format some variadic ? and convert to interface the names param.
	namesAsIntf := make([]interface{}, len(names))
	paramQ := make([]string, len(names))
	for i := range names {
		paramQ[i] = "?"
		namesAsIntf[i] = interface{}(names[i])
	}
	deleteStatement := fmt.Sprintf("DELETE FROM %s.store WHERE name in (%v);", connection.Config.Keyspace, strings.Join(paramQ, ", "))
	qry := connection.Session.Query(deleteStatement, namesAsIntf...).WithContext(ctx)
	if connection.Config.ConsistencyBook.StoreRemove > gocql.Any {
		qry.Consistency(connection.Config.ConsistencyBook.StoreRemove)
	}
	if err := qry.Exec(); err != nil {
		return err
	}

	// Delete the store records in Redis.
	for i := range names {
		// Tolerate Redis cache failure.
		if err := sr.redisCache.Delete(ctx, names[i]); err != nil && !redis.KeyNotFound(err) {
			log.Warn(fmt.Sprintf("Registry Add (redis setstruct) failed, details: %v", err))
		}
	}

	for i, n := range names {
		// Drop Blob table.
		if i < len(sis) {
			if err := sr.manageBlobStore.RemoveBlobStore(ctx, sis[i].BlobTable); err != nil {
				return err
			}
		}
		// Drop Virtual ID registry table.
		dropRegistryTable := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s;", connection.Config.Keyspace, sop.FormatRegistryTable(n))
		qry = connection.Session.Query(dropRegistryTable).WithContext(ctx)
		if connection.Config.ConsistencyBook.StoreRemove > gocql.Any {
			qry.Consistency(connection.Config.ConsistencyBook.StoreRemove)
		}
		if err := qry.Exec(); err != nil {
			return err
		}
	}

	return nil
}

func (sr *storeRepository) CreateBlobStore(ctx context.Context, blobStoreName string) error {
	// Create a new Blob table.
	createNewBlobTable := fmt.Sprintf("CREATE TABLE %s.%s(id UUID PRIMARY KEY, node blob);", connection.Config.Keyspace, blobStoreName)
	qry := connection.Session.Query(createNewBlobTable).WithContext(ctx)
	if connection.Config.ConsistencyBook.StoreAdd > gocql.Any {
		qry.Consistency(connection.Config.ConsistencyBook.StoreAdd)
	}
	if err := qry.Exec(); err != nil {
		return err
	}
	return nil
}

func (sr *storeRepository) RemoveBlobStore(ctx context.Context, blobStoreName string) error {
	// Drop Blob table.
	dropBlobTable := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s;", connection.Config.Keyspace, blobStoreName)

	qry := connection.Session.Query(dropBlobTable).WithContext(ctx)
	if connection.Config.ConsistencyBook.StoreRemove > gocql.Any {
		qry.Consistency(connection.Config.ConsistencyBook.StoreRemove)
	}
	if err := qry.Exec(); err != nil {
		return err
	}
	return nil
}
