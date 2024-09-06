// Package Cassandra contains code for integration or inter-operation with SOP's Cassandra DB.
// This package manage contents on tables like Registry, StoreRepository, Transaction Log.
package cassandra

import (
	"context"
	"fmt"
	log "log/slog"
	"strings"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/redis"
	"github.com/gocql/gocql"
)

// UpdateAllOrNothingError is a special error type that will allow caller to handle it differently than normal errors.
type UpdateAllOrNothingError struct {
	Err error
}

func (r *UpdateAllOrNothingError) Error() string {
	return r.Err.Error()
}

type registry struct {
	redisCache redis.Cache
}

var registryCacheDuration time.Duration = time.Duration(12 * time.Hour)

// SetRegistryDuration allows registry cache duration to get set globally.
func SetRegistryCacheDuration(duration time.Duration) {
	if duration < time.Minute {
		duration = time.Duration(1 * time.Hour)
	}
	registryCacheDuration = duration
}

// NewRegistry manages the Handle in the store's Cassandra registry table.
func NewRegistry() sop.Registry {
	return &registry{
		redisCache: redis.NewClient(),
	}
}

func (v *registry) Add(ctx context.Context, storesHandles ...sop.RegistryPayload[sop.Handle]) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}
	for _, sh := range storesHandles {
		insertStatement := fmt.Sprintf("INSERT INTO %s.%s (lid, is_idb, p_ida, p_idb, ver, wip_ts, is_del) VALUES(?,?,?,?,?,?,?);",
			connection.Config.Keyspace, sh.RegistryTable)
		for _, h := range sh.IDs {

			qry := connection.Session.Query(insertStatement, gocql.UUID(h.LogicalID), h.IsActiveIDB, gocql.UUID(h.PhysicalIDA),
				gocql.UUID(h.PhysicalIDB), h.Version, h.WorkInProgressTimestamp, h.IsDeleted).WithContext(ctx)
			if connection.Config.ConsistencyBook.RegistryAdd > gocql.Any {
				qry.Consistency(connection.Config.ConsistencyBook.RegistryAdd)
			}

			// Add a new store record.
			if err := qry.Exec(); err != nil {
				return err
			}
			// Tolerate Redis cache failure.
			if err := v.redisCache.SetStruct(ctx, h.LogicalID.String(), &h, registryCacheDuration); err != nil {
				log.Error(fmt.Sprintf("Registry Add (redis setstruct) failed, details: %v", err))
			}
		}
	}
	return nil
}

// Update does an all or nothing update of the batch of handles, mapping them to respective registry table(s).
func (v *registry) Update(ctx context.Context, allOrNothing bool, storesHandles ...sop.RegistryPayload[sop.Handle]) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}
	if len(storesHandles) == 0 {
		return nil
	}

	// Logged batch will do all or nothing. This is the only one "all or nothing" operation in the Commit process.
	if allOrNothing {
		// For now, keep it simple and rely on transaction commit's optimistic locking & multi-phase checks,
		// together with the logged batch update as shown below.

		// Enforce a Redis based version check as Cassandra logged transaction does not allow "conditional" check across partitions.
		for _, sh := range storesHandles {
			for _, h := range sh.IDs {
				var h2 sop.Handle
				if err := v.redisCache.GetStruct(ctx, h.LogicalID.String(), &h2); err != nil {
					return err
				}
				newVersion := h.Version
				// Version ID is incremental, 'thus we can compare with -1 the previous.
				newVersion--
				if newVersion != h2.Version || !h.IsEqual(&h2) {
					return &UpdateAllOrNothingError{
						Err: fmt.Errorf("Registry Update failed, handle logical ID(%v) version conflict detected", h.LogicalID.ToString()),
					}
				}
			}
		}

		// Do the actual batch logged transaction update in Cassandra.
		batch := connection.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
		if connection.Config.ConsistencyBook.RegistryUpdate > gocql.Any {
			batch.SetConsistency(connection.Config.ConsistencyBook.RegistryUpdate)
		}

		for _, sh := range storesHandles {
			updateStatement := fmt.Sprintf("UPDATE %s.%s SET is_idb = ?, p_ida = ?, p_idb = ?, ver = ?, wip_ts = ?, is_del = ? WHERE lid = ?;",
				connection.Config.Keyspace, sh.RegistryTable)
			for _, h := range sh.IDs {
				// Update registry record.
				batch.Query(updateStatement, h.IsActiveIDB, gocql.UUID(h.PhysicalIDA), gocql.UUID(h.PhysicalIDB),
					h.Version, h.WorkInProgressTimestamp, h.IsDeleted, gocql.UUID(h.LogicalID))
			}
		}
		// Failed update all, thus, return err to cause rollback.
		if err := connection.Session.ExecuteBatch(batch); err != nil {
			return err
		}
	} else {
		for _, sh := range storesHandles {
			updateStatement := fmt.Sprintf("UPDATE %s.%s SET is_idb = ?, p_ida = ?, p_idb = ?, ver = ?, wip_ts = ?, is_del = ? WHERE lid = ?;",
				connection.Config.Keyspace, sh.RegistryTable)
			// Fail on 1st encountered error. It is non-critical operation, SOP can "heal" those got left.
			for _, h := range sh.IDs {

				qry := connection.Session.Query(updateStatement, h.IsActiveIDB, gocql.UUID(h.PhysicalIDA), gocql.UUID(h.PhysicalIDB),
					h.Version, h.WorkInProgressTimestamp, h.IsDeleted, gocql.UUID(h.LogicalID)).WithContext(ctx)
				if connection.Config.ConsistencyBook.RegistryUpdate > gocql.Any {
					qry.Consistency(connection.Config.ConsistencyBook.RegistryUpdate)
				}

				// Update registry record.
				if err := qry.Exec(); err != nil {
					return err
				}
			}
		}
	}

	// Update redis cache.
	for _, sh := range storesHandles {
		for _, h := range sh.IDs {
			// Tolerate Redis cache failure.
			if err := v.redisCache.SetStruct(ctx, h.LogicalID.String(), &h, registryCacheDuration); err != nil {
				log.Error(fmt.Sprintf("Registry Update (redis setstruct) failed, details: %v", err))
			}
		}
	}
	return nil
}

func (v *registry) Get(ctx context.Context, storesLids ...sop.RegistryPayload[sop.UUID]) ([]sop.RegistryPayload[sop.Handle], error) {
	if connection == nil {
		return nil, fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	storesHandles := make([]sop.RegistryPayload[sop.Handle], 0, len(storesLids))
	for _, storeLids := range storesLids {
		handles := make([]sop.Handle, 0, len(storeLids.IDs))
		paramQ := make([]string, 0, len(storeLids.IDs))
		lidsAsIntfs := make([]interface{}, 0, len(storeLids.IDs))
		for i := range storeLids.IDs {
			h := sop.Handle{}
			if err := v.redisCache.GetStruct(ctx, storeLids.IDs[i].String(), &h); err != nil {
				if !redis.KeyNotFound(err) {
					log.Error(fmt.Sprintf("Registry Get (redis getstruct) failed, details: %v", err))
				}
				paramQ = append(paramQ, "?")
				lidsAsIntfs = append(lidsAsIntfs, interface{}(gocql.UUID(storeLids.IDs[i])))
				continue
			}
			handles = append(handles, h)
		}

		if len(paramQ) == 0 {
			storesHandles = append(storesHandles, sop.RegistryPayload[sop.Handle]{
				RegistryTable: storeLids.RegistryTable,
				BlobTable:     storeLids.BlobTable,
				IDs:           handles,
			})
			continue
		}
		selectStatement := fmt.Sprintf("SELECT lid, is_idb, p_ida, p_idb, ver, wip_ts, is_del FROM %s.%s WHERE lid in (%v);",
			connection.Config.Keyspace, storeLids.RegistryTable, strings.Join(paramQ, ", "))

		qry := connection.Session.Query(selectStatement, lidsAsIntfs...).WithContext(ctx)
		if connection.Config.ConsistencyBook.RegistryGet > gocql.Any {
			qry.Consistency(connection.Config.ConsistencyBook.RegistryGet)
		}

		iter := qry.Iter()
		handle := sop.Handle{}
		var lid, ida, idb gocql.UUID
		for iter.Scan(&lid, &handle.IsActiveIDB, &ida, &idb, &handle.Version, &handle.WorkInProgressTimestamp, &handle.IsDeleted) {
			handle.LogicalID = sop.UUID(lid)
			handle.PhysicalIDA = sop.UUID(ida)
			handle.PhysicalIDB = sop.UUID(idb)
			handles = append(handles, handle)

			if err := v.redisCache.SetStruct(ctx, handle.LogicalID.String(), &handle, registryCacheDuration); err != nil {
				log.Error(fmt.Sprintf("Registry Get (redis setstruct) failed, details: %v", err))
			}
			handle = sop.Handle{}
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
		storesHandles = append(storesHandles, sop.RegistryPayload[sop.Handle]{
			RegistryTable: storeLids.RegistryTable,
			BlobTable:     storeLids.BlobTable,
			IDs:           handles,
		})
	}
	return storesHandles, nil
}

func (v *registry) Remove(ctx context.Context, storesLids ...sop.RegistryPayload[sop.UUID]) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}

	for _, storeLids := range storesLids {
		paramQ := make([]string, len(storeLids.IDs))
		lidsAsIntfs := make([]interface{}, len(storeLids.IDs))
		for i := range storeLids.IDs {
			paramQ[i] = "?"
			lidsAsIntfs[i] = interface{}(gocql.UUID(storeLids.IDs[i]))
		}
		deleteStatement := fmt.Sprintf("DELETE FROM %s.%s WHERE lid in (%v);",
			connection.Config.Keyspace, storeLids.RegistryTable, strings.Join(paramQ, ", "))

		// Flush out the failing records from cache.
		deleteFromCache := func(storeLids sop.RegistryPayload[sop.UUID]) {
			for _, id := range storeLids.IDs {
				if err := v.redisCache.Delete(ctx, id.String()); err != nil && !redis.KeyNotFound(err) {
					log.Error(fmt.Sprintf("Registry Delete (redis delete) failed, details: %v", err))
				}
			}
		}

		qry := connection.Session.Query(deleteStatement, lidsAsIntfs...).WithContext(ctx)
		if connection.Config.ConsistencyBook.RegistryRemove > gocql.Any {
			qry.Consistency(connection.Config.ConsistencyBook.RegistryRemove)
		}

		if err := qry.Exec(); err != nil {
			deleteFromCache(storeLids)
			return err
		}
		deleteFromCache(storeLids)
	}
	return nil
}
