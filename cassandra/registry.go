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

type registry struct {
	cache sop.Cache
}

// Lock time out for the cache based conflict check routine in update (handles) function.
const updateAllOrNothingOfHandleSetLockTimeout = time.Duration(10 * time.Minute)

// NewRegistry manages the Handle in the store's Cassandra registry table.
func NewRegistry() sop.Registry {
	return &registry{
		cache: redis.NewClient(),
	}
}

func (v *registry) Add(ctx context.Context, storesHandles []sop.RegistryPayload[sop.Handle]) error {
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
			if err := v.cache.SetStruct(ctx, h.LogicalID.String(), &h, sh.CacheDuration); err != nil {
				log.Warn(fmt.Sprintf("Registry Add (redis setstruct) failed, details: %v", err))
			}
		}
	}
	return nil
}

// Update does an all or nothing update of the batch of handles, mapping them to respective registry table(s).
func (v *registry) Update(ctx context.Context, storesHandles []sop.RegistryPayload[sop.Handle]) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call OpenConnection(config) to open it")
	}
	if len(storesHandles) == 0 {
		return nil
	}

	for _, sh := range storesHandles {
		updateStatement := fmt.Sprintf("UPDATE %s.%s SET is_idb = ?, p_ida = ?, p_idb = ?, ver = ?, wip_ts = ?, is_del = ? WHERE lid = ?;",
			connection.Config.Keyspace, sh.RegistryTable)
		// Fail on 1st encountered error. It is non-critical operation, SOP can "heal" those got left.
		for _, h := range sh.IDs {
			// Update registry record.
			lk := v.cache.CreateLockKeys([]string{h.LogicalID.String()})
			if ok, err := v.cache.Lock(ctx, updateAllOrNothingOfHandleSetLockTimeout, lk); !ok || err != nil {
				if err == nil {
					err = fmt.Errorf("lock failed, key %v is already locked by another", lk[0].Key)
				}
				return err
			}

			qry := connection.Session.Query(updateStatement, h.IsActiveIDB, gocql.UUID(h.PhysicalIDA), gocql.UUID(h.PhysicalIDB),
				h.Version, h.WorkInProgressTimestamp, h.IsDeleted, gocql.UUID(h.LogicalID)).WithContext(ctx)
			if connection.Config.ConsistencyBook.RegistryUpdate > gocql.Any {
				qry.Consistency(connection.Config.ConsistencyBook.RegistryUpdate)
			}

			// Update registry record.
			if err := qry.Exec(); err != nil {
				// Unlock the object Keys before return.
				v.cache.Unlock(ctx, lk)
				return err
			}

			// Tolerate Redis cache failure.
			if err := v.cache.SetStruct(ctx, h.LogicalID.String(), &h, sh.CacheDuration); err != nil {
				log.Warn(fmt.Sprintf("Registry Update (redis setstruct) failed, details: %v", err))
			}

			// Unlock the object Keys.
			if err := v.cache.Unlock(ctx, lk); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *registry) UpdateNoLocks(ctx context.Context, storesHandles []sop.RegistryPayload[sop.Handle]) error {
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

			// Tolerate Redis cache failure.
			if err := v.cache.SetStruct(ctx, h.LogicalID.String(), &h, sh.CacheDuration); err != nil {
				log.Warn(fmt.Sprintf("Registry Update (redis setstruct) failed, details: %v", err))
			}
		}
	}
	return nil
}

func (v *registry) Get(ctx context.Context, storesLids []sop.RegistryPayload[sop.UUID]) ([]sop.RegistryPayload[sop.Handle], error) {
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
			var err error
			if storeLids.IsCacheTTL {
				err = v.cache.GetStructEx(ctx, storeLids.IDs[i].String(), &h, storeLids.CacheDuration)
			} else {
				err = v.cache.GetStruct(ctx, storeLids.IDs[i].String(), &h)
			}
			if err != nil {
				if !v.cache.KeyNotFound(err) {
					log.Warn(fmt.Sprintf("Registry Get (redis getstruct) failed, details: %v", err))
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
				CacheDuration: storeLids.CacheDuration,
				IsCacheTTL:    storeLids.IsCacheTTL,
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

			if err := v.cache.SetStruct(ctx, handle.LogicalID.String(), &handle, storeLids.CacheDuration); err != nil {
				log.Warn(fmt.Sprintf("Registry Set (redis setstruct) failed, details: %v", err))
			}
			handle = sop.Handle{}
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
		storesHandles = append(storesHandles, sop.RegistryPayload[sop.Handle]{
			RegistryTable: storeLids.RegistryTable,
			BlobTable:     storeLids.BlobTable,
			CacheDuration: storeLids.CacheDuration,
			IsCacheTTL:    storeLids.IsCacheTTL,
			IDs:           handles,
		})
	}
	return storesHandles, nil
}

func (v *registry) Remove(ctx context.Context, storesLids []sop.RegistryPayload[sop.UUID]) error {
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
				if err := v.cache.Delete(ctx, []string{id.String()}); err != nil && !v.cache.KeyNotFound(err) {
					log.Warn(fmt.Sprintf("Registry Delete (redis delete) failed, details: %v", err))
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

// Cassandra already provides replication for the registry tables, no need to do anything here.
func (v *registry) Replicate(ctx context.Context, newRootNodeHandles, addedNodeHandles, updatedNodeHandles, removedNodeHandles []sop.RegistryPayload[sop.Handle]) {}
