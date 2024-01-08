// Package Cassandra contains code for integration or inter-operation with Cassandra DB.
package cassandra

import (
	"context"
	"fmt"
	log "log/slog"
	"strings"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_red_ck/redis"
	"github.com/gocql/gocql"
)

// Manage or fetch Virtual Id request/response payload.
type RegistryPayload[T sop.Handle | btree.UUID] struct {
	// Registry table (name) where the Virtual Ids will be stored or fetched from.
	RegistryTable string
	// IDs is an array containing the Virtual Ids details to be stored or to be fetched.
	IDs []T
}

// Virtual Id registry is essential in our support for all or nothing (sub)feature,
// which is essential in "fault tolerant" & "self healing" feature.
//
// All methods are taking in a set of items and need to be implemented to do
// all or nothing feature, e.g. wrapped in transaction in Cassandra.
type Registry interface {
	// Get will fetch handles(given their Ids) from stores(given a store name).
	// Supports an array of store names with a set of handle Ids each.
	Get(context.Context, ...RegistryPayload[btree.UUID]) ([]RegistryPayload[sop.Handle], error)
	// Add will insert handles to stores(given a store name).
	// Supports an array of store names with a set of handles each.
	Add(context.Context, ...RegistryPayload[sop.Handle]) error
	// Update will update handles of stores(given a store name).
	// Supports an array of store names with a set of handle each.
	Update(context.Context, ...RegistryPayload[sop.Handle]) error
	// Remove will delete handles(given their Ids) from stores(given a store name).
	// Supports an array of store names with a set of handle each.
	Remove(context.Context, ...RegistryPayload[btree.UUID]) error
}

type registry struct {
	redisCache redis.Cache
}

// NewRegistry manages the Handle in the store's Cassandra registry table.
func NewRegistry() Registry {
	return &registry{
		redisCache: redis.NewClient(),
	}
}

// TODO: finalize Consistency levels to use in below CRUD methods.

func (v *registry) Add(ctx context.Context, storesHandles ...RegistryPayload[sop.Handle]) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
	}
	for _, sh := range storesHandles {
		insertStatement := fmt.Sprintf("INSERT INTO %s.%s (lid, is_idb, p_ida, p_idb, ts, wip_ts, is_del) VALUES(?,?,?,?,?,?,?);",
			connection.Config.Keyspace, sh.RegistryTable)
		for _, h := range sh.IDs {
			// Add a new store record.
			if err := connection.Session.Query(insertStatement, gocql.UUID(h.LogicalId), h.IsActiveIdB, gocql.UUID(h.PhysicalIdA),
				gocql.UUID(h.PhysicalIdB), h.Timestamp, h.WorkInProgressTimestamp, h.IsDeleted).WithContext(ctx).Exec(); err != nil {
				return err
			}
			// Tolerate Redis cache failure.
			if err := v.redisCache.SetStruct(ctx, v.formatKey(h.LogicalId.ToString()), &h, -1); err != nil {
				log.Error("Registry Add (redis setstruct) failed, details: %v", err)
			}
		}
	}
	return nil
}

// Update does an all or nothing update of the batch of handles, mapping them to respective registry table(s).
func (v *registry) Update(ctx context.Context, storesHandles ...RegistryPayload[sop.Handle]) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
	}
	if len(storesHandles) == 0 {
		return nil
	}

	// TODO: in order to ensure 0 race condition, we need to use Redis to ensure exclusive update on the set of Handles.
	// Logic: use a set of redis keys to enforce locks so only "winners"(using UUID) will be able to proceed, rest will return error
	// to cause rollback. See item_action_tracker.go "lockRecord" struct based locking.
	//
	// For now, keep it simple and rely on transaction commit's optimistic locking & multi-phase checks,
	// together with the logged batch update as shown below.

	// Logged batch will do all or nothing. This is the only one "all or nothing" operation in the Commit process.
	batch := connection.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	for _, sh := range storesHandles {
		updateStatement := fmt.Sprintf("UPDATE %s.%s SET is_idb = ?, p_ida = ?, p_idb = ?, ts = ?, wip_ts = ?, is_del = ? WHERE lid = ?;",
			connection.Config.Keyspace, sh.RegistryTable)
		for _, h := range sh.IDs {
			// Update store record.
			batch.Query(updateStatement, h.IsActiveIdB, gocql.UUID(h.PhysicalIdA), gocql.UUID(h.PhysicalIdB),
				h.Timestamp, h.WorkInProgressTimestamp, h.IsDeleted, gocql.UUID(h.LogicalId))
		}
	}
	// Failed update all, thus, return err to cause rollback.
	if err := connection.Session.ExecuteBatch(batch); err != nil {
		// Ensure to flush the failing batch from redis cache.
		for _, sh := range storesHandles {
			for _, h := range sh.IDs {
				// Tolerate Redis cache failure.
				if err := v.redisCache.Delete(ctx, v.formatKey(h.LogicalId.ToString())); err != nil {
					if !redis.KeyNotFound(err) {
						log.Error("Registry Update (redis setstruct) failed, details: %v", err)
					}
				}
			}
		}
		return err
	}

	// Update redis cache.
	for _, sh := range storesHandles {
		for _, h := range sh.IDs {
			// Tolerate Redis cache failure.
			if err := v.redisCache.SetStruct(ctx, v.formatKey(h.LogicalId.ToString()), &h, -1); err != nil {
				log.Error("Registry Update (redis setstruct) failed, details: %v", err)
			}
		}
	}
	return nil
}

func (v *registry) Get(ctx context.Context, storesLids ...RegistryPayload[btree.UUID]) ([]RegistryPayload[sop.Handle], error) {
	if connection == nil {
		return nil, fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
	}

	storesHandles := make([]RegistryPayload[sop.Handle], 0, len(storesLids))
	for _, storeLids := range storesLids {
		handles := make([]sop.Handle, 0, len(storeLids.IDs))
		paramQ := make([]string, 0, len(storeLids.IDs))
		lidsAsIntfs := make([]interface{}, 0, len(storeLids.IDs))
		for i := range storeLids.IDs {
			h := sop.Handle{}
			if err := v.redisCache.GetStruct(ctx, v.formatKey(storeLids.IDs[i].ToString()), &h); err != nil {
				if !redis.KeyNotFound(err) {
					log.Error("Registry Get (redis getstruct) failed, details: %v", err)
				}
				paramQ = append(paramQ, "?")
				lidsAsIntfs = append(lidsAsIntfs, interface{}(gocql.UUID(storeLids.IDs[i])))
				continue
			}
			handles = append(handles, h)
		}

		if len(paramQ) == 0 {
			storesHandles = append(storesHandles, RegistryPayload[sop.Handle]{
				RegistryTable: storeLids.RegistryTable,
				IDs:           handles,
			})
			continue
		}
		selectStatement := fmt.Sprintf("SELECT lid, is_idb, p_ida, p_idb, ts, wip_ts, is_del FROM %s.%s WHERE lid in (%v);",
			connection.Config.Keyspace, storeLids.RegistryTable, strings.Join(paramQ, ", "))
		iter := connection.Session.Query(selectStatement, lidsAsIntfs...).WithContext(ctx).Iter()
		handle := sop.Handle{}
		var lid, ida, idb gocql.UUID
		for iter.Scan(&lid, &handle.IsActiveIdB, &ida, &idb, &handle.Timestamp, &handle.WorkInProgressTimestamp, &handle.IsDeleted) {
			handle.LogicalId = btree.UUID(lid)
			handle.PhysicalIdA = btree.UUID(ida)
			handle.PhysicalIdB = btree.UUID(idb)
			handles = append(handles, handle)

			if err := v.redisCache.SetStruct(ctx, v.formatKey(handle.LogicalId.ToString()), &handle, -1); err != nil {
				log.Error("Registry Get (redis setstruct) failed, details: %v", err)
			}
			handle = sop.Handle{}
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
		storesHandles = append(storesHandles, RegistryPayload[sop.Handle]{
			RegistryTable: storeLids.RegistryTable,
			IDs:           handles,
		})
	}
	return storesHandles, nil
}

func (v *registry) Remove(ctx context.Context, storesLids ...RegistryPayload[btree.UUID]) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
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
		deleteFromCache := func () {
			for _, id := range storeLids.IDs {
				if err := v.redisCache.Delete(ctx, v.formatKey(id.ToString())); err != nil && !redis.KeyNotFound(err) {
					log.Error("Registry Delete (redis delete) failed, details: %v", err)
				}
			}
		}
		if err := connection.Session.Query(deleteStatement, lidsAsIntfs...).WithContext(ctx).Exec(); err != nil {
			deleteFromCache()
			return err
		}
		deleteFromCache()
	}
	return nil
}

// Virtual ID key in Redis is prefixed by V to differentiate from Node key.
func (v *registry) formatKey(k string) string {
	return k
}
