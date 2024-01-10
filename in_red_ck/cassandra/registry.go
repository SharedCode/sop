// Package Cassandra contains code for integration or inter-operation with Cassandra DB.
package cassandra

import (
	"context"
	"fmt"
	log "log/slog"
	"strings"
	"time"

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

func GetRegistryPayloadCount[T btree.UUID](payloads []RegistryPayload[T]) int {
	total := 0
	for _, p := range payloads {
		total = total + len(p.IDs)
	}
	return total
}

// Virtual Id registry is essential in our support for all or nothing (sub)feature,
// which is essential in "fault tolerant" & "self healing" feature.
//
// All methods are taking in a set of items.
type Registry interface {
	// Get will fetch handles(given their Ids) from stores.
	Get(context.Context, ...RegistryPayload[btree.UUID]) ([]RegistryPayload[sop.Handle], error)
	// Add will insert handles to stores.
	Add(context.Context, ...RegistryPayload[sop.Handle]) error
	// Update will update handles of stores.
	// Set allOrNothing to true if Update operation is crucial for data consistency and
	// wanting to do an all or nothing update for the entire batch of handles.
	// False is recommended if such consistency is not significant.
	Update(ctx context.Context, allOrNothing bool, handles ...RegistryPayload[sop.Handle]) error
	// Remove will delete handles(given their Ids) from stores.
	Remove(context.Context, ...RegistryPayload[btree.UUID]) error
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
			if err := v.redisCache.SetStruct(ctx, h.LogicalId.ToString(), &h, registryCacheDuration); err != nil {
				log.Error("Registry Add (redis setstruct) failed, details: %v", err)
			}
		}
	}
	return nil
}

// Update does an all or nothing update of the batch of handles, mapping them to respective registry table(s).
func (v *registry) Update(ctx context.Context, allOrNothing bool, storesHandles ...RegistryPayload[sop.Handle]) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it")
	}
	if len(storesHandles) == 0 {
		return nil
	}

	// Logged batch will do all or nothing. This is the only one "all or nothing" operation in the Commit process.
	if allOrNothing {
		// For now, keep it simple and rely on transaction commit's optimistic locking & multi-phase checks,
		// together with the logged batch update as shown below.
		batch := connection.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
		for _, sh := range storesHandles {
			updateStatement := fmt.Sprintf("UPDATE %s.%s SET is_idb = ?, p_ida = ?, p_idb = ?, ts = ?, wip_ts = ?, is_del = ? WHERE lid = ?;",
				connection.Config.Keyspace, sh.RegistryTable)
			for _, h := range sh.IDs {
				// Update registry record.
				batch.Query(updateStatement, h.IsActiveIdB, gocql.UUID(h.PhysicalIdA), gocql.UUID(h.PhysicalIdB),
					h.Timestamp, h.WorkInProgressTimestamp, h.IsDeleted, gocql.UUID(h.LogicalId))
			}
		}
		// Failed update all, thus, return err to cause rollback.
		if err := connection.Session.ExecuteBatch(batch); err != nil {
			return err
		}
	} else {
		for _, sh := range storesHandles {
			updateStatement := fmt.Sprintf("UPDATE %s.%s SET is_idb = ?, p_ida = ?, p_idb = ?, ts = ?, wip_ts = ?, is_del = ? WHERE lid = ?;",
				connection.Config.Keyspace, sh.RegistryTable)
			// Fail on 1st encountered error. It is non-critical operation, SOP can "heal" those got left.
			for _, h := range sh.IDs {
				// Update registry record.
				if err := connection.Session.Query(updateStatement, h.IsActiveIdB, gocql.UUID(h.PhysicalIdA), gocql.UUID(h.PhysicalIdB),
					h.Timestamp, h.WorkInProgressTimestamp, h.IsDeleted, gocql.UUID(h.LogicalId)).WithContext(ctx).Exec(); err != nil {
					return err
				}
			}
		}
	}

	// Update redis cache.
	for _, sh := range storesHandles {
		for _, h := range sh.IDs {
			// Tolerate Redis cache failure.
			if err := v.redisCache.SetStruct(ctx, h.LogicalId.ToString(), &h, registryCacheDuration); err != nil {
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
			if err := v.redisCache.GetStruct(ctx, storeLids.IDs[i].ToString(), &h); err != nil {
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

			if err := v.redisCache.SetStruct(ctx, handle.LogicalId.ToString(), &handle, registryCacheDuration); err != nil {
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
		deleteFromCache := func() {
			for _, id := range storeLids.IDs {
				if err := v.redisCache.Delete(ctx, id.ToString()); err != nil && !redis.KeyNotFound(err) {
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
