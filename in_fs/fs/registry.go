package fs

import (
	"context"
	"fmt"
	log "log/slog"
	"os"
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
	toFilePath ToFilePathFunc
	fileIO     FileIO
	marshaler  sop.Marshaler
	redisCache redis.Cache
}

var registryCacheDuration time.Duration = time.Duration(12 * time.Hour)

// NewRegistryUsingDefaults is synonymous to NewRegistry but uses default implementations of
// necessary parameter interfaces like for file IO, to file path formatter & object marshaler.
func NewRegistryUsingDefaults() sop.Registry {
	return NewRegistry(ToFilePath, defaultFileIO{}, sop.NewMarshaler())
}

// NewRegistry instantiates a new registry for File System storage.
// Parameters are specified for abstractions to things like File IO, filename formatter for efficient storage
// and access of files on directories & marshaler.
func NewRegistry(
	toFilePathFunc ToFilePathFunc,
	fileIO FileIO,
	marshaler sop.Marshaler) sop.Registry {
	return &registry{
		toFilePath: toFilePathFunc,
		fileIO:     fileIO,
		marshaler:  marshaler,
		redisCache: redis.NewClient(),
	}
}

func (r *registry) Add(ctx context.Context, storesHandles ...sop.RegistryPayload[sop.Handle]) error {
	for _, storeHandles := range storesHandles {
		for _, h := range storeHandles.IDs {
			ba, err := r.marshaler.Marshal(h)
			if err != nil {
				return err
			}
			fn := r.toFilePath(storeHandles.RegistryTable, h.LogicalID)
			// WriteFile will add or replace existing file.
			err = r.fileIO.WriteFile(fn, ba, os.ModeAppend)
			if err != nil {
				return err
			}
			// Tolerate Redis cache failure.
			if err := r.redisCache.SetStruct(ctx, h.LogicalID.String(), &h, registryCacheDuration); err != nil {
				log.Error(fmt.Sprintf("Registry Add (redis setstruct) failed, details: %v", err))
			}
		}
	}
	return nil
}

func (r *registry) Update(ctx context.Context, allOrNothing bool, storesHandles ...sop.RegistryPayload[sop.Handle]) error {
	// Logged batch will do all or nothing. This is the only one "all or nothing" operation in the Commit process.
	if allOrNothing {
		// 0. Enforce a Redis based version check for the batch.
		for _, sh := range storesHandles {
			for _, h := range sh.IDs {
				var h2 sop.Handle
				if err := r.redisCache.GetStruct(ctx, h.LogicalID.String(), &h2); err != nil {
					return err
				}
				newVersion := h.Version
				// Version ID is incremental, 'thus we can compare with -1 the previous.
				newVersion--
				if newVersion != h2.Version || !h.IsEqual(&h2) {
					return &UpdateAllOrNothingError{
						Err: fmt.Errorf("Registry Update failed, handle logical ID(%v) version conflict detected", h.LogicalID),
					}
				}
			}
		}

		// 1. Lock the batched files to be updated.
		for _, sh := range storesHandles {
			for _, h := range sh.IDs {
				fn := r.toFilePath(sh.RegistryTable, h.LogicalID)
				if ok, err := r.fileIO.Lock(fn); !ok || err != nil {
					// TODO
				}
				// batch.Query(updateStatement, h.IsActiveIDB, gocql.UUID(h.PhysicalIDA), gocql.UUID(h.PhysicalIDB),
				// 	h.Version, h.WorkInProgressTimestamp, h.IsDeleted, gocql.UUID(h.LogicalID))
			}
		}
		// 2. Update the batched files "atomically".
		//    Write to .tmp, remove from Redis, rename files to .bak, rename .tmp to target filenames, write to Redis, remove .bak files.
		//    Ensure "Get" will not read file to Redis if file is locked.

		// Failed update all, thus, return err to cause rollback.
		if err := connection.Session.ExecuteBatch(batch); err != nil {
			return err
		}
		// 3. Unlock the batched files.

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
func (r *registry) Get(ctx context.Context, storesLids ...sop.RegistryPayload[sop.UUID]) ([]sop.RegistryPayload[sop.Handle], error) {
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
			IDs:           handles,
		})
	}
	return storesHandles, nil
}
func (r *registry) Remove(ctx context.Context, storesLids ...sop.RegistryPayload[sop.UUID]) error {
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
