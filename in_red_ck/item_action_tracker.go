package in_red_ck

import (
	"context"
	"fmt"
	log "log/slog"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
	"github.com/SharedCode/sop/in_red_ck/redis"
)

type actionType int

const (
	defaultAction = iota
	getAction
	addAction
	updateAction
	removeAction
)

type lockRecord struct {
	LockId sop.UUID
	Action actionType
}
type cacheItem[TK btree.Comparable, TV any] struct {
	lockRecord
	item *btree.Item[TK, TV]
	// Version of the item as read from DB.
	versionInDB       int
	isLockOwner       bool
	inflightItemValue *TV
}

type itemActionTracker[TK btree.Comparable, TV any] struct {
	storeInfo        *btree.StoreInfo
	items            map[sop.UUID]cacheItem[TK, TV]
	forDeletionItems []sop.UUID
	redisCache       redis.Cache
	blobStore        cas.BlobStore
}

// Creates a new Item Action Tracker instance with frontend and backend interface/methods.
func newItemActionTracker[TK btree.Comparable, TV any](storeInfo *btree.StoreInfo, redisCache redis.Cache, blobStore cas.BlobStore) *itemActionTracker[TK, TV] {
	return &itemActionTracker[TK, TV]{
		storeInfo:  storeInfo,
		items:      make(map[sop.UUID]cacheItem[TK, TV]),
		redisCache: redisCache,
		blobStore:  blobStore,
	}
}

// Sample use-case logic table:
// Current		Action		Outcome
// _			Add			ForAdd
// _			Get			Get(fetch from blobStore)
// _			Update		ForUpdate
// _			Remove		ForRemove
// ForAdd		Get			ForAdd
// ForAdd		Update		ForAdd
// ForAdd		Remove		_
// ForRemove 	Remove		ForRemove
// ForRemove 	Get			ForRemove
// ForUpdate	Remove		ForRemove
// Get			Get			Get
// Get			Remove		ForRemove
// Get			Update		ForUpdate

func (t *itemActionTracker[TK, TV]) Get(ctx context.Context, item *btree.Item[TK, TV]) error {
	if val, ok := t.items[item.Id]; !ok || val.item.ValueNeedsFetch {
		if item.Value == nil && item.ValueNeedsFetch {
			var v TV
			if t.storeInfo.IsValueDataGloballyCached {
				if err := t.redisCache.GetStruct(ctx, t.formatKey(item.Id.String()), &v); err != nil {
					if !redis.KeyNotFound(err) {
						log.Error(err.Error())
					}
					// If item not found in Redis or an error fetching it, fetch from Blob store.
					if err := t.blobStore.GetOne(ctx, t.storeInfo.BlobTable, item.Id, &v); err != nil {
						return err
					}
					// Just log Redis error since it is just secondary.
					if err := t.redisCache.SetStruct(ctx, t.formatKey(item.Id.String()), &v, nodeCacheDuration); err != nil {
						log.Error(err.Error())
					}
				}
			} else {
				if err := t.blobStore.GetOne(ctx, t.storeInfo.BlobTable, item.Id, &v); err != nil {
					return err
				}
			}
			item.Value = &v
			item.ValueNeedsFetch = false
			if ok {
				return nil
			}
		}
		t.items[item.Id] = cacheItem[TK, TV]{
			lockRecord: lockRecord{
				LockId: sop.NewUUID(),
				Action: getAction,
			},
			item:        item,
			versionInDB: item.Version,
		}
	}
	return nil
}

func (t *itemActionTracker[TK, TV]) Add(ctx context.Context, item *btree.Item[TK, TV]) error {
	t.items[item.Id] = cacheItem[TK, TV]{
		lockRecord: lockRecord{
			LockId: sop.NewUUID(),
			Action: addAction,
		},
		item:        item,
		versionInDB: item.Version,
	}
	// Update upsert time, now that we have kept its DB value intact, for use in conflict resolution.
	item.Version++
	return nil
}

func (t *itemActionTracker[TK, TV]) Update(ctx context.Context, item *btree.Item[TK, TV]) error {
	v, ok := t.items[item.Id]
	if ok {
		if v.Action == addAction {
			return nil
		}
		v.lockRecord.Action = updateAction
		v.item = item
		t.items[item.Id] = v
		if item.Version == v.versionInDB {
			item.Version++
		}
		return nil
	}
	t.items[item.Id] = cacheItem[TK, TV]{
		lockRecord: lockRecord{
			LockId: sop.NewUUID(),
			Action: updateAction,
		},
		item:        item,
		versionInDB: item.Version,
	}
	// Update upsert time, now that we have kept its DB value intact, for use in conflict resolution.
	item.Version++
	return nil
}

func (t *itemActionTracker[TK, TV]) Remove(ctx context.Context, item *btree.Item[TK, TV]) error {
	if v, ok := t.items[item.Id]; ok && v.Action == addAction {
		delete(t.items, item.Id)
		return nil
	}
	t.items[item.Id] = cacheItem[TK, TV]{
		lockRecord: lockRecord{
			LockId: sop.NewUUID(),
			Action: removeAction,
		},
		item:        item,
		versionInDB: item.Version,
	}
	return nil
}

func (t *itemActionTracker[TK, TV]) hasTrackedItems() bool {
	return len(t.items) > 0
}

// checkTrackedItems for conflict so we can remove "race condition" caused issue.
// Returns nil if there are no tracked items or no conflict, otherwise returns an error.
func (t *itemActionTracker[TK, TV]) checkTrackedItems(ctx context.Context) error {
	for uuid, cachedItem := range t.items {
		if cachedItem.Action == addAction {
			continue
		}
		var readItem lockRecord
		if err := t.redisCache.GetStruct(ctx, redis.FormatLockKey(uuid.String()), &readItem); err != nil {
			return err
		}
		// Item found in Redis.
		if readItem.LockId == cachedItem.LockId {
			continue
		}
		// Lock compatibility check.
		if readItem.Action == getAction && cachedItem.Action == getAction {
			continue
		}
		return fmt.Errorf("lock(item: %v) call detected conflict", uuid)
	}
	return nil
}

// lock the tracked items in Redis in preparation to finalize the transaction commit.
// This should work in combination of optimistic locking.
func (t *itemActionTracker[TK, TV]) lock(ctx context.Context, duration time.Duration) error {
	for uuid, cachedItem := range t.items {
		if cachedItem.Action == addAction {
			continue
		}
		var readItem lockRecord
		if err := t.redisCache.GetStruct(ctx, redis.FormatLockKey(uuid.String()), &readItem); err != nil {
			if !redis.KeyNotFound(err) {
				return err
			}
			// Item does not exist, upsert it.
			if err := t.redisCache.SetStruct(ctx, redis.FormatLockKey(uuid.String()), &(cachedItem.lockRecord), duration); err != nil {
				return err
			}
			// Use a 2nd "get" to ensure we "won" the lock attempt & fail if not.
			if err := t.redisCache.GetStruct(ctx, redis.FormatLockKey(uuid.String()), &readItem); err != nil {
				return err
			} else if readItem.LockId != cachedItem.LockId {
				if readItem.Action == getAction && cachedItem.Action == getAction {
					continue
				}
				if readItem.LockId.IsNil() {
					return fmt.Errorf("lock(item: %v) call can't attain a lock in Redis", uuid)
				}
				return fmt.Errorf("lock(item: %v) call detected conflict", uuid)
			}
			// We got the item locked, ensure we can unlock it.
			cachedItem.isLockOwner = true
			t.items[uuid] = cachedItem
			continue
		}
		// Item found in Redis.
		if readItem.LockId == cachedItem.LockId {
			continue
		}
		// Lock compatibility check.
		if readItem.Action == getAction && cachedItem.Action == getAction {
			continue
		}
		return fmt.Errorf("lock(item: %v) call detected conflict", uuid)
	}
	return nil
}

// unlock will attempt to unlock or delete all tracked items from redis.
func (t *itemActionTracker[TK, TV]) unlock(ctx context.Context) error {
	var lastErr error
	for uuid, cachedItem := range t.items {
		if cachedItem.Action == addAction {
			continue
		}
		if !cachedItem.isLockOwner {
			continue
		}
		if err := t.redisCache.Delete(ctx, redis.FormatLockKey(uuid.String())); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
