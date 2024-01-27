package in_red_ck

import (
	"context"
	"fmt"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
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
	versionInDB int
	isLockOwner bool
}

type itemActionTracker[TK btree.Comparable, TV any] struct {
	items map[sop.UUID]cacheItem[TK, TV]
}

// Creates a new Item Action Tracker instance with frontend and backend interface/methods.
func newItemActionTracker[TK btree.Comparable, TV any]() *itemActionTracker[TK, TV] {
	return &itemActionTracker[TK, TV]{
		items: make(map[sop.UUID]cacheItem[TK, TV]),
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

func (t *itemActionTracker[TK, TV]) Get(item *btree.Item[TK, TV]) {
	if _, ok := t.items[item.Id]; !ok {
		t.items[item.Id] = cacheItem[TK, TV]{
			lockRecord: lockRecord{
				LockId: sop.NewUUID(),
				Action: getAction,
			},
			item:        item,
			versionInDB: item.Version,
		}
	}
}

func (t *itemActionTracker[TK, TV]) Add(item *btree.Item[TK, TV]) {
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
}

func (t *itemActionTracker[TK, TV]) Update(item *btree.Item[TK, TV]) {
	v, ok := t.items[item.Id]
	if ok {
		if v.Action == addAction {
			return
		}
		v.lockRecord.Action = updateAction
		v.item = item
		t.items[item.Id] = v
		return
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
}

func (t *itemActionTracker[TK, TV]) Remove(item *btree.Item[TK, TV]) {
	if v, ok := t.items[item.Id]; ok && v.Action == addAction {
		delete(t.items, item.Id)
		return
	}
	t.items[item.Id] = cacheItem[TK, TV]{
		lockRecord: lockRecord{
			LockId: sop.NewUUID(),
			Action: removeAction,
		},
		item:        item,
		versionInDB: item.Version,
	}
}

func (t *itemActionTracker[TK, TV]) hasTrackedItems() bool {
	return len(t.items) > 0
}

// checkTrackedItems for conflict so we can remove "race condition" caused issue.
// Returns nil if there are no tracked items or no conflict, otherwise returns an error.
func (t *itemActionTracker[TK, TV]) checkTrackedItems(ctx context.Context, itemRedisCache redis.Cache) error {
	for uuid, cachedItem := range t.items {
		var readItem lockRecord
		if err := itemRedisCache.GetStruct(ctx, redis.FormatLockKey(uuid.String()), &readItem); err != nil {
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
func (t *itemActionTracker[TK, TV]) lock(ctx context.Context, itemRedisCache redis.Cache, duration time.Duration) error {
	for uuid, cachedItem := range t.items {
		var readItem lockRecord
		if err := itemRedisCache.GetStruct(ctx, redis.FormatLockKey(uuid.String()), &readItem); err != nil {
			if !redis.KeyNotFound(err) {
				return err
			}
			// Item does not exist, upsert it.
			if err := itemRedisCache.SetStruct(ctx, redis.FormatLockKey(uuid.String()), &(cachedItem.lockRecord), duration); err != nil {
				return err
			}
			// Use a 2nd "get" to ensure we "won" the lock attempt & fail if not.
			if err := itemRedisCache.GetStruct(ctx, redis.FormatLockKey(uuid.String()), &readItem); err != nil {
				return err
			} else if readItem.LockId != cachedItem.LockId {
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
func (t *itemActionTracker[TK, TV]) unlock(ctx context.Context, itemRedisCache redis.Cache) error {
	var lastErr error
	for uuid, cachedItem := range t.items {
		if !cachedItem.isLockOwner {
			continue
		}
		if err := itemRedisCache.Delete(ctx, redis.FormatLockKey(uuid.String())); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
