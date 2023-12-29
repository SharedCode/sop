package in_cas_s3

import (
	"context"
	"fmt"
	"time"

	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_cas_s3/redis"
)

type actionType int

const (
	getAction = iota
	addAction
	updateAction
	removeAction
)

type lockRecord struct {
	LockId btree.UUID
	Action actionType
}
type cacheItem struct {
	lockRecord
	item *btree.Item[interface{}, interface{}]
	// upsert time in milliseconds.
	upsertTimeInDB int64
	isLockOwner    bool
}

type itemActionTracker struct {
	items map[btree.UUID]cacheItem
}

// Creates a new Item Action Tracker instance with frontend and backend interface/methods.
func newItemActionTracker() *itemActionTracker {
	return &itemActionTracker{
		items: make(map[btree.UUID]cacheItem),
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

func (t *itemActionTracker) Get(item *btree.Item[interface{}, interface{}]) {
	if _, ok := t.items[item.Id]; !ok {
		t.items[item.Id] = cacheItem{
			lockRecord: lockRecord{
				LockId: btree.NewUUID(),
				Action: getAction,
			},
			item: item,
		}
	}
}

func (t *itemActionTracker) Add(item *btree.Item[interface{}, interface{}]) {
	t.items[item.Id] = cacheItem{
		lockRecord: lockRecord{
			LockId: btree.NewUUID(),
			Action: addAction,
		},
		item:           item,
		upsertTimeInDB: item.UpsertTime,
	}
	// Update upsert time, now that we have kept its DB value intact, for use in conflict resolution.
	item.UpsertTime = Now()
}

func (t *itemActionTracker) Update(item *btree.Item[interface{}, interface{}]) {
	if v, ok := t.items[item.Id]; ok && v.Action == addAction {
		return
	}
	t.items[item.Id] = cacheItem{
		lockRecord: lockRecord{
			LockId: btree.NewUUID(),
			Action: updateAction,
		},
		item:           item,
		upsertTimeInDB: item.UpsertTime,
	}
	// Update upsert time, now that we have kept its DB value intact, for use in conflict resolution.
	item.UpsertTime = Now()
}

func (t *itemActionTracker) Remove(item *btree.Item[interface{}, interface{}]) {
	if v, ok := t.items[item.Id]; ok && v.Action == addAction {
		delete(t.items, item.Id)
		return
	}
	t.items[item.Id] = cacheItem{
		lockRecord: lockRecord{
			LockId: btree.NewUUID(),
			Action: removeAction,
		},
		item: item,
	}
}

// lock the tracked items in Redis in preparation to finalize the transaction commit.
// This should work in combination of optimistic locking implemented by hasConflict above.
func (t *itemActionTracker) lock(ctx context.Context, itemRedisCache redis.Cache, duration time.Duration) error {
	for uuid, cachedItem := range t.items {
		var readItem lockRecord
		if err := itemRedisCache.GetStruct(ctx, uuid.ToString(), &readItem); err != nil {
			if !redis.KeyNotFound(err) {
				return err
			}
			// Item does not exist, upsert it.
			if err := itemRedisCache.SetStruct(ctx, uuid.ToString(), &(cachedItem.lockRecord), duration); err != nil {
				return err
			}
			// Use a 2nd "get" to ensure we "won" the lock attempt & fail if not.
			if err := itemRedisCache.GetStruct(ctx, uuid.ToString(), &readItem); err != nil {
				return err
			} else if readItem.LockId != cachedItem.LockId {
				return fmt.Errorf("lock(item: %v) call detected conflict.", uuid)
			}
			// We got the item locked, ensure we can unlock it.
			cachedItem.isLockOwner = true
			t.items[uuid] = cachedItem
		}
		// Item found in Redis.
		if readItem.LockId == cachedItem.LockId {
			continue
		}
		// Lock compatibility check.
		if readItem.Action == getAction && cachedItem.Action == getAction {
			continue
		}
		return fmt.Errorf("lock(item: %v) call detected conflict.", uuid)
	}
	return nil
}

// unlock will attempt to unlock or delete all tracked items from redis.
func (t *itemActionTracker) unlock(ctx context.Context, itemRedisCache redis.Cache) error {
	for uuid, cachedItem := range t.items {
		if cachedItem.isLockOwner {
			if err := itemRedisCache.Delete(ctx, uuid.ToString()); err != nil {
				if !redis.KeyNotFound(err) {
					return err
				}
			}
		}
	}
	return nil
}
