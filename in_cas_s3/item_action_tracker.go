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

type cacheItem struct {
	lockId btree.UUID
	action actionType
	item   *btree.Item[interface{}, interface{}]
	// upsert time in milliseconds.
	upsertTimeInDB int64
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
			lockId: btree.NewUUID(),
			action: getAction,
			item:   item,
		}
	}
}

func (t *itemActionTracker) Add(item *btree.Item[interface{}, interface{}]) {
	t.items[item.Id] = cacheItem{
		lockId:         btree.NewUUID(),
		action:         addAction,
		item:           item,
		upsertTimeInDB: item.UpsertTime,
	}
	// Update upsert time, now that we have kept its DB value intact, for use in conflict resolution.
	item.UpsertTime = time.Now().UnixMilli()
}

func (t *itemActionTracker) Update(item *btree.Item[interface{}, interface{}]) {
	if v, ok := t.items[item.Id]; ok && v.action == addAction {
		return
	}
	t.items[item.Id] = cacheItem{
		lockId:         btree.NewUUID(),
		action:         updateAction,
		item:           item,
		upsertTimeInDB: item.UpsertTime,
	}
	// Update upsert time, now that we have kept its DB value intact, for use in conflict resolution.
	item.UpsertTime = time.Now().UnixMilli()
}

func (t *itemActionTracker) Remove(item *btree.Item[interface{}, interface{}]) {
	if v, ok := t.items[item.Id]; ok && v.action == addAction {
		delete(t.items, item.Id)
		return
	}
	t.items[item.Id] = cacheItem{
		lockId: btree.NewUUID(),
		action: removeAction,
		item:   item,
	}
}

// hasConflict simply checks whether tracked items are also in-flight in other transactions &
// returns true if such, false otherwise. Commit will cause rollback if returned true.
func (t *itemActionTracker) hasConflict(ctx context.Context, itemRedisCache redis.Cache) (bool, error) {
	for uuid := range t.items {
		if _, err := itemRedisCache.Get(ctx, uuid.ToString()); err != nil {
			if redis.KeyNotFound(err) {
				continue
			}
			return false, err
		}
		// If item is found in Redis, it means it is already being committed by another transaction.
		return true, nil
	}
	return false, nil
}

// lock the tracked items in Redis in preparation to finalize the transaction commit.
// This should work in combination of optimistic locking implemented by hasConflict above.
func (t *itemActionTracker) lock(ctx context.Context, itemRedisCache redis.Cache, duration time.Duration) error {
	for uuid, cachedData := range t.items {
		lid := cachedData.lockId
		if tlid, err := itemRedisCache.Get(ctx, uuid.ToString()); err != nil {
			if !redis.KeyNotFound(err) {
				return err
			}
			// Item does not exist, upsert it.
			if err := itemRedisCache.Set(ctx, uuid.ToString(), lid.ToString(), duration); err != nil {
				return err
			}
			// Use a 2nd "get" to ensure we "won" the lock attempt & fail if not.
			if tlid, err := itemRedisCache.Get(ctx, uuid.ToString()); err != nil {
				return err
			} else if tlid != lid.ToString() {
				return fmt.Errorf("lock(item: %v) call detected conflict.", uuid)
			}
		} else if tlid != lid.ToString() {
			return fmt.Errorf("lock(item: %v) call detected conflict.", uuid)
		}
	}
	return nil
}

// unlock will attempt to unlock or delete all tracked items from redis. It will issue a delete even
// if there is an error and complete trying to delete them all and return the last error encountered
// as a sample, if there is.
func (t *itemActionTracker) unlock(ctx context.Context, itemRedisCache redis.Cache) error {
	var lastError error
	for uuid := range t.items {
		if err := itemRedisCache.Delete(ctx, uuid.ToString()); err != nil {
			if !redis.KeyNotFound(err) {
				lastError = err
			}
		}
	}
	return lastError
}
