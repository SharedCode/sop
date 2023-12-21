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
	// UpsertTime in milliseconds.
	upsertTime int64
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
		lockId: btree.NewUUID(),
		action: addAction,
		item:   item,
		upsertTime: time.Now().UnixMilli(),
	}
}

func (t *itemActionTracker) Update(item *btree.Item[interface{}, interface{}]) {
	if v, ok := t.items[item.Id]; ok && v.action == addAction {
		return
	}
	t.items[item.Id] = cacheItem{
		lockId: btree.NewUUID(),
		action: updateAction,
		item:   item,
		upsertTime: time.Now().UnixMilli(),
	}
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

// hasConflict will compare the locally cached items' version with their copies in Redis.
// Returns true if there is at least an item that got modified(by another transaction) in Redis.
// Otherwise returns false.
func (t *itemActionTracker) hasConflict(ctx context.Context, itemRedisCache redis.Cache) (bool, error) {
	for uuid := range t.items {
		if _, err := itemRedisCache.Get(ctx, uuid.ToString()); err != nil {
			if !redis.KeyNotFound(err) {
				return false, err
			}
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
			if err := itemRedisCache.Set(ctx, uuid.ToString(), lid.ToString(), duration); err != nil {
				return err
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
