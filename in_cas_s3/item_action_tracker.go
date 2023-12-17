package in_cas_s3

import (
	"context"
	"encoding/json"
	"slices"

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

type cacheData struct {
	item   interface{}
	action actionType
}

type itemActionTracker struct {
	items map[btree.UUID]cacheData
}

type itemActionTrackerTyped[TK btree.Comparable, TV any] struct {
	realItemActionTracker *itemActionTracker
}

func newItemActionTracker[TK btree.Comparable, TV any]() *itemActionTrackerTyped[TK, TV] {
	iat := itemActionTracker{
		items: make(map[btree.UUID]cacheData),
	}
	return &itemActionTrackerTyped[TK, TV] {
		realItemActionTracker: &iat,
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

func (t *itemActionTrackerTyped[TK, TV]) Get(item *btree.Item[TK, TV]) {
	if _, ok := t.realItemActionTracker.items[item.Id]; !ok {
		t.realItemActionTracker.items[item.Id] = cacheData{
			item:   item,
			action: getAction,
		}
	}
}

func (t *itemActionTrackerTyped[TK, TV]) Add(item *btree.Item[TK, TV]) {
	t.realItemActionTracker.items[item.Id] = cacheData{
		item:   item,
		action: addAction,
	}
}

func (t *itemActionTrackerTyped[TK, TV]) Update(item *btree.Item[TK, TV]) {
	if v, ok := t.realItemActionTracker.items[item.Id]; ok && v.action == addAction {
		v.item = item
		return
	}
	t.realItemActionTracker.items[item.Id] = cacheData{
		item:   item,
		action: updateAction,
	}
}

func (t *itemActionTrackerTyped[TK, TV]) Remove(item *btree.Item[TK, TV]) {
	if v, ok := t.realItemActionTracker.items[item.Id]; ok && v.action == addAction {
		delete(t.realItemActionTracker.items, item.Id)
		return
	}
	t.realItemActionTracker.items[item.Id] = cacheData{
		item:   item,
		action: removeAction,
	}
}

// hasConflict will compare the locally cached items' version with their copies in Redis.
// Returns true if there is at least an item that got modified(by another transaction) in Redis.
// Otherwise returns false.
func (t *itemActionTracker) hasConflict(ctx context.Context, itemRedisCache redis.Cache) (bool, error) {
	for uuid,_ := range t.items {
		var target interface{}
		if err := itemRedisCache.GetStruct(ctx, uuid.ToString(), &target); err != nil {
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
func (t *itemActionTracker) lock(ctx context.Context, itemRedisCache redis.Cache, toLock bool) (bool, error) {
	for uuid, cachedData := range t.items {
		inLocalCacheItem := cachedData.item.(btree.TimestampedData)
		inLocalCacheItem.SetUpsertTime()
		var target interface{}
		if err := itemRedisCache.GetStruct(ctx, uuid.ToString(), &target); err != nil {
			if redis.KeyNotFound(err) {
				if err := itemRedisCache.SetStruct(ctx, uuid.ToString(), inLocalCacheItem, 0); err != nil {
					return false, err
				}
				if err := itemRedisCache.GetStruct(ctx, uuid.ToString(), &target); err != nil {
					ba1, err := json.Marshal(target)
					if err != nil {
						return false, err
					}
					ba2, err := json.Marshal(inLocalCacheItem)
					if err != nil {
							return false, err
					}
					if slices.Compare[[]byte](ba1, ba2) != 0 {
						return false, nil
					}
				}
			}
			return false, err
		}
	}
	return true, nil
}
