package in_cas_s3

import (
	"github.com/SharedCode/sop/btree"
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
