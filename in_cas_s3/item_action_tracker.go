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

type cacheData[TK btree.Comparable, TV any] struct {
	item   *btree.Item[TK, TV]
	action actionType
}

type itemActionTracker[TK btree.Comparable, TV any] struct {
	storeInterface *StoreInterface[TK, TV]
	items          map[btree.UUID]cacheData[TK, TV]
}

func newItemActionTracker[TK btree.Comparable, TV any](storeInterface *StoreInterface[TK, TV]) btree.ItemActionTracker[TK, TV] {
	return &itemActionTracker[TK, TV]{
		storeInterface: storeInterface,
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
		item := btree.Item[TK, TV]{}
		t.items[item.Id] = cacheData[TK, TV]{
			item:   &item,
			action: getAction,
		}
	}
}

func (t *itemActionTracker[TK, TV]) Add(item *btree.Item[TK, TV]) {
	t.items[item.Id] = cacheData[TK, TV]{
		item:   item,
		action: addAction,
	}
}

func (t *itemActionTracker[TK, TV]) Update(item *btree.Item[TK, TV]) {
	if v, ok := t.items[item.Id]; ok && v.action == addAction {
		v.item = item
		return
	}
	t.items[item.Id] = cacheData[TK, TV]{
		item:   item,
		action: updateAction,
	}
}

func (t *itemActionTracker[TK, TV]) Remove(item *btree.Item[TK, TV]) {
	if v, ok := t.items[item.Id]; ok && v.action == addAction {
		delete(t.items, item.Id)
		return
	}
	t.items[item.Id] = cacheData[TK, TV]{
		item: item,
		action: removeAction,
	}
}
