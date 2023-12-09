package in_cas_s3

import "github.com/SharedCode/sop/btree"

type actionType int
const(
	getAction = iota
	addAction
	updateAction
	remoAction
)

type cacheData[TK btree.Comparable, TV any] struct {
	item *btree.Item[TK, TV]
	action actionType
}

type itemCacheRepository[TK btree.Comparable, TV any] struct {
	items map[btree.UUID]cacheData[TK, TV]
}

func newItemCacheRepository[TK btree.Comparable, TV any]() btree.ItemCacheRepository[TK, TV] {
	return &itemCacheRepository[TK, TV]{}
}

func (c *itemCacheRepository[TK, TV])Add(item *btree.Item[TK, TV]) bool {
	// if c.items
	return false
}

func (c *itemCacheRepository[TK, TV])Get(itemId btree.UUID) *btree.Item[TK, TV] {
	return nil
}

func (c *itemCacheRepository[TK, TV])Update(item *btree.Item[TK, TV]) bool {
	return false
}

func (c *itemCacheRepository[TK, TV])Remove(itemId btree.UUID) bool {
	return false
}
