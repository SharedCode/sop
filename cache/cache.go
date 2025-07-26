package cache

import "github.com/sharedcode/sop"

// Generic Cache is useful for general MRU cache needs.
type Cache[TK comparable, TV any] interface {
	Clear()
	Set(items []sop.KeyValuePair[TK, TV])
	Get(keys []TK) []TV
	Delete(keys []TK)
	Count() int
	// Returns yes if cache is at max capacity.
	IsFull() bool
	// Evict the Least Recently Used (LRU) items, if cache is at max capacity.
	Evict()
}

type cacheEntry[TK, TV any] struct {
	data    TV
	dllNode *node[TK]
}

type cache[TK comparable, TV any] struct {
	lookup map[TK]*cacheEntry[TK, TV]
	mru    *mru[TK, TV]
}

// Instantiate a new instance of this Cache w/ MRU management logic.
func NewCache[TK comparable, TV any](minCapacity, maxCapacity int) Cache[TK, TV] {
	c := cache[TK, TV]{
		lookup: make(map[TK]*cacheEntry[TK, TV], maxCapacity),
	}
	c.mru = newMru(&c, minCapacity, maxCapacity)
	return &c
}

func (c *cache[TK, TV]) Clear() {
	c.lookup = make(map[TK]*cacheEntry[TK, TV], c.mru.maxCapacity)
	c.mru = newMru(c, c.mru.minCapacity, c.mru.maxCapacity)
}

func (c *cache[TK, TV]) Set(items []sop.KeyValuePair[TK, TV]) {
	for i := range items {
		if v, ok := c.lookup[items[i].Key]; ok {
			v.data = items[i].Value
			c.mru.remove(v.dllNode)
			v.dllNode = c.mru.add(items[i].Key)
			continue
		}
		n := c.mru.add(items[i].Key)
		c.lookup[items[i].Key] = &cacheEntry[TK, TV]{
			data:    items[i].Value,
			dllNode: n,
		}
	}
	c.Evict()
}

func (c *cache[TK, TV]) Get(keys []TK) []TV {
	r := make([]TV, len(keys))
	for i := range keys {
		if v, ok := c.lookup[keys[i]]; ok {
			c.mru.remove(v.dllNode)
			v.dllNode = c.mru.add(keys[i])
			r[i] = v.data
		}
	}
	return r
}

func (c *cache[TK, TV]) Delete(keys []TK) {
	for i := range keys {
		if v, ok := c.lookup[keys[i]]; ok {
			c.mru.remove(v.dllNode)
			v.dllNode = nil
			delete(c.lookup, keys[i])
		}
	}
}

// Returns the count of items store in this L1 Cache.
func (c *cache[TK, TV]) Count() int {
	return len(c.lookup)
}

func (c *cache[TK, TV]) IsFull() bool {
	return c.mru.isFull()
}
func (c *cache[TK, TV]) Evict() {
	c.mru.evict()
}
