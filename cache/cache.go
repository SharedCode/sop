package cache

// Generic Cache is useful for general MRU cache needs.
type Cache[TK any, TV any] interface {
	Clear()
	Set(key TK, value TV)
	Get(key TK) TV
	Delete(key TK)
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

func (c *cache[TK, TV]) Set(key TK, value TV) {
	if v, ok := c.lookup[key]; ok {
		v.data = value
		c.mru.remove(v.dllNode)
		v.dllNode = c.mru.add(key)
		return
	}
	n := c.mru.add(key)
	c.lookup[key] = &cacheEntry[TK, TV]{
		data:    value,
		dllNode: n,
	}

	c.Evict()
}

func (c *cache[TK, TV]) Get(key TK) TV {
	if v, ok := c.lookup[key]; ok {
		c.mru.remove(v.dllNode)
		v.dllNode = c.mru.add(key)
		return v.data
	}
	var d TV
	return d
}

func (c *cache[TK, TV]) Delete(key TK) {
	if v, ok := c.lookup[key]; ok {
		c.mru.remove(v.dllNode)
		v.dllNode = nil
		delete(c.lookup, key)
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
