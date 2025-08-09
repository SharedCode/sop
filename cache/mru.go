package cache

// mru manages MRU ordering and eviction for the generic cache type.
type mru[TK comparable, TV any] struct {
	minCapacity int
	maxCapacity int
	dll         *doublyLinkedList[TK]
	cache       *cache[TK, TV]
}

func newMru[TK comparable, TV any](c *cache[TK, TV], minCapacity, maxCapacity int) *mru[TK, TV] {
	return &mru[TK, TV]{
		cache:       c,
		minCapacity: minCapacity,
		maxCapacity: maxCapacity,
		dll:         newDoublyLinkedList[TK](),
	}
}

// add inserts the id at the head of the MRU list and returns its node handle.
func (m *mru[TK, TV]) add(id TK) *node[TK] {
	return m.dll.addToHead(id)
}

// remove unchains the node from the MRU list.
func (m *mru[TK, TV]) remove(n *node[TK]) {
	m.dll.delete(n)
}

// evict removes entries from the tail while the cache exceeds its capacity, updating the index.
func (m *mru[TK, TV]) evict() {
	for {
		if !m.isFull() {
			break
		}
		if id, ok := m.dll.deleteFromTail(); ok {
			if v, found := m.cache.lookup[id]; found {
				v.dllNode = nil
				delete(m.cache.lookup, id)
			}
		} else {
			break
		}
	}
}

// isFull reports whether the cache has reached its maximum capacity.
func (m *mru[TK, TV]) isFull() bool {
	return m.dll.count() >= m.maxCapacity
}
