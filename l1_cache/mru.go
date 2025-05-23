package l1_cache

import "github.com/SharedCode/sop"

type mru struct {
	minCapacity int
	maxCapacity int
	dll         *doublyLinkedList[sop.UUID]
	lcCache     *l1Cache
}

func newMru(l1c *l1Cache, minCapacity, maxCapacity int) *mru {
	return &mru{
		lcCache:     l1c,
		minCapacity: minCapacity,
		maxCapacity: maxCapacity,
		dll:         newDoublyLinkedList[sop.UUID](),
	}
}

func (m *mru) add(id sop.UUID) *node[sop.UUID] {
	return m.dll.addToHead(id)
}
func (m *mru) remove(n *node[sop.UUID]) {
	m.dll.delete(n)
}
func (m *mru) prune() {
	for {
		if !m.isFull() {
			break
		}
		if id, ok := m.dll.deleteFromTail(); ok {
			delete(m.lcCache.handles, id)
		} else {
			break
		}
	}
}
func (m *mru) isFull() bool {
	return m.dll.count() >= m.maxCapacity
}
