package cache

import "github.com/SharedCode/sop"

type l1_mru struct {
	minCapacity int
	maxCapacity int
	dll         *doublyLinkedList[sop.UUID]
	l1Cache     *L1Cache
}

func newL1Mru(l1c *L1Cache, minCapacity, maxCapacity int) *l1_mru {
	return &l1_mru{
		l1Cache:     l1c,
		minCapacity: minCapacity,
		maxCapacity: maxCapacity,
		dll:         newDoublyLinkedList[sop.UUID](),
	}
}

func (m *l1_mru) add(id sop.UUID) *node[sop.UUID] {
	return m.dll.addToHead(id)
}
func (m *l1_mru) remove(n *node[sop.UUID]) {
	m.dll.delete(n)
}
func (m *l1_mru) evict() {
	for {
		if !m.isFull() {
			break
		}
		if id, ok := m.dll.deleteFromTail(); ok {
			if v, found := m.l1Cache.lookup[id]; found {
				v.nodeData = nil
				v.dllNode = nil
				delete(m.l1Cache.lookup, id)
			}
		} else {
			break
		}
	}
}
func (m *l1_mru) isFull() bool {
	return m.dll.count() >= m.maxCapacity
}
