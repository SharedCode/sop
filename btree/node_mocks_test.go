package btree

import (
	"fmt"
	"testing"
)

// in-memory mockup implementation of NodeRepository. Uses a map to manage nodes in memory.
type nodeRepository[TK Comparable, TV any] struct {
	lookup map[UUID]*Node[TK, TV]
}
func newNodeRepository[TK Comparable, TV any]() NodeRepository[TK, TV] {
	return &nodeRepository[TK, TV]{
		lookup: make(map[UUID]*Node[TK, TV]),
	}
}
func (nr *nodeRepository[TK, TV]) Upsert(n *Node[TK, TV]) error {
	nr.lookup[n.Id] = n
	return nil
}
func (nr *nodeRepository[TK, TV]) Get(nodeId UUID) (*Node[TK, TV], error) {
	v, _ := nr.lookup[nodeId]
	return v, nil
}
func (nr *nodeRepository[TK, TV]) Remove(nodeId UUID) error {
	delete(nr.lookup, nodeId)
	return nil
}

func Test_MockNodeWithLeftNilChild(t *testing.T) {
	t.Log("Mock MockNodeWithLeftNilChild.\n")
	store := Store{
		SlotLength: 4,
	}
	si := StoreInterface[int, string]{
		NodeRepository: newNodeRepository[int, string](),
	}
	b3 := NewBtree[int, string](store, &si)

	for i := 0; i < 25; i++ {
		x := i * 5
		b3.Add(x, fmt.Sprintf("foo%d", x))
	}
	// node illustration:
	// root: 70
	// node1: 20,45
	// node2: 95,10
	// node11: 0,5,10,15
	// node12: 25,30,35,40
	// node13: 50,55,60,65
	// node21: 75,80,85,90
	// node22: 100,105
	// node23: 115,120

	// Remove node 11 to create nil child(leftmost child) on node1.
	b3.Remove(0)
	b3.Remove(5)
	b3.Remove(10)
	b3.Remove(15)
	// node illustration after deleting 0,5,10,15:
	// root: 70
	// node1: 20,45
	// node2: 95,10
	// node11: (deleted, nil child)
	// node12: 25,30,35,40
	// node13: 50,55,60,65
	// node21: 75,80,85,90
	// node22: 100,105
	// node23: 115,120

	b3.Add(26, "foo26")

	t.Log("\nMock MockNodeWithLeftNilChild MoveToNext test.\n")
	b3.MoveToFirst()
	ctr := 0
	for {
		ctr++
		t.Logf("key: %d", b3.GetCurrentKey())
		if ok, _ := b3.MoveToNext(); !ok {
			break
		}
	}
	if ctr != 22 {
		t.Errorf("Mock MockNodeWithLeftNilChild MoveToNext failed, got = %d, want = 22 items found.", ctr)
	}

	t.Log("\nMock MockNodeWithLeftNilChild MoveToPrevious test.\n")
	b3.MoveToLast()
	ctr = 0
	for {
		ctr++
		t.Logf("key: %d", b3.GetCurrentKey())
		if ok, _ := b3.MoveToPrevious(); !ok {
			break
		}
	}
	if ctr != 22 {
		t.Errorf("Mock MockNodeWithLeftNilChild MoveToPrevious failed, got = %d, want = 22 items found.", ctr)
	}

	t.Log("Mock MockNodeWithLeftNilChild end.\n\n")
}

func Test_MockNodeWithRightNilChild(t *testing.T) {
	t.Log("Mock MockNodeWithRightNilChild.\n")
	store := Store{
		SlotLength: 4,
	}
	si := StoreInterface[int, string]{
		NodeRepository: newNodeRepository[int, string](),
	}
	b3 := NewBtree[int, string](store, &si)

	for i := 0; i < 25; i++ {
		x := i * 5
		b3.Add(x, fmt.Sprintf("foo%d", x))
	}
	// node illustration:
	// root: 70
	// node1: 20,45
	// node2: 95,10
	// node11: 0,5,10,15
	// node12: 25,30,35,40
	// node13: 50,55,60,65
	// node21: 75,80,85,90
	// node22: 100,105
	// node23: 115,120

	// Remove node 11 to create nil child(leftmost child) on node1.
	b3.Remove(50)
	b3.Remove(55)
	b3.Remove(60)
	b3.Remove(65)
	// node illustration after deleting 50,55,60,65:
	// root: 70
	// node1: 20,45
	// node2: 95,10
	// node11: 0,5,10,15
	// node12: 25,30,35,40
	// node13: (deleted, nil child)
	// node21: 75,80,85,90
	// node22: 100,105
	// node23: 115,120

	b3.Add(39, "foo39")

	t.Log("\nMock MockNodeWithRightNilChild MoveToNext test.\n")
	b3.MoveToFirst()
	ctr := 0
	for {
		ctr++
		t.Logf("key: %d", b3.GetCurrentKey())
		if ok, _ := b3.MoveToNext(); !ok {
			break
		}
	}
	if ctr != 22 {
		t.Errorf("Mock MockNodeWithRightNilChild MoveToNext failed, got = %d, want = 22 items found.", ctr)
	}

	t.Log("\nMock MockNodeWithRightNilChild MoveToPrevious test.\n")
	b3.MoveToLast()
	ctr = 0
	for {
		ctr++
		t.Logf("key: %d", b3.GetCurrentKey())
		if ok, _ := b3.MoveToPrevious(); !ok {
			break
		}
	}
	if ctr != 22 {
		t.Errorf("Mock MockNodeWithRightNilChild MoveToPrevious failed, got = %d, want = 22 items found.", ctr)
	}

	t.Log("Mock MockNodeWithRightNilChild end.\n\n")
}

func Test_MockDistributeItemOnNodeWithRightNilChild(t *testing.T) {
	t.Log("Mock DistributeItemOnNodeWithRightNilChild.\n")
	store := Store{
		SlotLength: 4,
	}
	si := StoreInterface[int, string]{
		NodeRepository: newNodeRepository[int, string](),
	}
	b3 := NewBtree[int, string](store, &si)

	for i := 0; i < 25; i++ {
		x := i * 5
		b3.Add(x, fmt.Sprintf("foo%d", x))
	}
	// node illustration:
	// root: 70
	// node1: 20,45
	// node2: 95,10
	// node11: 0,5,10,15
	// node12: 25,30,35,40
	// node13: 50,55,60,65
	// node21: 75,80,85,90
	// node22: 100,105
	// node23: 115,120

	// Remove node 11 to create nil child(leftmost child) on node1.
	b3.Remove(50)
	b3.Remove(55)
	b3.Remove(60)
	b3.Remove(65)
	// node illustration after deleting 50,55,60,65:
	// root: 70
	// node1: 20,45
	// node2: 95,10
	// node11: 0,5,10,15
	// node12: 25,30,35,40
	// node13: (deleted, nil child)
	// node21: 75,80,85,90
	// node22: 100,105
	// node23: 115,120

	b3.Add(38, "foo38")
	b3.Add(39, "foo39")
	b3.Add(50, "foo35")

	const want = 24

	t.Log("\nMock DistributeItemOnNodeWithRightNilChild MoveToNext test.\n")
	b3.MoveToFirst()
	got := 0
	for {
		got++
		t.Logf("key: %d", b3.GetCurrentKey())
		if ok, _ := b3.MoveToNext(); !ok {
			break
		}
	}
	if got != want {
		t.Errorf("Mock DistributeItemOnNodeWithRightNilChild MoveToNext failed, got = %d, want = %d items found.", got, want)
	}

	t.Log("\nMock DistributeItemOnNodeWithRightNilChild MoveToPrevious test.\n")
	b3.MoveToLast()
	got = 0
	for {
		got++
		t.Logf("key: %d", b3.GetCurrentKey())
		if ok, _ := b3.MoveToPrevious(); !ok {
			break
		}
	}
	if got != want {
		t.Errorf("Mock DistributeItemOnNodeWithRightNilChild MoveToPrevious failed, got = %d, want = %d items found.", got, want)
	}

	t.Log("Mock DistributeItemOnNodeWithRightNilChild end.\n\n")
}
