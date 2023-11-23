package btree

import (
	"fmt"
	"testing"
)

func Test_MockDistributeItemOnNodeWithNilChild(t *testing.T) {
	t.Log("Mock DistributeItemOnNodeWithNilChild.\n")
	store := Store{
		SlotLength: 4,
	}
	si := StoreInterface[int,string] {
		NodeRepository: NewInMemoryNodeRepository[int, string](),
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

	b3.MoveToFirst()
	ctr := 0
	for {
		ctr++
		fmt.Printf("key: %d", b3.GetCurrentKey())
		if ok,_ := b3.MoveToNext(); !ok {
			break
		}
	}
	if ctr != 22 {
		t.Errorf("Mock DistributeItemOnNodeWithNilChild failed, got = %d, want = 22 items found.", ctr)
	}

	t.Log("Mock DistributeItemOnNodeWithNilChild end.\n\n")
}
