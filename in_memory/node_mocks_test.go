package in_memory

import (
	"context"
	"fmt"
	"testing"

	"github.com/SharedCode/sop/btree"
)

var ctx = context.Background()

func Test_MockNodeWithLeftNilChild(t *testing.T) {
	t.Log("Mock MockNodeWithLeftNilChild.\n")
	store := btree.StoreInfo{
		SlotLength: 4,
	}
	si := btree.StoreInterface[int, string]{
		NodeRepository:    newNodeRepository[int, string](),
		ItemActionTracker: newDumbItemActionTracker[int, string](),
	}
	b3, _ := btree.New[int, string](&store, &si)

	for i := 0; i < 25; i++ {
		x := i * 5
		b3.Add(ctx, x, fmt.Sprintf("foo%d", x))
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
	b3.Remove(ctx, 0)
	b3.Remove(ctx, 5)
	b3.Remove(ctx, 10)
	b3.Remove(ctx, 15)
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

	b3.Add(ctx, 26, "foo26")

	t.Log("\nMock MockNodeWithLeftNilChild Next test.\n")
	b3.First(ctx)
	ctr := 0
	for {
		ctr++
		k := b3.GetCurrentKey()
		t.Logf("key: %d", k)
		if ok, _ := b3.Next(ctx); !ok {
			break
		}
	}
	if ctr != 22 {
		t.Errorf("Mock MockNodeWithLeftNilChild Next failed, got = %d, want = 22 items found.", ctr)
	}

	t.Log("\nMock MockNodeWithLeftNilChild Previous test.\n")
	b3.Last(ctx)
	ctr = 0
	for {
		ctr++
		k := b3.GetCurrentKey()
		t.Logf("key: %d", k)
		if ok, _ := b3.Previous(ctx); !ok {
			break
		}
	}
	if ctr != 22 {
		t.Errorf("Mock MockNodeWithLeftNilChild Previous failed, got = %d, want = 22 items found.", ctr)
	}

	t.Log("Mock MockNodeWithLeftNilChild end.\n\n")
}

func Test_MockNodeWithRightNilChild(t *testing.T) {
	t.Log("Mock MockNodeWithRightNilChild.\n")
	store := btree.StoreInfo{
		SlotLength: 4,
	}
	si := btree.StoreInterface[int, string]{
		NodeRepository:    newNodeRepository[int, string](),
		ItemActionTracker: newDumbItemActionTracker[int, string](),
	}
	b3, _ := btree.New[int, string](&store, &si)

	for i := 0; i < 25; i++ {
		x := i * 5
		b3.Add(ctx, x, fmt.Sprintf("foo%d", x))
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
	b3.Remove(ctx, 50)
	b3.Remove(ctx, 55)
	b3.Remove(ctx, 60)
	b3.Remove(ctx, 65)
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

	b3.Add(ctx, 39, "foo39")

	t.Log("\nMock MockNodeWithRightNilChild Next test.\n")
	b3.First(ctx)
	ctr := 0
	for {
		ctr++
		k := b3.GetCurrentKey()
		t.Logf("key: %d", k)
		if ok, _ := b3.Next(ctx); !ok {
			break
		}
	}
	if ctr != 22 {
		t.Errorf("Mock MockNodeWithRightNilChild Next failed, got = %d, want = 22 items found.", ctr)
	}

	t.Log("\nMock MockNodeWithRightNilChild Previous test.\n")
	b3.Last(ctx)
	ctr = 0
	for {
		ctr++
		k := b3.GetCurrentKey()
		t.Logf("key: %d", k)
		if ok, _ := b3.Previous(ctx); !ok {
			break
		}
	}
	if ctr != 22 {
		t.Errorf("Mock MockNodeWithRightNilChild Previous failed, got = %d, want = 22 items found.", ctr)
	}

	t.Log("Mock MockNodeWithRightNilChild end.\n\n")
}

func Test_MockDistributeItemOnNodeWithRightNilChild(t *testing.T) {
	t.Log("Mock DistributeItemOnNodeWithRightNilChild.\n")
	store := btree.StoreInfo{
		SlotLength: 4,
	}
	si := btree.StoreInterface[int, string]{
		NodeRepository:    newNodeRepository[int, string](),
		ItemActionTracker: newDumbItemActionTracker[int, string](),
	}
	b3, _ := btree.New[int, string](&store, &si)

	for i := 0; i < 25; i++ {
		x := i * 5
		b3.Add(ctx, x, fmt.Sprintf("foo%d", x))
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
	b3.Remove(ctx, 50)
	b3.Remove(ctx, 55)
	b3.Remove(ctx, 60)
	b3.Remove(ctx, 65)
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

	b3.Add(ctx, 38, "foo38")
	b3.Add(ctx, 39, "foo39")
	b3.Add(ctx, 50, "foo35")

	const want = 24

	t.Log("\nMock DistributeItemOnNodeWithRightNilChild Next test.\n")
	b3.First(ctx)
	got := 0
	for {
		got++
		k := b3.GetCurrentKey()
		t.Logf("key: %d", k)
		if ok, _ := b3.Next(ctx); !ok {
			break
		}
	}
	if got != want {
		t.Errorf("Mock DistributeItemOnNodeWithRightNilChild Next failed, got = %d, want = %d items found.", got, want)
	}

	t.Log("\nMock DistributeItemOnNodeWithRightNilChild Previous test.\n")
	b3.Last(ctx)
	got = 0
	for {
		got++
		k := b3.GetCurrentKey()
		t.Logf("key: %d", k)
		if ok, _ := b3.Previous(ctx); !ok {
			break
		}
	}
	if got != want {
		t.Errorf("Mock DistributeItemOnNodeWithRightNilChild Previous failed, got = %d, want = %d items found.", got, want)
	}

	t.Log("Mock DistributeItemOnNodeWithRightNilChild end.\n\n")
}
