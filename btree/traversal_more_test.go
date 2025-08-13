package btree

import (
	"testing"

	"github.com/sharedcode/sop"
)

// Previous at the very first item should return false and clear selection (root termination path).
func TestPrevious_AtFirstItem_ReturnsFalse(t *testing.T) {
	b, _ := newTestBtree[string]()
	for _, k := range []int{2, 1, 3} {
		ok, _ := b.Add(nil, k, "v")
		if !ok {
			t.Fatal("seed add")
		}
	}
	if ok, _ := b.First(nil); !ok {
		t.Fatal("First")
	}
	if ok, err := b.Previous(nil); err != nil || ok {
		t.Fatalf("Previous at first should be false,nil; got ok=%v err=%v", ok, err)
	}
	if b.isCurrentItemSelected() {
		t.Fatalf("selection should be cleared at start boundary")
	}
}

// Previous when current node has children should descend into left subtree and pick the last there.
func TestPrevious_DescendLeftSubtree(t *testing.T) {
	b, fnr := newTestBtree[string]()
	// Parent with two items and three children; left child has two items.
	p := newNode[int, string](b.getSlotLength())
	p.newID(sop.NilUUID)
	v := "v"
	vv := v
	p.Slots[0] = &Item[int, string]{Key: 50, Value: &vv, ID: sop.NewUUID()}
	p.Slots[1] = &Item[int, string]{Key: 100, Value: &vv, ID: sop.NewUUID()}
	p.Count = 2
	p.ChildrenIDs = make([]sop.UUID, 3)
	left := newNode[int, string](b.getSlotLength())
	left.newID(p.ID)
	mid := newNode[int, string](b.getSlotLength())
	mid.newID(p.ID)
	right := newNode[int, string](b.getSlotLength())
	right.newID(p.ID)
	p.ChildrenIDs[0] = left.ID
	p.ChildrenIDs[1] = mid.ID
	p.ChildrenIDs[2] = right.ID
	left.Slots[0] = &Item[int, string]{Key: 10, Value: &vv, ID: sop.NewUUID()}
	left.Slots[1] = &Item[int, string]{Key: 20, Value: &vv, ID: sop.NewUUID()}
	left.Count = 2
	fnr.Add(p)
	fnr.Add(left)
	fnr.Add(mid)
	fnr.Add(right)
	b.StoreInfo.RootNodeID = p.ID
	// Ensure non-empty guard passes
	b.StoreInfo.Count = 4
	// Select parent's first item (index 0), then Previous should go into left child last (20)
	b.setCurrentItemID(p.ID, 0)
	if ok, err := b.Previous(nil); err != nil || !ok {
		t.Fatalf("Previous err=%v ok=%v", err, ok)
	}
	if it, _ := b.GetCurrentItem(nil); it.Key != 20 {
		t.Fatalf("want 20, got %d", it.Key)
	}
}

// Early return for unlinkNodeWithNilChild when node has no children.
func TestUnlinkNodeWithNilChild_EarlyReturn(t *testing.T) {
	b, fnr := newTestBtree[string]()
	n := newNode[int, string](b.getSlotLength())
	n.newID(sop.NewUUID())
	// No ChildrenIDs slice => isNilChildren() true
	fnr.Add(n)
	if ok, err := n.unlinkNodeWithNilChild(nil, b); err != nil || ok {
		t.Fatalf("expected false,nil early return; got ok=%v err=%v", ok, err)
	}
}
