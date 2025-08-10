package btree

import (
	"testing"

	"github.com/sharedcode/sop"
)

// Cover shiftSlots branch that calls moveArrayElements when position < occupied.
func TestShiftSlots_Shifts(t *testing.T) {
	arr := []int{1, 2, 3, 4, 0}
	// Make space at position 2 among first 4 occupied slots.
	shiftSlots(arr, 2, 4)
	// Now simulate an insert at position 2 and verify layout becomes [1,2,99,3,4].
	arr[2] = 99
	if got := arr; !(got[0] == 1 && got[1] == 2 && got[2] == 99 && got[3] == 3 && got[4] == 4) {
		t.Fatalf("unexpected layout after shift+insert: %v", arr)
	}
}

// Cover moveToPrevious when current node has children and left-down shortcut applies (nil child at index).
func TestMoveToPrevious_WithChildren_GoLeftUpShortcut(t *testing.T) {
	store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: true})
	fnr := &fakeNR[int, string]{n: map[sop.UUID]*Node[int, string]{}}
	si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: fakeIAT[int, string]{}}
	b, _ := New[int, string](store, &si, nil)

	n := newNode[int, string](b.getSlotLength())
	n.newID(sop.NilUUID)
	v := "v"
	vv := v
	n.Slots[0] = &Item[int, string]{Key: 1, Value: &vv, ID: sop.NewUUID()}
	n.Slots[1] = &Item[int, string]{Key: 2, Value: &vv, ID: sop.NewUUID()}
	n.Count = 2
	n.ChildrenIDs = make([]sop.UUID, 3)
	n.ChildrenIDs[0] = sop.NewUUID()
	n.ChildrenIDs[1] = sop.NilUUID // nil child at index triggers goLeftUp shortcut
	n.ChildrenIDs[2] = sop.NewUUID()
	fnr.Add(n)
	b.StoreInfo.RootNodeID = n.ID
	// Ensure B-tree is considered non-empty by API checks.
	b.StoreInfo.Count = 2
	// Set current to index 1 (key 2)
	b.setCurrentItemID(n.ID, 1)

	if ok, err := b.Previous(nil); !ok || err != nil {
		t.Fatalf("Previous failed: %v", err)
	}
	if it, _ := b.GetCurrentItem(nil); it.Key != 1 {
		t.Fatalf("expected to move to left item key=1, got %v", it.Key)
	}
}

// Cover moveToPrevious when descending into left subtree and selecting last item of that leaf.
func TestMoveToPrevious_DescendLeftSubtree_LastItem(t *testing.T) {
	store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: true})
	fnr := &fakeNR[int, string]{n: map[sop.UUID]*Node[int, string]{}}
	si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: fakeIAT[int, string]{}}
	b, _ := New[int, string](store, &si, nil)

	parent := newNode[int, string](b.getSlotLength())
	parent.newID(sop.NilUUID)
	pv := "p"
	pvv := pv
	parent.Slots[0] = &Item[int, string]{Key: 100, Value: &pvv, ID: sop.NewUUID()}
	parent.Count = 1
	parent.ChildrenIDs = make([]sop.UUID, 2)

	child := newNode[int, string](b.getSlotLength())
	child.newID(parent.ID)
	v := "c"
	vv := v
	child.Slots[0] = &Item[int, string]{Key: 10, Value: &vv, ID: sop.NewUUID()}
	child.Slots[1] = &Item[int, string]{Key: 20, Value: &vv, ID: sop.NewUUID()}
	child.Count = 2

	parent.ChildrenIDs[0] = child.ID
	parent.ChildrenIDs[1] = sop.NewUUID()

	fnr.Add(parent)
	fnr.Add(child)
	b.StoreInfo.RootNodeID = parent.ID
	// Total items across tree: 1 (parent) + 2 (child)
	b.StoreInfo.Count = 3

	// Current at parent's index 0; Previous should descend into child and pick last item (20)
	b.setCurrentItemID(parent.ID, 0)
	if ok, err := b.Previous(nil); !ok || err != nil {
		t.Fatalf("Previous descend failed: %v", err)
	}
	if it, _ := b.GetCurrentItem(nil); it.Key != 20 {
		t.Fatalf("expected to land on key=20, got %v", it.Key)
	}
}
