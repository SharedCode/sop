package btree

import (
	"testing"

	"github.com/sharedcode/sop"
)

// No selection -> RemoveCurrentItem returns false, nil.
func TestRemoveCurrentItem_NoSelection(t *testing.T) {
	b, _ := newTestBtree[string]()
	b.setCurrentItemID(sop.NilUUID, 0)
	if ok, err := b.RemoveCurrentItem(nil); ok || err != nil {
		t.Fatalf("expected false,nil when no selection, got ok=%v err=%v", ok, err)
	}
}

// Current points to a node/slot that's nil -> returns false, nil.
func TestRemoveCurrentItem_SlotNil(t *testing.T) {
	b, fnr := newTestBtree[string]()
	n := newNode[int, string](b.getSlotLength())
	n.newID(sop.NilUUID)
	// No slot value assigned
	n.Count = 1
	fnr.Add(n)
	b.StoreInfo.RootNodeID = n.ID
	b.StoreInfo.Count = 1
	b.setCurrentItemID(n.ID, 0)
	if ok, err := b.RemoveCurrentItem(nil); ok || err != nil {
		t.Fatalf("expected false,nil when slot is nil, got ok=%v err=%v", ok, err)
	}
}

// Leaf deletion path: node has no children, fixVacatedSlot runs, selection cleared.
func TestRemoveCurrentItem_LeafDelete(t *testing.T) {
	b, fnr := newTestBtree[string]()
	n := newNode[int, string](b.getSlotLength())
	n.newID(sop.NilUUID)
	v := "x"
	n.Slots[0] = &Item[int, string]{Key: 1, Value: &v, ID: sop.NewUUID()}
	n.Count = 1
	fnr.Add(n)
	b.StoreInfo.RootNodeID = n.ID
	b.StoreInfo.Count = 1
	b.setCurrentItemID(n.ID, 0)

	ok, err := b.RemoveCurrentItem(nil)
	if err != nil || !ok {
		t.Fatalf("RemoveCurrentItem leaf err=%v ok=%v", err, ok)
	}
	if b.isCurrentItemSelected() {
		t.Fatalf("expected selection cleared")
	}
	if b.StoreInfo.Count != 0 {
		t.Fatalf("expected store count=0, got %d", b.StoreInfo.Count)
	}
}

// Internal with children: exercise removeItemOnNodeWithNilChild early path returning true.
func TestRemoveCurrentItem_WithChildren_ShiftPath(t *testing.T) {
	b, fnr := newTestBtree[string]()
	n := newNode[int, string](b.getSlotLength())
	n.newID(sop.NilUUID)
	v := "v"
	vv := v
	n.Slots[0] = &Item[int, string]{Key: 10, Value: &vv, ID: sop.NewUUID()}
	n.Slots[1] = &Item[int, string]{Key: 20, Value: &vv, ID: sop.NewUUID()}
	n.Count = 2
	// Children with a nil on the right of index 1 to trigger the right-nil shift path
	n.ChildrenIDs = make([]sop.UUID, 3)
	n.ChildrenIDs[0] = sop.NewUUID()
	n.ChildrenIDs[1] = sop.NewUUID()
	n.ChildrenIDs[2] = sop.NilUUID
	fnr.Add(n)
	b.StoreInfo.RootNodeID = n.ID
	b.StoreInfo.Count = 2
	b.setCurrentItemID(n.ID, 1) // select key 20 at index 1

	ok, err := b.RemoveCurrentItem(nil)
	if err != nil || !ok {
		t.Fatalf("RemoveCurrentItem with-children err=%v ok=%v", err, ok)
	}
	if b.isCurrentItemSelected() {
		t.Fatalf("expected selection cleared")
	}
	if b.StoreInfo.Count != 1 {
		t.Fatalf("expected store count=1, got %d", b.StoreInfo.Count)
	}
}

// Internal node with both children non-nil: RemoveCurrentItem should move to a leaf,
// replace the internal slot with the leaf item, then delete on the leaf via fixVacatedSlot.
func TestRemoveCurrentItem_Internal_MoveToLeafReplacement(t *testing.T) {
	b, fnr := newTestBtree[string]()
	// Root with two items and three non-nil children
	root := newNode[int, string](b.getSlotLength())
	root.newID(sop.NilUUID)
	v := "v"
	vv := v
	root.Slots[0] = &Item[int, string]{Key: 50, Value: &vv, ID: sop.NewUUID()}
	root.Slots[1] = &Item[int, string]{Key: 100, Value: &vv, ID: sop.NewUUID()}
	root.Count = 2
	root.ChildrenIDs = make([]sop.UUID, 3)
	// Create left and middle children (non-nil) so early remove path returns false
	left := newNode[int, string](b.getSlotLength())
	left.newID(root.ID)
	mid := newNode[int, string](b.getSlotLength())
	mid.newID(root.ID)
	right := newNode[int, string](b.getSlotLength())
	right.newID(root.ID)
	root.ChildrenIDs[0] = left.ID
	root.ChildrenIDs[1] = mid.ID
	root.ChildrenIDs[2] = right.ID
	// Left is a leaf with two items
	left.Slots[0] = &Item[int, string]{Key: 10, Value: &vv, ID: sop.NewUUID()}
	left.Slots[1] = &Item[int, string]{Key: 20, Value: &vv, ID: sop.NewUUID()}
	left.Count = 2

	fnr.Add(root)
	fnr.Add(left)
	fnr.Add(mid)
	fnr.Add(right)
	b.StoreInfo.RootNodeID = root.ID
	// Total items across nodes (root has 2, left has 2). We only care about decremented count.
	b.StoreInfo.Count = 4

	// Select root index 0
	b.setCurrentItemID(root.ID, 0)
	ok, err := b.RemoveCurrentItem(nil)
	if err != nil || !ok {
		t.Fatalf("RemoveCurrentItem internal->leaf path err=%v ok=%v", err, ok)
	}
	if b.isCurrentItemSelected() {
		t.Fatalf("expected selection cleared")
	}
	if b.StoreInfo.Count != 3 {
		t.Fatalf("expected store count decremented to 3, got %d", b.StoreInfo.Count)
	}
}
