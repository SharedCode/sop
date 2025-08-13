package btree

import (
	"context"
	"github.com/sharedcode/sop"
	"testing"
)

func TestRemoveCurrentItem_WithChildren_LeftNilChild_Shortcut(t *testing.T) {
	b, fnr := newTestBtree[string]()
	// Build a node with two items and a nil left child at index 0
	n := newNode[int, string](b.getSlotLength())
	n.newID(sop.NilUUID)
	v := "v"
	vv := v
	n.Slots[0] = &Item[int, string]{Key: 10, Value: &vv, ID: sop.NewUUID()}
	n.Slots[1] = &Item[int, string]{Key: 20, Value: &vv, ID: sop.NewUUID()}
	n.Count = 2
	n.ChildrenIDs = make([]sop.UUID, 3)
	// Left child (index 0) nil triggers removeItemOnNodeWithNilChild path
	n.ChildrenIDs[0] = sop.NilUUID
	n.ChildrenIDs[1] = sop.NewUUID()
	n.ChildrenIDs[2] = sop.NewUUID()
	fnr.Add(n)
	b.StoreInfo.RootNodeID = n.ID
	b.StoreInfo.Count = 2

	// Select index 0 and remove
	b.setCurrentItemID(n.ID, 0)
	ok, err := b.RemoveCurrentItem(context.TODO())
	if err != nil || !ok {
		t.Fatalf("RemoveCurrentItem err=%v ok=%v", err, ok)
	}
	if b.isCurrentItemSelected() {
		t.Fatalf("selection should be cleared")
	}
	if b.Count() != 1 {
		t.Fatalf("Store count should decrement to 1, got %d", b.Count())
	}
}

func TestRemoveCurrentItem_RootSingleItem_Delete(t *testing.T) {
	b, fnr := newTestBtree[string]()
	root := newNode[int, string](b.getSlotLength())
	root.newID(sop.NilUUID)
	v := "v"
	vv := v
	root.Slots[0] = &Item[int, string]{Key: 1, Value: &vv, ID: sop.NewUUID()}
	root.Count = 1
	fnr.Add(root)
	b.StoreInfo.RootNodeID = root.ID
	b.StoreInfo.Count = 1
	b.setCurrentItemID(root.ID, 0)

	ok, err := b.RemoveCurrentItem(context.TODO())
	if err != nil || !ok {
		t.Fatalf("RemoveCurrentItem root err=%v ok=%v", err, ok)
	}
	if b.isCurrentItemSelected() {
		t.Fatalf("selection should be cleared")
	}
	if b.Count() != 0 {
		t.Fatalf("store count should be 0, got %d", b.Count())
	}
	if root.Count != 0 || root.Slots[0] != nil {
		t.Fatalf("root not cleared")
	}
}

func TestRemoveCurrentItem_NonRoot_UnlinkEarly(t *testing.T) {
	b, fnr := newTestBtree[string]()
	parent := newNode[int, string](b.getSlotLength())
	parent.newID(sop.NilUUID)
	// No ChildrenIDs on parent -> hasChildren() false triggers early return in unlink
	child := newNode[int, string](b.getSlotLength())
	child.newID(parent.ID)
	v := "v"
	vv := v
	child.Slots[0] = &Item[int, string]{Key: 1, Value: &vv, ID: sop.NewUUID()}
	child.Count = 1
	fnr.Add(parent)
	fnr.Add(child)
	b.StoreInfo.RootNodeID = parent.ID
	b.StoreInfo.Count = 1
	b.setCurrentItemID(child.ID, 0)

	ok, err := b.RemoveCurrentItem(context.TODO())
	if err != nil || !ok {
		t.Fatalf("RemoveCurrentItem unlink err=%v ok=%v", err, ok)
	}
	if b.isCurrentItemSelected() {
		t.Fatalf("selection should be cleared")
	}
	if b.Count() != 0 {
		t.Fatalf("store count should be 0, got %d", b.Count())
	}
}
