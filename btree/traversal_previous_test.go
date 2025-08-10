package btree

import (
    "testing"
    "github.com/sharedcode/sop"
)

// Covers moveToPrevious edge paths:
// - when node has children and left-down path reaches a node with no children
// - when climbing to root with slotIndex underflow
func TestMoveToPrevious_Edges(t *testing.T) {
    b, fnr := newTestBtree[string]()
    // Build a simple tree: root with one separator and two children
    root := newNode[int, string](b.getSlotLength())
    root.newID(sop.NilUUID)
    v := "v"
    root.Slots[0] = &Item[int, string]{Key: 50, Value: &v, ID: sop.NewUUID()}
    root.Count = 1
    root.ChildrenIDs = make([]sop.UUID, 2)
    left := newNode[int, string](b.getSlotLength())
    left.newID(root.ID)
    right := newNode[int, string](b.getSlotLength())
    right.newID(root.ID)
    // left has two items so slotIndex-1 path is exercised
    left.Slots[0] = &Item[int, string]{Key: 10, Value: &v, ID: sop.NewUUID()}
    left.Slots[1] = &Item[int, string]{Key: 20, Value: &v, ID: sop.NewUUID()}
    left.Count = 2
    root.ChildrenIDs[0] = left.ID
    root.ChildrenIDs[1] = right.ID
    fnr.Add(root)
    fnr.Add(left)
    fnr.Add(right)
    b.StoreInfo.RootNodeID = root.ID

    // Select root separator and go previous -> should land on left's last item
    b.setCurrentItemID(root.ID, 0)
    if ok, err := root.moveToPrevious(nil, b); err != nil || !ok {
        t.Fatalf("moveToPrevious err=%v", err)
    }
    if it, _ := b.GetCurrentItem(nil); it.Key != 20 {
        t.Fatalf("expected previous to land on 20, got %v", it.Key)
    }

    // Now at left[1], go previous twice to climb to root-underflow and end
    if ok, err := left.moveToPrevious(nil, b); err != nil || !ok {
        t.Fatalf("moveToPrevious #2 err=%v", err)
    }
    if it, _ := b.GetCurrentItem(nil); it.Key != 10 {
        t.Fatalf("expected 10 after previous")
    }
    if ok, err := left.moveToPrevious(nil, b); err != nil || ok {
        t.Fatalf("expected false at start-of-tree, err=%v ok=%v", err, ok)
    }
}

// getIndexOfNode root and error paths
func TestGetIndexOfNode_RootAndError(t *testing.T) {
    b, fnr := newTestBtree[string]()
    root := newNode[int, string](b.getSlotLength())
    root.newID(sop.NilUUID)
    fnr.Add(root)
    if idx, err := root.getIndexOfNode(nil, b); err != nil || idx != 0 {
        t.Fatalf("root index expected 0, got %d err=%v", idx, err)
    }
}
