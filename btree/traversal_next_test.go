package btree

import (
    "testing"
    "github.com/sharedcode/sop"
)

// Exercise moveToNext path where the right child is nil at index, so goRightUp hits root and ends.
func TestNext_GoRightUpToEnd(t *testing.T) {
    b, fnr := newTestBtree[string]()
    // Root with one item and right child nil
    root := newNode[int,string](b.getSlotLength())
    root.newID(sop.NilUUID)
    v := "v"; vv := v
    root.Slots[0] = &Item[int,string]{Key: 50, Value: &vv, ID: sop.NewUUID()}
    root.Count = 1
    root.ChildrenIDs = make([]sop.UUID, 2)
    // left child can be anything or nil; right child nil triggers goRightUp end
    root.ChildrenIDs[1] = sop.NilUUID
    fnr.Add(root)
    b.StoreInfo.RootNodeID = root.ID
    b.StoreInfo.Count = 1

    // Select the only item and call Next; should return false and clear selection
    b.setCurrentItemID(root.ID, 0)
    if ok, err := b.Next(nil); err != nil || ok { t.Fatalf("Next at end should be false,nil; got ok=%v err=%v", ok, err) }
    if b.isCurrentItemSelected() { t.Fatalf("selection should be cleared at end-of-tree") }
}
