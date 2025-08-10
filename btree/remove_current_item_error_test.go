package btree

import (
    "context"
    "testing"
    "github.com/sharedcode/sop"
)

// Trigger the RemoveCurrentItem branch where removeItemOnNodeWithNilChild returns false
// (both adjacent children non-nil) and moveToNext returns an error from the repository.
func TestRemoveCurrentItem_MoveToNext_Error(t *testing.T) {
    store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: true})
    fnr := &fakeNRSelectiveErr[int, string]{n: map[sop.UUID]*Node[int, string]{}, errs: map[sop.UUID]bool{}}
    si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: fakeIAT[int, string]{}}
    b, _ := New[int, string](store, &si, nil)

    // Root with one item and two non-nil children so early nil-child delete path is skipped.
    root := newNode[int, string](b.getSlotLength())
    root.newID(sop.NilUUID)
    v := "v"; vv := v
    root.Slots[0] = &Item[int, string]{Key: 10, Value: &vv, ID: sop.NewUUID()}
    root.Count = 1
    left := newNode[int, string](b.getSlotLength()); left.newID(root.ID)
    right := newNode[int, string](b.getSlotLength()); right.newID(root.ID)
    root.ChildrenIDs = make([]sop.UUID, 2)
    root.ChildrenIDs[0] = left.ID
    root.ChildrenIDs[1] = right.ID

    // Add to repo and force an error only when fetching the right child during moveToNext.
    fnr.Add(root); fnr.Add(left); fnr.Add(right)
    fnr.errs[right.ID] = true

    b.StoreInfo.RootNodeID = root.ID
    b.StoreInfo.Count = 1
    b.setCurrentItemID(root.ID, 0)

    ok, err := b.RemoveCurrentItem(context.TODO())
    if err == nil || ok {
        t.Fatalf("expected RemoveCurrentItem to return error via moveToNext, got ok=%v err=%v", ok, err)
    }
}
