package btree

import (
    "testing"

    "github.com/sharedcode/sop"
)

// Ensure AddItem returns an error when fetching the root node fails.
func TestAddItem_RootFetchError(t *testing.T) {
    store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: true})
    fnr := &fakeNRSelectiveErr[int, string]{n: map[sop.UUID]*Node[int, string]{}, errs: map[sop.UUID]bool{}}
    si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: fakeIAT[int, string]{}}
    b, _ := New[int, string](store, &si, nil)

    // Pre-create a root ID and set Count > 0 to force getRootNode to fetch via repo
    rootID := sop.NewUUID()
    b.StoreInfo.RootNodeID = rootID
    b.StoreInfo.Count = 1

    // Force repo to error for root fetch
    fnr.errs[rootID] = true

    v := "v"; vv := v
    if ok, err := b.AddItem(nil, &Item[int, string]{Key: 1, Value: &vv, ID: sop.NewUUID()}); err == nil || ok {
        t.Fatalf("expected AddItem to fail on root fetch error, got ok=%v err=%v", ok, err)
    }
}

// Cover the path where AddItem triggers distribute and an error occurs during distribution.
func TestAddItem_DistributeError_Propagates(t *testing.T) {
    store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: true, LeafLoadBalancing: true})
    fnr := &fakeNRSelectiveErr[int, string]{n: map[sop.UUID]*Node[int, string]{}, errs: map[sop.UUID]bool{}}
    si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: fakeIAT[int, string]{}}
    b, _ := New[int, string](store, &si, nil)

    // Build a parent with two children: left (full) and right (has vacancy)
    parent := newNode[int, string](b.getSlotLength())
    parent.newID(sop.NilUUID)
    parent.Count = 1

    left := newNode[int, string](b.getSlotLength())
    left.newID(parent.ID)
    // Make left full
    v := "v"; vv := v
    for i := 0; i < b.getSlotLength(); i++ {
        left.Slots[i] = &Item[int, string]{Key: 10 + i, Value: &vv, ID: sop.NewUUID()}
    }
    left.Count = b.getSlotLength()

    right := newNode[int, string](b.getSlotLength())
    right.newID(parent.ID)
    // right has vacancy (Count 0)

    parent.ChildrenIDs = make([]sop.UUID, 2)
    parent.ChildrenIDs[0] = left.ID
    parent.ChildrenIDs[1] = right.ID
    // Set a valid separator key on parent slot 0 to avoid nil deref during add traversal
    parent.Slots[0] = &Item[int, string]{Key: 60, Value: &vv, ID: sop.NewUUID()}

    // Add to repo
    fnr.Add(parent)
    fnr.Add(left)
    fnr.Add(right)

    // Mark the right sibling to error when fetched during distributeToRight
    fnr.errs[right.ID] = true

    // Wire as the B-tree root
    b.StoreInfo.RootNodeID = parent.ID
    b.StoreInfo.Count = int64(parent.Count)

    // Add an item that will go into the left child and trigger distribution to right
    // Choose a key that will insert at the beginning (forcing tempSlots[0] path)
    if ok, err := b.AddItem(nil, &Item[int, string]{Key: 5, Value: &vv, ID: sop.NewUUID()}); err == nil || ok {
        t.Fatalf("expected AddItem to propagate distribute error, got ok=%v err=%v", ok, err)
    }
}
