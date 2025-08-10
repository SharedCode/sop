package btree

import (
    "testing"

    "github.com/sharedcode/sop"
)

// Ensure AddItem returns false on duplicate when IsUnique=true.
func TestAddItem_DuplicateUnique_ReturnsFalse(t *testing.T) {
    store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: true, IsValueDataInNodeSegment: true})
    fnr := &fakeNR[int, string]{n: map[sop.UUID]*Node[int, string]{}}
    si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: fakeIAT[int, string]{}}
    b, _ := New[int, string](store, &si, nil)

    root := newNode[int, string](b.getSlotLength())
    root.newID(sop.NilUUID)
    b.StoreInfo.RootNodeID = root.ID
    fnr.Add(root)

    // Seed an item with key=7
    if ok, err := b.Add(nil, 7, "a"); err != nil || !ok { t.Fatalf("seed add err=%v ok=%v", err, ok) }
    // Attempt AddItem with same key, distinct ID; should be rejected when unique
    vv := "b"
    dup := &Item[int, string]{Key: 7, Value: &vv, ID: sop.NewUUID()}
    if ok, err := b.AddItem(nil, dup); err != nil { t.Fatalf("AddItem duplicate err: %v", err) } else if ok {
        t.Fatalf("expected AddItem duplicate to return false")
    }
}
