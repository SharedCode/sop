package btree

import (
    "testing"

    "github.com/sharedcode/sop"
)

// Ensure distributeToLeft short-circuits when distributeItemOnNodeWithNilChild attaches a new child.
func TestDistributeToLeft_NilChildShortCircuit(t *testing.T) {
    store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: true, LeafLoadBalancing: true})
    fnr := &fakeNR[int, string]{n: map[sop.UUID]*Node[int, string]{}}
    si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: fakeIAT[int, string]{}}
    b, _ := New[int, string](store, &si, nil)

    // Build a single node that is full and has a ChildrenIDs slice with a nil child
    // so distributeToLeft takes the early nil-child path and attaches a new child.
    n := newNode[int, string](b.getSlotLength())
    n.newID(sop.NilUUID)
    for i := 0; i < b.getSlotLength(); i++ {
        v := "a"; vv := v
        n.Slots[i] = &Item[int, string]{Key: i + 1, Value: &vv, ID: sop.NewUUID()}
    }
    n.Count = b.getSlotLength()
    n.ChildrenIDs = make([]sop.UUID, b.getSlotLength()+1) // all NilUUID but hasChildren() => true
    b.StoreInfo.RootNodeID = n.ID
    fnr.Add(n)

    // Ask distributeToLeft; it should short-circuit and create a new child under n
    item := &Item[int, string]{Key: 0, Value: &[]string{"z"}[0], ID: sop.NewUUID()}
    if err := n.distributeToLeft(nil, b, item); err != nil { t.Fatalf("distributeToLeft err: %v", err) }

    // A new child should be attached (non-nil child id present)
    attached := false
    for _, cid := range n.ChildrenIDs { if !cid.IsNil() { attached = true; break } }
    if !attached { t.Fatalf("expected a new child to be attached via nil-child path") }
}
