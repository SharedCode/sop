package btree

import (
	"testing"

	"github.com/sharedcode/sop"
)

// Covers AddItem path including distribute() and promote() controller loops.
func TestAddItem_CoversDistributeAndPromote(t *testing.T) {
	store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: true, IsValueDataInNodeSegment: true, LeafLoadBalancing: true})
	fnr := &fakeNR[int, string]{n: map[sop.UUID]*Node[int, string]{}}
	si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: fakeIAT[int, string]{}}
	b, _ := New[int, string](store, &si, nil)

	// Create root
	root := newNode[int, string](b.getSlotLength())
	root.newID(sop.NilUUID)
	b.StoreInfo.RootNodeID = root.ID
	fnr.Add(root)

	// Insert enough items via AddItem to trigger split and possible distribution
	vals := []int{10, 20, 30, 40, 50, 60}
	for _, k := range vals {
		v := "v"
		vv := v
		ok, err := b.AddItem(nil, &Item[int, string]{Key: k, Value: &vv, ID: sop.NewUUID()})
		if err != nil || !ok {
			t.Fatalf("AddItem %d err=%v ok=%v", k, err, ok)
		}
	}
	if got := b.Count(); got != int64(len(vals)) {
		t.Fatalf("count=%d", got)
	}
	// Ensure tree has grown to have children at root (promote happened)
	rn, _ := b.getNode(nil, b.StoreInfo.RootNodeID)
	if !rn.hasChildren() {
		t.Fatalf("expected root to have children after AddItem operations")
	}
}
