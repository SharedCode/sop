package btree

// Scenario file merged from: additem_duplicate_test.go, additem_error_test.go, additem_more_test.go
// NOTE: Pure content merge; no refactors. Original files removed.

import (
	"testing"

	"github.com/sharedcode/sop"
)

// (from additem_duplicate_test.go)
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
	if ok, err := b.Add(nil, 7, "a"); err != nil || !ok {
		t.Fatalf("seed add err=%v ok=%v", err, ok)
	}
	// Attempt AddItem with same key, distinct ID; should be rejected when unique
	vv := "b"
	dup := &Item[int, string]{Key: 7, Value: &vv, ID: sop.NewUUID()}
	if ok, err := b.AddItem(nil, dup); err != nil {
		t.Fatalf("AddItem duplicate err: %v", err)
	} else if ok {
		t.Fatalf("expected AddItem duplicate to return false")
	}
}

// (from additem_error_test.go)
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

	v := "v"
	vv := v
	if ok, err := b.AddItem(nil, &Item[int, string]{Key: 1, Value: &vv, ID: sop.NewUUID()}); err == nil || ok {
		t.Fatalf("expected AddItem to fail on root fetch error, got ok=%v err=%v", ok, err)
	}
}

// (from additem_error_test.go)
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
	v := "v"
	vv := v
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

// (from additem_more_test.go)
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
