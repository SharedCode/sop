package btree

import (
	"testing"

	"github.com/sharedcode/sop"
)

// Covers RemoveCurrentItem path where node has children and we replace with next leaf item.
func TestRemoveCurrentItem_InternalReplaceThenLeafDelete(t *testing.T) {
	b, fnr, root := buildSmallTree()
	// Move to key 30 (middle in initial set) ensuring current is internal before deletion
	if ok, _ := b.Find(nil, 30, false); !ok {
		t.Fatal("find 30")
	}
	// Root should have children after buildSmallTree; removing 30 will exercise moveToNext and leaf delete path
	if n, _ := b.getNode(nil, b.currentItemRef.getNodeID()); !n.hasChildren() {
		t.Fatalf("expected current node to have children for internal delete path")
	}
	if ok, err := b.RemoveCurrentItem(nil); err != nil || !ok {
		t.Fatalf("RemoveCurrentItem err=%v ok=%v", err, ok)
	}
	if got := b.Count(); got != 4 {
		t.Fatalf("count after remove=%d", got)
	}
	// Sanity: key 30 should no longer be present
	if ok, _ := b.Find(nil, 30, false); ok {
		t.Fatalf("30 should be removed")
	}
	// Avoid unused warnings
	_ = fnr
	_ = root
}

// Covers FindWithID success for a non-first duplicate.
func TestFindWithID_DuplicateNonFirst(t *testing.T) {
	store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: false})
	fnr := &fakeNR[int, string]{n: map[sop.UUID]*Node[int, string]{}}
	si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: fakeIAT[int, string]{}}
	b, _ := New[int, string](store, &si, nil)
	root := newNode[int, string](b.getSlotLength())
	root.newID(sop.NilUUID)
	b.StoreInfo.RootNodeID = root.ID
	fnr.Add(root)

	// Add duplicates for key 5
	ids := []sop.UUID{}
	for i := 0; i < 3; i++ {
		v := "v"
		vv := v
		id := sop.NewUUID()
		ids = append(ids, id)
		if ok, err := b.AddItem(nil, &Item[int, string]{Key: 5, Value: &vv, ID: id}); err != nil || !ok {
			t.Fatalf("add dup %d", i)
		}
	}
	// FindWithID should walk Next until matching id
	if ok, err := b.FindWithID(nil, 5, ids[2]); err != nil || !ok {
		t.Fatalf("FindWithID err=%v ok=%v", err, ok)
	}
	it, _ := b.GetCurrentItem(nil)
	if it.ID != ids[2] {
		t.Fatalf("expected id match on third duplicate")
	}
}

// Covers GetCurrentValue and GetCurrentItem when no item is selected.
func TestCurrent_Fetch_NoSelection(t *testing.T) {
	b, _ := newTestBtree[string]()
	// No selection: both should return zero values with no error
	if _, err := b.GetCurrentValue(nil); err != nil {
		t.Fatalf("GetCurrentValue unexpected err: %v", err)
	}
	if _, err := b.GetCurrentItem(nil); err != nil {
		t.Fatalf("GetCurrentItem unexpected err: %v", err)
	}
}
