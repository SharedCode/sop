package btree

import (
	"testing"

	"github.com/sharedcode/sop"
)

func TestFindWithID_Table(t *testing.T) {
	b, _, _ := buildSmallTree()
	// Allow duplicates
	b.StoreInfo.IsUnique = false
	if ok, err := b.Add(nil, 100, "x1"); err != nil || !ok {
		t.Fatalf("add 100 #1 err=%v", err)
	}
	if ok, err := b.Add(nil, 100, "x2"); err != nil || !ok {
		t.Fatalf("add 100 #2 err=%v", err)
	}

	// Capture both IDs via traversal
	if ok, _ := b.Find(nil, 100, true); !ok {
		t.Fatalf("find 100 first")
	}
	it, _ := b.GetCurrentItem(nil)
	id1 := it.ID
	// Move to next duplicate (same key)
	if ok, _ := b.Next(nil); !ok {
		t.Fatalf("next to second duplicate")
	}
	it2, _ := b.GetCurrentItem(nil)
	if it2.Key != 100 {
		t.Fatalf("expected same key 100 for second duplicate")
	}
	id2 := it2.ID

	cases := []struct {
		key int
		id  sop.UUID
	}{
		{100, id1},
		{100, id2},
	}
	for _, c := range cases {
		if ok, err := b.FindWithID(nil, c.key, c.id); err != nil || !ok {
			t.Fatalf("FindWithID(%d,%v) failed: %v", c.key, c.id, err)
		}
		got, _ := b.GetCurrentItem(nil)
		if got.ID != c.id {
			t.Fatalf("FindWithID got wrong id: %v want %v", got.ID, c.id)
		}
	}
}

func TestAPI_Last_AddIfNotExist_Upsert_Update_Remove(t *testing.T) {
	b, _ := newTestBtree[string]()

	// Seed
	for _, kv := range []struct {
		k int
		v string
	}{{2, "b"}, {1, "a"}, {3, "c"}} {
		if ok, err := b.Add(nil, kv.k, kv.v); err != nil || !ok {
			t.Fatalf("add %d err=%v", kv.k, err)
		}
	}

	// Last()
	if ok, err := b.Last(nil); err != nil || !ok {
		t.Fatalf("Last err=%v", err)
	}
	if k := b.GetCurrentKey().Key; k != 3 {
		t.Fatalf("Last want 3 got %d", k)
	}

	// AddIfNotExist
	if ok, err := b.AddIfNotExist(nil, 2, "bb"); err != nil {
		t.Fatalf("AddIfNotExist err=%v", err)
	} else if ok {
		t.Fatalf("AddIfNotExist should be false for existing key")
	}
	if ok, err := b.AddIfNotExist(nil, 4, "d"); err != nil || !ok {
		t.Fatalf("AddIfNotExist new failed: %v", err)
	}

	// Upsert add then update
	if ok, err := b.Upsert(nil, 5, "e"); err != nil || !ok {
		t.Fatalf("Upsert add failed: %v", err)
	}
	if ok, err := b.Upsert(nil, 5, "ee"); err != nil || !ok {
		t.Fatalf("Upsert update failed: %v", err)
	}
	if ok, err := b.Find(nil, 5, false); err != nil || !ok {
		t.Fatalf("Find 5 after upsert: %v", err)
	}
	if it, _ := b.GetCurrentItem(nil); *it.Value != "ee" {
		t.Fatalf("Upsert did not update value")
	}

	// Update by key
	if ok, err := b.Update(nil, 1, "aa"); err != nil || !ok {
		t.Fatalf("Update err=%v", err)
	}
	if ok, _ := b.Find(nil, 1, false); !ok {
		t.Fatalf("Find 1 after update")
	}
	if it, _ := b.GetCurrentItem(nil); *it.Value != "aa" {
		t.Fatalf("Update did not apply")
	}

	// Remove by key
	if ok, err := b.Remove(nil, 2); err != nil || !ok {
		t.Fatalf("Remove 2 err=%v", err)
	}
	if ok, _ := b.Find(nil, 2, false); ok {
		t.Fatalf("2 should be gone")
	}
}

func TestGetters_And_Lock(t *testing.T) {
	b, _ := newTestBtree[string]()
	if !b.IsUnique() {
		t.Fatalf("IsUnique should be true for test setup")
	}
	si := b.GetStoreInfo()
	if si.SlotLength == 0 {
		t.Fatalf("GetStoreInfo returned zero SlotLength")
	}
	if err := b.Lock(nil, false); err != nil {
		t.Fatalf("Lock returned error: %v", err)
	}
}

// Cover updateChildrenParent and getChildren
func TestUpdateChildrenParent_And_GetChildren(t *testing.T) {
	store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: true, IsValueDataInNodeSegment: true})
	fnr := &fakeNR[int, string]{n: map[sop.UUID]*Node[int, string]{}}
	si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: fakeIAT[int, string]{}}
	b, _ := New[int, string](store, &si, nil)
	parent := newNode[int, string](b.getSlotLength())
	parent.newID(sop.NilUUID)
	parent.ChildrenIDs = make([]sop.UUID, b.getSlotLength()+1)
	// Two children
	c1 := newNode[int, string](b.getSlotLength())
	c1.newID(parent.ID)
	c2 := newNode[int, string](b.getSlotLength())
	c2.newID(parent.ID)
	parent.ChildrenIDs[0] = c1.ID
	parent.ChildrenIDs[1] = c2.ID
	fnr.Add(parent)
	fnr.Add(c1)
	fnr.Add(c2)
	if err := parent.updateChildrenParent(nil, b); err != nil {
		t.Fatalf("updateChildrenParent err: %v", err)
	}
	if c1.ParentID != parent.ID || c2.ParentID != parent.ID {
		t.Fatalf("children parent not updated")
	}
	kids, err := parent.getChildren(nil, b)
	if err != nil || len(kids) < 2 || kids[0] == nil || kids[1] == nil {
		t.Fatalf("getChildren failed: %v", err)
	}
}

// Cover UpdateCurrentNodeItem success path and IsValueDataInNodeSegment getter.
func TestUpdateCurrentNodeItem_And_FlagsGetter(t *testing.T) {
	b, fnr := newTestBtree[string]()
	root := newNode[int, string](b.getSlotLength())
	root.newID(sop.NilUUID)
	v1 := "old"
	it1 := &Item[int, string]{Key: 1, Value: &v1, ID: sop.NewUUID()}
	root.Slots[0] = it1
	root.Count = 1
	b.StoreInfo.RootNodeID = root.ID
	fnr.Add(root)
	b.setCurrentItemID(root.ID, 0)

	v2 := "new"
	it2 := &Item[int, string]{Key: 1, Value: &v2, ID: sop.NewUUID()}
	if ok, err := b.UpdateCurrentNodeItem(nil, it2); !ok || err != nil {
		t.Fatalf("UpdateCurrentNodeItem failed: %v", err)
	}
	if root.Slots[0] != it2 {
		t.Fatalf("node slot not replaced")
	}
	if b.currentItem != it2 {
		t.Fatalf("btree currentItem not updated")
	}

	// Getter coverage
	_ = b.IsValueDataInNodeSegment()
}

// --- Transaction wrapper coverage: preconditions, rollback-on-error, and happy path ---

func TestWithTransaction_PreconditionsAndErrors(t *testing.T) {
	// Base btree for delegation
	b, _ := newTestBtree[string]()

	// Case 1: read op without begun -> rollback and error
	tx1 := &mockTx{begun: false, mode: sop.ForReading}
	w1 := NewBtreeWithTransaction(tx1, b)
	if ok, err := w1.Find(nil, 1, true); ok || err == nil {
		t.Fatalf("Find without begun should fail")
	}
	if tx1.rollbackCount != 1 {
		t.Fatalf("expected rollback on read without begun")
	}

	// Case 2: write op with wrong mode -> rollback and error
	tx2 := &mockTx{begun: true, mode: sop.ForReading}
	w2 := NewBtreeWithTransaction(tx2, b)
	if ok, err := w2.Add(nil, 1, "x"); ok || err == nil {
		t.Fatalf("Add in read mode should fail and not succeed")
	}
	if tx2.rollbackCount != 1 {
		t.Fatalf("expected rollback on write in read mode")
	}

	// Case 3: delegate error triggers rollback
	store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: true, IsValueDataInNodeSegment: true})
	fnr := &fakeNR[int, string]{n: map[sop.UUID]*Node[int, string]{}}
	si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: iatErr[int, string]{}}
	bErr, _ := New[int, string](store, &si, nil)
	tx3 := &mockTx{begun: true, mode: sop.ForWriting}
	w3 := NewBtreeWithTransaction(tx3, bErr)
	if ok, err := w3.Add(nil, 1, "x"); ok || err == nil {
		t.Fatalf("expected underlying Add error to bubble and not succeed")
	}
	if tx3.rollbackCount != 1 {
		t.Fatalf("expected rollback when delegate returns error")
	}

	// Case 4: happy path, writer mode, begun
	tx4 := &mockTx{begun: true, mode: sop.ForWriting}
	w4 := NewBtreeWithTransaction(tx4, b)
	if ok, err := w4.Add(nil, 2, "y"); !ok || err != nil {
		t.Fatalf("Add happy path failed: %v", err)
	}
	if tx4.rollbackCount != 0 {
		t.Fatalf("unexpected rollback on happy path")
	}
}

// Exercise wrapper FindWithID and Lock to cover remaining withtransaction paths.
func TestWithTransaction_FindWithID_And_Lock(t *testing.T) {
	// Underlying tree allowing duplicates
	store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: false, IsValueDataInNodeSegment: true})
	fnr := &fakeNR[int, string]{n: map[sop.UUID]*Node[int, string]{}}
	si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: fakeIAT[int, string]{}}
	b, _ := New[int, string](store, &si, nil)
	// Seed duplicates
	if ok, _ := b.Add(nil, 7, "a"); !ok {
		t.Fatalf("add 7 a")
	}
	if ok, _ := b.Add(nil, 7, "b"); !ok {
		t.Fatalf("add 7 b")
	}
	if ok, _ := b.Find(nil, 7, true); !ok {
		t.Fatalf("find 7")
	}
	first, _ := b.GetCurrentItem(nil)
	if ok, _ := b.Next(nil); !ok {
		t.Fatalf("next dup")
	}
	second, _ := b.GetCurrentItem(nil)

	// Read-mode transaction, begun
	tx := &mockTx{begun: true, mode: sop.ForReading}
	w := NewBtreeWithTransaction(tx, b)

	// Cover Lock()
	if err := w.Lock(nil, false); err != nil {
		t.Fatalf("Lock: %v", err)
	}

	// FindWithID should select the requested duplicate
	if ok, err := w.FindWithID(nil, 7, second.ID); !ok || err != nil {
		t.Fatalf("FindWithID failed: %v", err)
	}
	got := w.GetCurrentKey()
	if got.ID != second.ID || got.ID == first.ID {
		t.Fatalf("FindWithID positioned wrong duplicate")
	}
}
