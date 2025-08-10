package btree

import (
    "testing"
    "github.com/sharedcode/sop"
)

func TestAddItem_DuplicateReturnsFalse(t *testing.T) {
    b, _ := newTestBtree[string]()
    // Prepare root
    root := newNode[int, string](b.getSlotLength())
    root.newID(sop.NilUUID)
    b.StoreInfo.RootNodeID = root.ID
    // First insert via AddItem
    v := "v"
    item1 := &Item[int, string]{Key: 10, Value: &v, ID: sop.NewUUID()}
    if ok, err := b.AddItem(nil, item1); err != nil || !ok { t.Fatalf("AddItem #1 err=%v ok=%v", err, ok) }
    // Duplicate key should return false
    item2 := &Item[int, string]{Key: 10, Value: &v, ID: sop.NewUUID()}
    if ok, err := b.AddItem(nil, item2); err != nil || ok { t.Fatalf("AddItem duplicate should be false,nil; got ok=%v err=%v", ok, err) }
}

func TestGetCurrentKey_NoSelection(t *testing.T) {
    b, _ := newTestBtree[string]()
    k := b.GetCurrentKey()
    if k.ID != sop.NilUUID || k.Key != 0 { t.Fatalf("expected zero key and NilUUID when no selection") }
}

func TestUpdateCurrentNodeItem_Guards(t *testing.T) {
    b, fnr := newTestBtree[string]()
    // Nil selection
    b.setCurrentItemID(sop.NilUUID, 0)
    if ok, err := b.UpdateCurrentNodeItem(nil, &Item[int,string]{}); err != nil || ok { t.Fatalf("expected false,nil on nil selection") }
    // Selection points to nil slot
    root := newNode[int, string](b.getSlotLength())
    root.newID(sop.NilUUID)
    fnr.Add(root)
    b.StoreInfo.RootNodeID = root.ID
    b.StoreInfo.Count = 1
    b.setCurrentItemID(root.ID, 0)
    if ok, err := b.UpdateCurrentNodeItem(nil, &Item[int,string]{}); err != nil || ok { t.Fatalf("expected false,nil on nil slot selection") }
}

func TestUpdateCurrentItem_Guards(t *testing.T) {
    b, fnr := newTestBtree[string]()
    // Nil selection
    if ok, err := b.UpdateCurrentItem(nil, "x"); err != nil || ok { t.Fatalf("expected false,nil on nil selection") }
    // Selection points to nil slot
    root := newNode[int, string](b.getSlotLength())
    root.newID(sop.NilUUID)
    fnr.Add(root)
    b.StoreInfo.RootNodeID = root.ID
    b.StoreInfo.Count = 1
    b.setCurrentItemID(root.ID, 0)
    if ok, err := b.UpdateCurrentItem(nil, "y"); err != nil || ok { t.Fatalf("expected false,nil on nil slot selection") }
}

func TestFirstLast_EmptyReturnsFalse(t *testing.T) {
    b, _ := newTestBtree[string]()
    b.StoreInfo.Count = 0
    if ok, err := b.First(nil); err != nil || ok { t.Fatalf("First on empty should be false,nil") }
    if ok, err := b.Last(nil); err != nil || ok { t.Fatalf("Last on empty should be false,nil") }
}

// Update/Remove not-found paths should return false without error.
func TestAPI_Update_Remove_NotFound(t *testing.T) {
    b, _ := newTestBtree[string]()
    if ok, err := b.Update(nil, 42, "x"); err != nil || ok {
        t.Fatalf("Update not-found should be false,nil, got ok=%v err=%v", ok, err)
    }
    if ok, err := b.Remove(nil, 42); err != nil || ok {
        t.Fatalf("Remove not-found should be false,nil, got ok=%v err=%v", ok, err)
    }
}
