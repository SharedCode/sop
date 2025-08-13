package btree

import (
	"testing"

	"github.com/sharedcode/sop"
)

// Cover getCurrentNode and saveNode add/update branches.
func TestGetCurrentNode_And_SaveNode_AddUpdate(t *testing.T) {
	b, fnr := newTestBtree[string]()

	// Create a node without ID and save -> Add branch
	n := newNode[int, string](b.getSlotLength())
	if !n.ID.IsNil() {
		t.Fatalf("new node should have nil ID")
	}
	b.saveNode(n)
	if n.ID.IsNil() {
		t.Fatalf("saveNode should assign ID on Add branch")
	}
	if fnr.n[n.ID] == nil {
		t.Fatalf("node not added to repo")
	}

	// Update branch: modify and save again
	n.Count = 1
	b.saveNode(n)
	if got := fnr.n[n.ID]; got == nil || got.Count != 1 {
		t.Fatalf("update branch did not persist changes")
	}

	// getCurrentNode: set selection and fetch
	b.StoreInfo.RootNodeID = n.ID
	b.setCurrentItemID(n.ID, 0)
	gn, err := b.getCurrentNode(nil)
	if err != nil || gn == nil || gn.ID != n.ID {
		t.Fatalf("getCurrentNode failed: %v", err)
	}
}

// Cover Node getters and version set/get.
func TestNode_Getters_Version(t *testing.T) {
	n := newNode[int, string](4)
	if !n.GetID().IsNil() {
		t.Fatalf("GetID should be nil before newID")
	}
	n.newID(sop.NilUUID)
	if n.GetID().IsNil() {
		t.Fatalf("newID did not assign ID")
	}
	if n.GetVersion() != 0 {
		t.Fatalf("default version should be 0")
	}
	n.SetVersion(2)
	if n.GetVersion() != 2 {
		t.Fatalf("SetVersion did not apply")
	}
}

// Cover moveToLast when right-most child is nil (stop early) and when it descends to a child.
func TestMoveToLast_Paths(t *testing.T) {
	store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: true})
	fnr := &fakeNR[int, string]{n: map[sop.UUID]*Node[int, string]{}}
	si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: fakeIAT[int, string]{}}
	b, _ := New[int, string](store, &si, nil)

	// Case 1: parent has children slice, but last child is nil -> stop at parent last slot
	p := newNode[int, string](b.getSlotLength())
	p.newID(sop.NilUUID)
	v := "v"
	vv := v
	p.Slots[0] = &Item[int, string]{Key: 1, Value: &vv, ID: sop.NewUUID()}
	p.Slots[1] = &Item[int, string]{Key: 2, Value: &vv, ID: sop.NewUUID()}
	p.Count = 2
	p.ChildrenIDs = make([]sop.UUID, 3)
	p.ChildrenIDs[0] = sop.NewUUID()
	p.ChildrenIDs[1] = sop.NewUUID()
	p.ChildrenIDs[2] = sop.NilUUID // right-most child nil
	fnr.Add(p)
	b.StoreInfo.RootNodeID = p.ID
	ok, err := p.moveToLast(nil, b)
	if err != nil || !ok {
		t.Fatalf("moveToLast #1: %v", err)
	}
	if it, _ := b.GetCurrentItem(nil); it.Key != 2 {
		t.Fatalf("expected to land on parent last item 2")
	}

	// Case 2: right-most child exists with two items -> descend and select child's last
	c := newNode[int, string](b.getSlotLength())
	c.newID(p.ID)
	c.Slots[0] = &Item[int, string]{Key: 20, Value: &vv, ID: sop.NewUUID()}
	c.Slots[1] = &Item[int, string]{Key: 30, Value: &vv, ID: sop.NewUUID()}
	c.Count = 2
	fnr.Add(c)
	p.ChildrenIDs[2] = c.ID
	ok, err = p.moveToLast(nil, b)
	if err != nil || !ok {
		t.Fatalf("moveToLast #2: %v", err)
	}
	if it, _ := b.GetCurrentItem(nil); it.Key != 30 {
		t.Fatalf("expected to land on child last item 30")
	}
}

// Cover fixVacatedSlot leaf compaction and root single-item removal branches.
func TestFixVacatedSlot_LeafAndRoot(t *testing.T) {
	b, fnr := newTestBtree[string]()

	// Leaf compaction: non-root node with 3 items, delete middle
	leaf := newNode[int, string](b.getSlotLength())
	leaf.newID(sop.NewUUID())
	v := "x"
	vv := v
	leaf.Slots[0] = &Item[int, string]{Key: 1, Value: &vv, ID: sop.NewUUID()}
	leaf.Slots[1] = &Item[int, string]{Key: 2, Value: &vv, ID: sop.NewUUID()}
	leaf.Slots[2] = &Item[int, string]{Key: 3, Value: &vv, ID: sop.NewUUID()}
	leaf.Count = 3
	fnr.Add(leaf)
	b.setCurrentItemID(leaf.ID, 1)
	if err := leaf.fixVacatedSlot(nil, b); err != nil {
		t.Fatalf("fixVacatedSlot leaf: %v", err)
	}
	if leaf.Count != 2 || leaf.Slots[0].Key != 1 || leaf.Slots[1] == nil || leaf.Slots[1].Key != 3 {
		t.Fatalf("leaf compaction did not shift/trim correctly")
	}

	// Root single-item removal: root with one item should clear and set current to nil
	root := newNode[int, string](b.getSlotLength())
	root.newID(sop.NilUUID)
	b.StoreInfo.RootNodeID = root.ID
	root.Slots[0] = &Item[int, string]{Key: 10, Value: &vv, ID: sop.NewUUID()}
	root.Count = 1
	fnr.Add(root)
	b.setCurrentItemID(root.ID, 0)
	if err := root.fixVacatedSlot(nil, b); err != nil {
		t.Fatalf("fixVacatedSlot root: %v", err)
	}
	if root.Count != 0 || root.Slots[0] != nil {
		t.Fatalf("root item not cleared")
	}
}

// Cover unlinkNodeWithNilChild wrapper around promoteSingleChildAsParentChild.
func TestUnlinkNodeWithNilChild(t *testing.T) {
	b, fnr := newTestBtree[string]()
	parent := newNode[int, string](b.getSlotLength())
	parent.newID(sop.NilUUID)
	parent.ChildrenIDs = make([]sop.UUID, b.getSlotLength()+1)
	fnr.Add(parent)
	b.StoreInfo.RootNodeID = parent.ID

	node := newNode[int, string](b.getSlotLength())
	node.newID(parent.ID)
	child := newNode[int, string](b.getSlotLength())
	child.newID(node.ID)
	node.ChildrenIDs = make([]sop.UUID, 1)
	node.ChildrenIDs[0] = child.ID // has non-nil child
	parent.ChildrenIDs[0] = node.ID
	fnr.Add(node)
	fnr.Add(child)

	if ok, err := node.unlinkNodeWithNilChild(nil, b); err != nil || !ok {
		t.Fatalf("unlinkNodeWithNilChild: %v", err)
	}
	if parent.ChildrenIDs[0] != child.ID {
		t.Fatalf("parent not rewired to child")
	}
	if child.ParentID != parent.ID {
		t.Fatalf("child parent not updated")
	}
	if _, exists := fnr.n[node.ID]; exists {
		t.Fatalf("intermediate node should be removed from repo")
	}
}
