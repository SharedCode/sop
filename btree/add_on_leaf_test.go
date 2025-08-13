package btree

import (
	"github.com/sharedcode/sop"
	"testing"
)

// helper: fill a node with sequential keys starting at base
func fillFullNode(b *Btree[int, string], n *Node[int, string], base int) {
	v := "v"
	for i := 0; i < b.getSlotLength(); i++ {
		vv := v
		n.Slots[i] = &Item[int, string]{Key: base + i, Value: &vv, ID: sop.NewUUID()}
	}
	n.Count = b.getSlotLength()
}

func TestAddOnLeaf_NotFull_InsertsAndShifts(t *testing.T) {
	b, fnr := newTestBtree[string]()
	leaf := newNode[int, string](b.getSlotLength())
	leaf.newID(sop.NewUUID())
	// Preload two items [10, 30] leaving a gap at index 1
	v := "v"
	vv := v
	leaf.Slots[0] = &Item[int, string]{Key: 10, Value: &vv, ID: sop.NewUUID()}
	leaf.Slots[1] = &Item[int, string]{Key: 30, Value: &vv, ID: sop.NewUUID()}
	leaf.Count = 2
	fnr.Add(leaf)
	// Insert 20 at index 1
	item := &Item[int, string]{Key: 20, Value: &vv, ID: sop.NewUUID()}
	if err := leaf.addOnLeaf(nil, b, item, 1); err != nil {
		t.Fatalf("addOnLeaf err: %v", err)
	}
	if leaf.Count != 3 || leaf.Slots[1] == nil || leaf.Slots[1].Key != 20 {
		t.Fatalf("expected insert+shift; got count=%d mid=%v", leaf.Count, leaf.Slots[1])
	}
}

func TestAddOnLeaf_Full_VacancyLeft_SetsDistributeLeft(t *testing.T) {
	b, fnr := newTestBtree[string]()
	b.StoreInfo.LeafLoadBalancing = true
	parent := newNode[int, string](b.getSlotLength())
	parent.newID(sop.NilUUID)
	parent.Count = 2

	left := newNode[int, string](b.getSlotLength())
	left.newID(parent.ID)
	// left has room
	v := "v"
	vv := v
	left.Slots[0] = &Item[int, string]{Key: 1, Value: &vv, ID: sop.NewUUID()}
	left.Count = 1

	cur := newNode[int, string](b.getSlotLength())
	cur.newID(parent.ID)
	fillFullNode(b, cur, 10)

	right := newNode[int, string](b.getSlotLength())
	right.newID(parent.ID)
	fillFullNode(b, right, 100)

	parent.ChildrenIDs = make([]sop.UUID, 3)
	parent.ChildrenIDs[0] = left.ID
	parent.ChildrenIDs[1] = cur.ID
	parent.ChildrenIDs[2] = right.ID
	fnr.Add(parent)
	fnr.Add(left)
	fnr.Add(cur)
	fnr.Add(right)

	item := &Item[int, string]{Key: 15, Value: &vv, ID: sop.NewUUID()}
	if err := cur.addOnLeaf(nil, b, item, 1); err != nil {
		t.Fatalf("addOnLeaf err: %v", err)
	}
	if b.distributeAction.sourceNode != cur || !b.distributeAction.distributeToLeft || b.distributeAction.item == nil {
		t.Fatalf("expected distribute left to be set")
	}
}

func TestAddOnLeaf_Full_VacancyRight_SetsDistributeRight(t *testing.T) {
	b, fnr := newTestBtree[string]()
	b.StoreInfo.LeafLoadBalancing = true
	parent := newNode[int, string](b.getSlotLength())
	parent.newID(sop.NilUUID)
	parent.Count = 2

	left := newNode[int, string](b.getSlotLength())
	left.newID(parent.ID)
	fillFullNode(b, left, 1)

	cur := newNode[int, string](b.getSlotLength())
	cur.newID(parent.ID)
	fillFullNode(b, cur, 10)

	right := newNode[int, string](b.getSlotLength())
	right.newID(parent.ID)
	// right has room
	v := "v"
	vv := v
	right.Slots[0] = &Item[int, string]{Key: 200, Value: &vv, ID: sop.NewUUID()}
	right.Count = 1

	parent.ChildrenIDs = make([]sop.UUID, 3)
	parent.ChildrenIDs[0] = left.ID
	parent.ChildrenIDs[1] = cur.ID
	parent.ChildrenIDs[2] = right.ID
	fnr.Add(parent)
	fnr.Add(left)
	fnr.Add(cur)
	fnr.Add(right)

	item := &Item[int, string]{Key: 15, Value: &vv, ID: sop.NewUUID()}
	if err := cur.addOnLeaf(nil, b, item, 1); err != nil {
		t.Fatalf("addOnLeaf err: %v", err)
	}
	if b.distributeAction.sourceNode != cur || b.distributeAction.distributeToLeft || b.distributeAction.item == nil {
		t.Fatalf("expected distribute right to be set")
	}
}

func TestAddOnLeaf_Full_Unbalanced_SplitsIntoChildren(t *testing.T) {
	b, fnr := newTestBtree[string]()
	b.StoreInfo.LeafLoadBalancing = true
	parent := newNode[int, string](b.getSlotLength())
	parent.newID(sop.NilUUID)
	parent.Count = 1

	cur := newNode[int, string](b.getSlotLength())
	cur.newID(parent.ID)
	fillFullNode(b, cur, 10)

	// Make a right sibling that is an inner node (ChildrenIDs non-nil) to mark branch unbalanced via right-side scan.
	right := newNode[int, string](b.getSlotLength())
	right.newID(parent.ID)
	right.ChildrenIDs = make([]sop.UUID, 1)
	// avoid nodeHasNilChild shortcut; mark as inner node without nil child
	right.ChildrenIDs[0] = sop.NewUUID()

	parent.ChildrenIDs = make([]sop.UUID, 2)
	parent.ChildrenIDs[0] = cur.ID
	parent.ChildrenIDs[1] = right.ID
	fnr.Add(parent)
	fnr.Add(right)
	fnr.Add(cur)

	v := "v"
	vv := v
	item := &Item[int, string]{Key: 15, Value: &vv, ID: sop.NewUUID()}
	if err := cur.addOnLeaf(nil, b, item, 1); err != nil {
		t.Fatalf("addOnLeaf err: %v", err)
	}

	// cur should now have children (split into two); do not assert exact count due to implementation detail
	if cur.ChildrenIDs == nil || len(cur.ChildrenIDs) == 0 || cur.ChildrenIDs[0] == sop.NilUUID || cur.ChildrenIDs[1] == sop.NilUUID {
		t.Fatalf("expected split into children; children=%v", cur.ChildrenIDs)
	}
}

func TestAddOnLeaf_Full_AllFull_PromoteRequested(t *testing.T) {
	b, fnr := newTestBtree[string]()
	b.StoreInfo.LeafLoadBalancing = true
	parent := newNode[int, string](b.getSlotLength())
	parent.newID(sop.NilUUID)
	parent.Count = 2

	left := newNode[int, string](b.getSlotLength())
	left.newID(parent.ID)
	fillFullNode(b, left, 1)
	cur := newNode[int, string](b.getSlotLength())
	cur.newID(parent.ID)
	fillFullNode(b, cur, 10)
	right := newNode[int, string](b.getSlotLength())
	right.newID(parent.ID)
	fillFullNode(b, right, 100)

	parent.ChildrenIDs = make([]sop.UUID, 3)
	parent.ChildrenIDs[0] = left.ID
	parent.ChildrenIDs[1] = cur.ID
	parent.ChildrenIDs[2] = right.ID
	fnr.Add(parent)
	fnr.Add(left)
	fnr.Add(cur)
	fnr.Add(right)

	v := "v"
	vv := v
	item := &Item[int, string]{Key: 15, Value: &vv, ID: sop.NewUUID()}
	if err := cur.addOnLeaf(nil, b, item, 1); err != nil {
		t.Fatalf("addOnLeaf err: %v", err)
	}

	if b.promoteAction.targetNode == nil || b.promoteAction.targetNode.ID != parent.ID {
		t.Fatalf("expected promote action to target parent")
	}
}

func TestAddOnLeaf_Full_RootBreaksIntoChildren(t *testing.T) {
	b, fnr := newTestBtree[string]()
	root := newNode[int, string](b.getSlotLength())
	root.newID(sop.NilUUID)
	b.StoreInfo.RootNodeID = root.ID
	fillFullNode(b, root, 10)
	fnr.Add(root)

	v := "v"
	vv := v
	item := &Item[int, string]{Key: 15, Value: &vv, ID: sop.NewUUID()}
	if err := root.addOnLeaf(nil, b, item, 1); err != nil {
		t.Fatalf("addOnLeaf err: %v", err)
	}

	if root.ChildrenIDs == nil || root.Count != 1 {
		t.Fatalf("expected root to split into two children; count=%d children=%v", root.Count, root.ChildrenIDs)
	}
}
