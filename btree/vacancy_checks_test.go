package btree

import (
	"github.com/sharedcode/sop"
	"testing"
)

func TestIsThereVacantSlotInLeft_Cases(t *testing.T) {
	b, fnr := newTestBtree[string]()
	b.StoreInfo.LeafLoadBalancing = true
	// Node with ChildrenIDs having a nil child -> immediate true
	n := newNode[int, string](b.getSlotLength())
	n.newID(sop.NewUUID())
	n.ChildrenIDs = make([]sop.UUID, 2)
	n.ChildrenIDs[0] = sop.NilUUID
	fnr.Add(n)
	if ok, err := n.isThereVacantSlotInLeft(nil, b, new(bool)); err != nil || !ok {
		t.Fatalf("expected true due to nil child, err=%v", err)
	}

	// Unbalanced branch: ChildrenIDs non-nil without nil child triggers isUnBalanced=true and false
	n2 := newNode[int, string](b.getSlotLength())
	n2.newID(sop.NewUUID())
	n2.ChildrenIDs = []sop.UUID{sop.NewUUID()}
	fnr.Add(n2)
	unb := false
	if ok, err := n2.isThereVacantSlotInLeft(nil, b, &unb); err != nil || ok || !unb {
		t.Fatalf("expected unbalanced=false return, got ok=%v unb=%v err=%v", ok, unb, err)
	}

	// Not full leaf -> true
	n3 := newNode[int, string](b.getSlotLength())
	n3.newID(sop.NewUUID())
	// no children slice means leaf; Count < slotLength
	fnr.Add(n3)
	if ok, err := n3.isThereVacantSlotInLeft(nil, b, new(bool)); err != nil || !ok {
		t.Fatalf("expected true for not-full leaf, err=%v", err)
	}

	// Scan to left sibling: current full, left sibling not full
	parent := newNode[int, string](b.getSlotLength())
	parent.newID(sop.NilUUID)
	parent.Count = 2
	parent.ChildrenIDs = make([]sop.UUID, 3)
	left := newNode[int, string](b.getSlotLength())
	left.newID(parent.ID)
	// left is not full -> target
	full := newNode[int, string](b.getSlotLength())
	full.newID(parent.ID)
	full.Count = b.getSlotLength()
	// wire parent
	parent.ChildrenIDs[0] = left.ID
	parent.ChildrenIDs[1] = full.ID
	fnr.Add(parent)
	fnr.Add(left)
	fnr.Add(full)
	// pretend full is the current node
	if ok, err := full.isThereVacantSlotInLeft(nil, b, new(bool)); err != nil || !ok {
		t.Fatalf("expected true by scanning to left sibling, err=%v", err)
	}
}

func TestIsThereVacantSlotInRight_Cases(t *testing.T) {
	b, fnr := newTestBtree[string]()
	b.StoreInfo.LeafLoadBalancing = true
	// Node with ChildrenIDs having a nil child -> immediate true
	n := newNode[int, string](b.getSlotLength())
	n.newID(sop.NewUUID())
	n.ChildrenIDs = make([]sop.UUID, 2)
	n.ChildrenIDs[1] = sop.NilUUID
	fnr.Add(n)
	if ok, err := n.isThereVacantSlotInRight(nil, b, new(bool)); err != nil || !ok {
		t.Fatalf("expected true due to nil child, err=%v", err)
	}

	// Unbalanced branch
	n2 := newNode[int, string](b.getSlotLength())
	n2.newID(sop.NewUUID())
	n2.ChildrenIDs = []sop.UUID{sop.NewUUID()}
	fnr.Add(n2)
	unb := false
	if ok, err := n2.isThereVacantSlotInRight(nil, b, &unb); err != nil || ok || !unb {
		t.Fatalf("expected unbalanced=false return, got ok=%v unb=%v err=%v", ok, unb, err)
	}

	// Not full leaf -> true
	n3 := newNode[int, string](b.getSlotLength())
	n3.newID(sop.NewUUID())
	fnr.Add(n3)
	if ok, err := n3.isThereVacantSlotInRight(nil, b, new(bool)); err != nil || !ok {
		t.Fatalf("expected true for not-full leaf, err=%v", err)
	}

	// Scan to right sibling: current full, right sibling not full
	parent := newNode[int, string](b.getSlotLength())
	parent.newID(sop.NilUUID)
	parent.Count = 2
	parent.ChildrenIDs = make([]sop.UUID, 3)
	full := newNode[int, string](b.getSlotLength())
	full.newID(parent.ID)
	full.Count = b.getSlotLength()
	right := newNode[int, string](b.getSlotLength())
	right.newID(parent.ID)
	parent.ChildrenIDs[1] = full.ID
	parent.ChildrenIDs[2] = right.ID
	fnr.Add(parent)
	fnr.Add(full)
	fnr.Add(right)
	if ok, err := full.isThereVacantSlotInRight(nil, b, new(bool)); err != nil || !ok {
		t.Fatalf("expected true by scanning to right sibling, err=%v", err)
	}
}
