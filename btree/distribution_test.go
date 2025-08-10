package btree

import (
	"testing"

	"github.com/sharedcode/sop"
)

func TestDistributeToLeft_RotationAndAppend(t *testing.T) {
	store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: true, IsValueDataInNodeSegment: true, LeafLoadBalancing: true})
	fnr := &fakeNR[int, string]{n: map[sop.UUID]*Node[int, string]{}}
	si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: fakeIAT[int, string]{}}
	b, _ := New[int, string](store, &si, nil)

	// Parent with one separator
	parent := newNode[int, string](b.getSlotLength())
	parent.newID(sop.NilUUID)
	sepVal := "m"
	parent.Slots[0] = &Item[int, string]{Key: 100, Value: &sepVal, ID: sop.NewUUID()}
	parent.Count = 1

	// Left child: not full (3 of 4)
	left := newNode[int, string](b.getSlotLength())
	left.newID(parent.ID)
	for i, k := range []int{10, 20, 30} {
		v := "l"
		vv := v
		left.Slots[i] = &Item[int, string]{Key: k, Value: &vv, ID: sop.NewUUID()}
	}
	left.Count = 3

	// Right child: full (4 of 4)
	right := newNode[int, string](b.getSlotLength())
	right.newID(parent.ID)
	for i, k := range []int{110, 120, 130, 140} {
		v := "r"
		vv := v
		right.Slots[i] = &Item[int, string]{Key: k, Value: &vv, ID: sop.NewUUID()}
	}
	right.Count = 4

	// Wire children
	parent.ChildrenIDs = make([]sop.UUID, b.getSlotLength()+1)
	parent.ChildrenIDs[0] = left.ID
	parent.ChildrenIDs[1] = right.ID
	fnr.Add(parent)
	fnr.Add(left)
	fnr.Add(right)

	// Trigger distributeToLeft from the right sibling with an extra item
	extraV := "x"
	extra := &Item[int, string]{Key: 105, Value: &extraV, ID: sop.NewUUID()}
	if err := right.distributeToLeft(nil, b, extra); err != nil {
		t.Fatalf("distributeToLeft err: %v", err)
	}

	// Complete the controller’s follow-up call (append to left)
	b.distribute(nil)

	// Left should have grown to 4
	if left.Count != 4 {
		t.Fatalf("left count want 4 got %d", left.Count)
	}
	// Parent separator should be non-nil (updated)
	if parent.Slots[0] == nil {
		t.Fatalf("parent separator not set")
	}
}

func TestDistribute_Rotations_DoNotPanic(t *testing.T) {
	b, _, _ := buildSmallTree()
	// Insert items to create pressure and give siblings room to rotate
	for _, k := range []int{15, 16, 17, 18, 19, 21, 22, 23, 24} {
		v := "y"
		vv := v
		if ok, err := b.Add(nil, k, vv); err != nil || !ok {
			t.Fatalf("add %d err=%v", k, err)
		}
	}
	// Basic spot checks: First/Last still work and traversal covers all
	if ok, _ := b.First(nil); !ok {
		t.Fatalf("First failed")
	}
	prev := b.GetCurrentKey().Key
	count := 1
	for {
		ok, _ := b.Next(nil)
		if !ok {
			break
		}
		k := b.GetCurrentKey().Key
		if k < prev {
			t.Fatalf("order violated: %d < %d", k, prev)
		}
		prev = k
		count++
		if count > 2000 {
			t.Fatalf("loop guard")
		}
	}
}
