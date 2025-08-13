package btree

import (
	"context"
	"fmt"
	"github.com/sharedcode/sop"
	"testing"
)

// fakeNRSelectiveErr wraps an in-memory node map and returns an error for IDs in errs.
type fakeNRSelectiveErr[TK Ordered, TV any] struct {
	n    map[sop.UUID]*Node[TK, TV]
	errs map[sop.UUID]bool
}

func (f *fakeNRSelectiveErr[TK, TV]) Add(node *Node[TK, TV])    { f.n[node.ID] = node }
func (f *fakeNRSelectiveErr[TK, TV]) Update(node *Node[TK, TV]) { f.n[node.ID] = node }
func (f *fakeNRSelectiveErr[TK, TV]) Remove(id sop.UUID)        { delete(f.n, id) }
func (f *fakeNRSelectiveErr[TK, TV]) Fetched(id sop.UUID)       {}
func (f *fakeNRSelectiveErr[TK, TV]) Get(_ context.Context, id sop.UUID) (*Node[TK, TV], error) {
	if f.errs[id] {
		return nil, fmt.Errorf("forced get error for %v", id)
	}
	return f.n[id], nil
}

func TestDistributeToRight_ReturnsParentError(t *testing.T) {
	store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: true})
	fnr := &fakeNRSelectiveErr[int, string]{n: map[sop.UUID]*Node[int, string]{}, errs: map[sop.UUID]bool{}}
	si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: fakeIAT[int, string]{}}
	b, _ := New[int, string](store, &si, nil)

	// Build a full node with a parent id that will error when fetched.
	parentID := sop.NewUUID()
	fnr.errs[parentID] = true

	node := newNode[int, string](b.getSlotLength())
	node.ID = sop.NewUUID()
	node.ParentID = parentID
	// Make node full with valid last slot for access during rotation path.
	v := "v"
	vv := v
	for i := 0; i < b.getSlotLength(); i++ {
		node.Slots[i] = &Item[int, string]{Key: 100 + i, Value: &vv, ID: sop.NewUUID()}
	}
	node.Count = b.getSlotLength()

	// Call distributeToRight and expect the parent fetch error to bubble up.
	err := node.distributeToRight(nil, b, &Item[int, string]{Key: 999, Value: &vv, ID: sop.NewUUID()})
	if err == nil {
		t.Fatalf("expected error from getParent to be returned")
	}
}

func TestDistributeToRight_ReturnsRightSiblingError(t *testing.T) {
	store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: true})
	fnr := &fakeNRSelectiveErr[int, string]{n: map[sop.UUID]*Node[int, string]{}, errs: map[sop.UUID]bool{}}
	si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: fakeIAT[int, string]{}}
	b, _ := New[int, string](store, &si, nil)

	// Setup parent with two children: node (left) and right sibling.
	parent := newNode[int, string](b.getSlotLength())
	parent.newID(sop.NilUUID)
	parent.Count = 1

	node := newNode[int, string](b.getSlotLength())
	node.newID(parent.ID)
	// Make node full
	v := "v"
	vv := v
	for i := 0; i < b.getSlotLength(); i++ {
		node.Slots[i] = &Item[int, string]{Key: 100 + i, Value: &vv, ID: sop.NewUUID()}
	}
	node.Count = b.getSlotLength()

	right := newNode[int, string](b.getSlotLength())
	right.newID(parent.ID)

	// Wire parent children; node is left child at index 0, right sibling at index 1.
	parent.ChildrenIDs = make([]sop.UUID, 2)
	parent.ChildrenIDs[0] = node.ID
	parent.ChildrenIDs[1] = right.ID

	// Add parent and node to repo; force error only when fetching the right sibling ID.
	fnr.Add(parent)
	fnr.Add(node)
	fnr.errs[right.ID] = true

	// Trigger distributeToRight; expect the right sibling fetch error to be returned.
	err := node.distributeToRight(nil, b, &Item[int, string]{Key: 999, Value: &vv, ID: sop.NewUUID()})
	if err == nil {
		t.Fatalf("expected error from getRightSibling to be returned")
	}
}

func TestDistribute_Controller_ReturnsError(t *testing.T) {
	store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: true})
	fnr := &fakeNRSelectiveErr[int, string]{n: map[sop.UUID]*Node[int, string]{}, errs: map[sop.UUID]bool{}}
	si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: fakeIAT[int, string]{}}
	b, _ := New[int, string](store, &si, nil)

	// Parent with two children
	parent := newNode[int, string](b.getSlotLength())
	parent.newID(sop.NilUUID)
	parent.Count = 1

	left := newNode[int, string](b.getSlotLength())
	left.newID(parent.ID)
	// Fill left so rotation path is used
	v := "v"
	vv := v
	for i := 0; i < b.getSlotLength(); i++ {
		left.Slots[i] = &Item[int, string]{Key: 10 + i, Value: &vv, ID: sop.NewUUID()}
	}
	left.Count = b.getSlotLength()

	right := newNode[int, string](b.getSlotLength())
	right.newID(parent.ID)

	parent.ChildrenIDs = make([]sop.UUID, 2)
	parent.ChildrenIDs[0] = left.ID
	parent.ChildrenIDs[1] = right.ID

	fnr.Add(parent)
	fnr.Add(left)
	fnr.Add(right)

	// Seed controller to distribute to the right from the left node
	b.distributeAction.sourceNode = left
	b.distributeAction.item = &Item[int, string]{Key: 999, Value: &vv, ID: sop.NewUUID()}
	b.distributeAction.distributeToLeft = false

	// Force an error only when fetching the right sibling during controller execution
	fnr.errs[right.ID] = true

	if err := b.distribute(nil); err == nil {
		t.Fatalf("expected controller distribute to return error from distributeToRight")
	}
}
