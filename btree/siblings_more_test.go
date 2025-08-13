package btree

import (
	"github.com/sharedcode/sop"
	"testing"
)

func TestGetLeftRightSibling_Edges(t *testing.T) {
	b, fnr := newTestBtree[string]()
	p := newNode[int, string](b.getSlotLength())
	p.newID(sop.NilUUID)
	p.Count = 2
	// Three children; test leftmost and rightmost sibling returns nil
	c0 := newNode[int, string](b.getSlotLength())
	c0.newID(p.ID)
	c1 := newNode[int, string](b.getSlotLength())
	c1.newID(p.ID)
	c2 := newNode[int, string](b.getSlotLength())
	c2.newID(p.ID)
	p.ChildrenIDs = []sop.UUID{c0.ID, c1.ID, c2.ID}
	fnr.Add(p)
	fnr.Add(c0)
	fnr.Add(c1)
	fnr.Add(c2)

	if ls, err := c0.getLeftSibling(nil, b); err != nil || ls != nil {
		t.Fatalf("leftmost should have no left sibling")
	}
	if rs, err := c2.getRightSibling(nil, b); err != nil || rs != nil {
		t.Fatalf("rightmost should have no right sibling")
	}
}
