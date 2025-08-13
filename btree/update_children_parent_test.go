package btree

import (
	"github.com/sharedcode/sop"
	"testing"
)

func TestUpdateChildrenParent_WiresAllChildren(t *testing.T) {
	b, fnr := newTestBtree[string]()
	p := newNode[int, string](b.getSlotLength())
	p.newID(sop.NilUUID)
	p.ChildrenIDs = make([]sop.UUID, 3)
	c0 := newNode[int, string](b.getSlotLength())
	c0.newID(p.ID)
	c1 := newNode[int, string](b.getSlotLength())
	c1.newID(p.ID)
	c2 := newNode[int, string](b.getSlotLength())
	c2.newID(p.ID)
	fnr.Add(p)
	fnr.Add(c0)
	fnr.Add(c1)
	fnr.Add(c2)
	p.ChildrenIDs[0] = c0.ID
	p.ChildrenIDs[1] = c1.ID
	p.ChildrenIDs[2] = c2.ID
	if err := p.updateChildrenParent(nil, b); err != nil {
		t.Fatalf("updateChildrenParent err: %v", err)
	}
	if c0.ParentID != p.ID || c1.ParentID != p.ID || c2.ParentID != p.ID {
		t.Fatalf("children ParentID not rewired correctly")
	}
}
