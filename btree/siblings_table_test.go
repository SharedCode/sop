package btree

import (
	"testing"

	"github.com/sharedcode/sop"
)

func TestSiblings_Table(t *testing.T) {
	b, fnr := newTestBtree[string]()
	// Build parent with three children
	p := newNode[int, string](b.getSlotLength())
	p.newID(sop.NilUUID)
	b.StoreInfo.RootNodeID = p.ID
	p.Slots[0] = &Item[int, string]{Key: 10, Value: &[]string{"a"}[0], ID: sop.NewUUID()}
	p.Slots[1] = &Item[int, string]{Key: 20, Value: &[]string{"b"}[0], ID: sop.NewUUID()}
	p.Count = 2
	c0 := newNode[int, string](b.getSlotLength())
	c0.newID(p.ID)
	c1 := newNode[int, string](b.getSlotLength())
	c1.newID(p.ID)
	c2 := newNode[int, string](b.getSlotLength())
	c2.newID(p.ID)
	p.ChildrenIDs = make([]sop.UUID, 3)
	p.ChildrenIDs[0] = c0.ID
	p.ChildrenIDs[1] = c1.ID
	p.ChildrenIDs[2] = c2.ID
	fnr.Add(p)
	fnr.Add(c0)
	fnr.Add(c1)
	fnr.Add(c2)

	tests := []struct {
		name    string
		child   *Node[int, string]
		left    bool
		wantNil bool
	}{
		{"leftmost has no left", c0, true, true},
		{"middle has left", c1, true, false},
		{"rightmost has no right", c2, false, true},
		{"middle has right", c1, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.left {
				n, err := tc.child.getLeftSibling(nil, b)
				_ = err
				if (n == nil) != tc.wantNil {
					t.Fatalf("left sibling nil=%v want %v", n == nil, tc.wantNil)
				}
			} else {
				n, err := tc.child.getRightSibling(nil, b)
				_ = err
				if (n == nil) != tc.wantNil {
					t.Fatalf("right sibling nil=%v want %v", n == nil, tc.wantNil)
				}
			}
		})
	}
}
