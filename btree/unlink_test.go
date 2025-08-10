package btree

import "github.com/sharedcode/sop"
import "testing"

// Cover unlinkNodeWithNilChild -> promoteSingleChildAsParentChild path.
func TestUnlinkNodeWithNilChild_Promote(t *testing.T) {
    b, fnr := newTestBtree[string]()
    // Build parent -> node(with one child) structure
    p := newNode[int, string](b.getSlotLength())
    p.newID(sop.NilUUID)
    p.ChildrenIDs = make([]sop.UUID, 2)

    n := newNode[int, string](b.getSlotLength())
    n.newID(p.ID)
    n.ParentID = p.ID
    n.ChildrenIDs = make([]sop.UUID, 1)

    c := newNode[int, string](b.getSlotLength())
    c.newID(n.ID)
    c.ParentID = n.ID
    n.ChildrenIDs[0] = c.ID

    // Wire p -> n
    p.ChildrenIDs[0] = n.ID
    fnr.Add(p)
    fnr.Add(n)
    fnr.Add(c)
    b.StoreInfo.RootNodeID = p.ID

    if ok, err := n.unlinkNodeWithNilChild(nil, b); !ok || err != nil {
        t.Fatalf("unlinkNodeWithNilChild failed: %v", err)
    }
    // After promote, p should now directly reference c and n should be removed.
    if p.ChildrenIDs[0] != c.ID {
        t.Fatalf("parent did not adopt child")
    }
    if _, exists := fnr.n[n.ID]; exists {
        t.Fatalf("intermediate node not removed")
    }
    if gotP := c.ParentID; gotP != p.ID {
        t.Fatalf("child parent not rewired")
    }
}
