package btree

import (
    "context"
    "testing"
    "github.com/sharedcode/sop"
)

// Force promoteSingleChildAsParentChild to fail when parent can't be fetched (nil parent).
func TestPromoteSingleChildAsParentChild_Error_NoParent(t *testing.T) {
    b, fnr := newTestBtree[string]()
    // Node claims a non-nil parent ID but repo lacks that parent
    n := newNode[int,string](b.getSlotLength())
    n.ID = sop.NewUUID()
    n.ParentID = sop.NewUUID()
    // Single child setup
    n.ChildrenIDs = make([]sop.UUID, 1)
    child := newNode[int,string](b.getSlotLength())
    child.newID(n.ID)
    n.ChildrenIDs[0] = child.ID
    fnr.Add(n); fnr.Add(child)

    if ok, err := n.promoteSingleChildAsParentChild(context.TODO(), b); ok || err == nil {
        t.Fatalf("expected error when parent is missing in repo, got ok=%v err=%v", ok, err)
    }
}
