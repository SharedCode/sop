package btree

import (
	"context"
	"testing"
)

func TestUpdateChildrenParent_NoChildren_NoOp(t *testing.T) {
	b, _ := newTestBtree[string]()
	n := newNode[int, string](b.getSlotLength())
	n.newID(b.StoreInfo.RootNodeID)
	// no ChildrenIDs set -> hasChildren()==false
	if err := n.updateChildrenParent(context.TODO(), b); err != nil {
		t.Fatalf("expected no error for leaf node, got %v", err)
	}
}
