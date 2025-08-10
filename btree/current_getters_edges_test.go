package btree

import (
    "testing"
)

func TestGetCurrentItem_NoSelection(t *testing.T) {
    b, _ := newTestBtree[string]()
    b.setCurrentItemID(b.StoreInfo.RootNodeID, 0) // root not added yet -> nil get
    // Clear selection explicitly
    b.setCurrentItemID(b.StoreInfo.RootNodeID, 0)
    // emulate nil selection
    b.setCurrentItemID(b.StoreInfo.RootNodeID, 0)
}
