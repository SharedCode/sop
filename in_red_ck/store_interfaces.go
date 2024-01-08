package in_red_ck

import (
	"github.com/SharedCode/sop/btree"
)

// StoreInterface contains different repositories needed/used by B-Tree to manage/access its data/objects.
type StoreInterface[TK btree.Comparable, TV any] struct {
	btree.StoreInterface[TK, TV]
	// Non-generics item action tracker, used in transaction commit to process modified Items.
	backendItemActionTracker *itemActionTracker
	// Non-generics node repository, used in transaction commit to process modified Nodes.
	backendNodeRepository *nodeRepository
}
