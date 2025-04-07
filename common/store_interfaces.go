package common

import (
	"github.com/SharedCode/sop/btree"
)

// StoreInterface contains different repositories needed/used by B-Tree to manage/access its data/objects.
type StoreInterface[TK btree.Comparable, TV any] struct {
	btree.StoreInterface[TK, TV]
	// Non-generics node repository, used in transaction commit to process modified Nodes.
	backendNodeRepository *nodeRepository
}
