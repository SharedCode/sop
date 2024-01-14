package in_memory

import "github.com/SharedCode/sop/btree"

type mt[TK btree.Comparable, TV any] struct{}

func newDumbItemActionTracker[TK btree.Comparable, TV any]() btree.ItemActionTracker[TK, TV] {
	return &mt[TK, TV]{}
}

// in-memory SOP does not track item actions, thus, do nothing for its ItemActionTracker.

func (iat mt[TK, TV])Add(item *btree.Item[TK, TV]) {}
func (iat mt[TK, TV])Get(item *btree.Item[TK, TV]) {}
func (iat mt[TK, TV])Update(item *btree.Item[TK, TV]) {}
func (iat mt[TK, TV])Remove(item *btree.Item[TK, TV]){}
