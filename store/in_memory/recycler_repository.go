package in_memory

import "github.com/SharedCode/sop/btree"

// Recycler is not used in in-memory store, 'below are just stubs.
type Recyclable struct {
	Year  int
	Month int
	Day   int
	Hour  int
	btree.Recyclable
}

func newRecycler() btree.RecyclerRepository {
	return Recyclable{}
}

func (Recyclable) Get(batch int, objectType int) []btree.Recyclable {
	return nil
}
func (Recyclable) Add(recyclable []btree.Recyclable) error {
	return nil
}

func (Recyclable) Remove(items []btree.Recyclable) error {
	return nil
}
