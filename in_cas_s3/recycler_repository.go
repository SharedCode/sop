package in_aws

import "github.com/SharedCode/sop/btree"

// RecyclerRepository provides capability to recycle storage areas for storing data such as Node, etc...
// There are backends where this is not needed at all, e.g. Cassandra backend will not need this.
type RecyclerRepository interface {
	Get(itemCount int, objectType int) []Recyclable
	Add(recyclables []Recyclable) error
	Remove(items []Recyclable) error
}

type Recyclable struct {
	ObjectType int
	ObjectId   btree.UUID
	LockDate   int64
	IsDeleted  bool
}

func newRecycler() RecyclerRepository {
	return Recyclable{}
}

func (Recyclable) Get(batch int, objectType int) []Recyclable {
	return nil
}
func (Recyclable) Add(recyclable []Recyclable) error {
	return nil
}

func (Recyclable) Remove(items []Recyclable) error {
	return nil
}
