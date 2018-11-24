package mocks

import "testing"
import "../../btree"

func TestBtreeBasic(t *testing.T){
	var store = btree.NewStore("fooBar", 10, false)
	var repo = NewNodeRepository()
	var tree = btree.NewBtree(store, repo)
	tree.Add("foo", "bar")
}
