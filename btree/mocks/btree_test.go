package mocks

import "testing"
import "../../btree"
//import "../../transaction"

func TestBtreeBasic(t *testing.T){
	var store = btree.NewStoreDefaultSerializer("fooBar", 10, false)
	var repo = NewNodeRepository()
	var tree = btree.NewBtree(store, repo, nil)
	tree.Add("foo", "bar")
}

func TestBtreeTransaction(t *testing.T){
	var trans = NewTransaction()
	trans.Begin()
	var store = btree.NewStoreDefaultSerializer("fooBar", 10, false)
	var repo = NewNodeRepository()
	var tree = btree.NewBtree(store, repo, trans)
	tree.Add("foo", "bar")
	trans.Commit()
}
