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
	
	// assign the User or Application custom transaction.
	trans.UserTransaction = &UserTransaction{}

	trans.Begin()
	var store = btree.NewStoreDefaultSerializer("fooBar", 10, false)
	var repo = NewNodeRepository()
	var tree = btree.NewBtree(store, repo, trans)
	tree.Add("foo", "bar")

	var store2 = btree.NewStoreDefaultSerializer("fooBar2", 11, false)
	var repo2 = NewNodeRepository()
	var tree2 = btree.NewBtree(store2, repo2, trans)
	tree2.Add("foo", "bar")

	trans.Commit()
}
