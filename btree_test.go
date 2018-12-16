package sop

import "testing"
import "./btree"
import "./store"
import "./mocks"

func TestBtreeBasic(t *testing.T){
	var store = btree.NewStoreDefaultSerializer("fooBar", 10, false)
	var tree, _ = NewBtree(store, nil)
	tree.Add("foo", "bar")
}

func TestBtreeTransaction(t *testing.T){
	var trans = NewTransaction(store.Cassandra)
	
	// assign the User or Application custom transaction.
	trans.UserTransaction = &mocks.UserTransaction{}

	trans.Begin()
	var store = btree.NewStoreDefaultSerializer("fooBar", 10, false)
	var tree, _ = NewBtree(store, trans)
	tree.Add("foo", "bar")

	var store2 = btree.NewStoreDefaultSerializer("fooBar2", 11, false)
	var tree2, _ = NewBtree(store2, trans)
	tree2.Add("foo", "bar")

	trans.Commit()
}
