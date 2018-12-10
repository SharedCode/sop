package sop

import "testing"
import "./btree"
import "./mocks"
//import "../../transaction"

func TestBtreeBasic(t *testing.T){
	var store = btree.NewStoreDefaultSerializer("fooBar", 10, false)
	var tree = NewBtree(store, nil)
	tree.Add("foo", "bar")
}

func TestBtreeTransaction(t *testing.T){
	var trans = NewTransaction(Cassandra)
	
	// assign the User or Application custom transaction.
	trans.UserTransaction = &mocks.UserTransaction{}

	trans.Begin()
	var store = btree.NewStoreDefaultSerializer("fooBar", 10, false)
	var tree = NewBtree(store, trans)
	tree.Add("foo", "bar")

	var store2 = btree.NewStoreDefaultSerializer("fooBar2", 11, false)
	var tree2 = NewBtree(store2, trans)
	tree2.Add("foo", "bar")

	trans.Commit()
}
