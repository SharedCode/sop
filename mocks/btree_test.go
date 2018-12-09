package mocks

import "testing"
import "../btree"
import "../../sop"
//import "../../transaction"

func TestBtreeBasic(t *testing.T){
	var store = btree.NewStoreDefaultSerializer("fooBar", 10, false)
	var tree = sop.NewBtree(store, nil)
	tree.Add("foo", "bar")
}

func TestBtreeTransaction(t *testing.T){
	var trans = sop.NewTransaction(sop.Cassandra)
	
	// assign the User or Application custom transaction.
	trans.UserTransaction = &UserTransaction{}

	trans.Begin()
	var store = btree.NewStoreDefaultSerializer("fooBar", 10, false)
	var tree = sop.NewBtree(store, trans)
	tree.Add("foo", "bar")

	var store2 = btree.NewStoreDefaultSerializer("fooBar2", 11, false)
	var tree2 = sop.NewBtree(store2, trans)
	tree2.Add("foo", "bar")

	trans.Commit()
}
