package sop

import "testing"
import "os"
import "github.com/SharedCode/sop.git/btree"
import "github.com/SharedCode/sop.git/store"
import "github.com/SharedCode/sop.git/mocks"

import log "github.com/sirupsen/logrus"

func TestBtreeBasic(t *testing.T){

	dir, err := os.Getwd()
	if err != nil {
	  log.Fatal(err)
	}
	var config, _ = LoadConfiguration(dir + "/config.json")
	var trans = NewTransaction(store.Cassandra)
	var store = btree.NewStoreDefaultSerializer("fooBar", 10, false)
	// assign the User or Application custom transaction.
	trans.UserTransaction = &mocks.UserTransaction{}
	trans.Begin()
	tree, err := NewBtree(store, trans, config)

	// works now!!! :)
	tree.Add("foo", "bar")
	tree.Add("foo2", "bar2")
}

func TestBtreeTransaction(t *testing.T){

	dir, err := os.Getwd()
	if err != nil {
	  log.Fatal(err)
	}
	config, err := LoadConfiguration(dir + "/config.json")

	var trans = NewTransaction(store.Cassandra)
	
	// assign the User or Application custom transaction.
	trans.UserTransaction = &mocks.UserTransaction{}

	trans.Begin()
	var store = btree.NewStoreDefaultSerializer("fooBar", 10, false)

	tree, err := NewBtree(store, trans, config)
	if err != nil{
		t.Error("Can't get Btree instance.")
	}
	tree.Add("foo", "bar")
	tree.MoveTo("foo", false)

	var store2 = btree.NewStoreDefaultSerializer("fooBar2", 11, false)
	tree2, _ := NewBtree(store2, trans, config)
	tree2.Add("foo", "bar")

	trans.Commit()
}
