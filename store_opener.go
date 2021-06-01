package sop

import (
	"fmt"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/store"
)

// For now, below code only caters for Cassandra Store.

func OpenBtreeNoTrans(storeName string, itemSerializer btree.ItemSerializer, storeType uint,
	config Configuration) (btree.BtreeInterface, error){
	return OpenBtree(storeName, itemSerializer, storeType, nil, config)
}

func OpenBtree(storeName string, itemSerializer btree.ItemSerializer, 
	storeType uint, trans *TransactionSession,
	config Configuration) (btree.BtreeInterface, error){
	var si, err = newStoreInterface(storeType, config)
	if err != nil {return nil, err}
	var store = si.StoreRepository.Get(storeName)
	store.ItemSerializer = itemSerializer
	var r = btree.NewBtree(store,si)
	if trans != nil {
		trans.StoreMap[storeName] = r
	}
	return r, nil
}

func NewBtree(store btree.Store, trans *TransactionSession, config Configuration) (btree.BtreeInterface, error){
	var si, err = newStoreInterface(trans.storeType, config)
	if err != nil{ return nil, err}
	var r = btree.NewBtree(store,si)
	if trans == nil {
		return nil, fmt.Errorf("Transaction session is req'd when creating a new Btree")
	}
	trans.ActionRepository = si.TransactionRepository
	trans.StoreMap[store.Name] = r
	// record the "Create new Btree action".
	// todo

	return r, nil
}

func newStoreInterface(storeType uint, config Configuration) (btree.StoreInterface, error){
	conn, err := store.NewConnection(storeType, config.RedisConfig, config.CassandraConfig)
	if err != nil{
		return btree.StoreInterface{}, err
	}
	return conn.GetStoreInterface(), nil
}

func NewTransaction(storeType uint) *TransactionSession{
	var t = TransactionSession{
		storeType: storeType,
		StoreMap: make(map[string]*btree.Btree,5),
	}
	var bt = store.TransactionSession{
		TransactionID: &t.TransactionID,
		Started: &t.Started,
		StoreMap: &t.StoreMap,
	}
	t.btreeTransaction = &bt
	return &t
}
