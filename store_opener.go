package sop

import "./btree"
import "./cache"
import "./store"

// For now, below code only caters for Cassandra Store.

func OpenBtreeNoTrans(storeName string, itemSerializer btree.ItemSerializer, storeType uint) (btree.BtreeInterface, error){
	return OpenBtree(storeName, itemSerializer, storeType, nil)
}

func OpenBtree(storeName string, itemSerializer btree.ItemSerializer, 
	storeType uint, trans *TransactionSession) (btree.BtreeInterface, error){
	var si, err = newStoreInterface(storeType)
	if err != nil {return nil, err}
	var store = si.StoreRepository.Get(storeName)
	store.ItemSerializer = itemSerializer
	var r = btree.Btree{
		Store:store,
		StoreInterface:si,
	}
	if trans != nil {
		trans.StoreMap[storeName] = &r
	}
	return &r, nil
}

func NewBtree(store *btree.Store, trans *TransactionSession) (btree.BtreeInterface, error){
	var si, err = newStoreInterface(trans.storeType)
	if err != nil{ return nil, err}
	var r = btree.Btree{
		Store:store,
		StoreInterface:si,
	}
	if trans != nil {
		trans.StoreMap[store.Name] = &r
	}
	return &r, nil
}

func newStoreInterface(storeType uint) (*btree.StoreInterface, error){
	var opt cache.Options
	conn, err := store.NewConnection(storeType, opt)
	if err != nil{
		return nil, err
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
