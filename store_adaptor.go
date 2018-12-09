package sop

import "./btree"
import cass "./store/cassandra"

type StoreType uint
const (
	Cassandra = iota
	//AwsS3
)

func OpenBtreeNoTrans(storeName string, 
	itemSerializer btree.ItemSerializer, 
	storeType uint) btree.BtreeInterface{
	return OpenBtree(storeName, itemSerializer, storeType, nil)
}

func OpenBtree(storeName string, 
	itemSerializer btree.ItemSerializer, 
	storeType uint,
	trans *TransactionSession) btree.BtreeInterface{
	var si = newStoreInterface(storeType)
	var store = si.StoreRepository.Get(storeName)
	store.ItemSerializer = itemSerializer
	var r = btree.Btree{
		Store:store,
		StoreInterface:si,
	}
	return &r;
}

func NewBtree(store *btree.Store, trans *TransactionSession) btree.BtreeInterface{
	var si = newStoreInterface(trans.storeType)
	var r = btree.Btree{
		Store:store,
		StoreInterface:si,
	}
	return &r;
}

func newStoreInterface(storeType uint) *btree.StoreInterface{
	var si = btree.StoreInterface{
		StoreType: storeType,
	}
	return &si
}

func NewTransaction(storeType uint) *TransactionSession{
	var t = TransactionSession{
		storeType: storeType,
	}
	var bt = cass.TransactionSession{
		TransactionID: &t.TransactionID,
		Started: &t.Started,
		StoreMap: &t.StoreMap,
	}
	t.btreeTransaction = &bt
	return &t
}
