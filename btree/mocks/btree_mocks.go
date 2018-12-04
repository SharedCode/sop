package mocks

import "../../btree"
import cb3 "../../store/cassandra/btree"

func NewStoreRepository() btree.StoreRepository{
	return nil
}

func NewNodeRepository() btree.NodeRepository{
	return cb3.NewNodeRepository()
}
