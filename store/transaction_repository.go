package store;

import "../btree"

type tc Connection

func (conn *tc) Get(transactionID btree.UUID) ([]*btree.TransactionEntry, error){
	return nil, nil
}
func (conn *tc) GetByStore(transactionID btree.UUID, storeName string) ([]*btree.TransactionEntry, error){
	return nil, nil
}
func (conn *tc) Add([]*btree.TransactionEntry) error{
	return nil
}
//Update([]*TransactionEntry) error
func (conn *tc) MarkDone([]*btree.TransactionEntryKeys) error{
	return nil
}
