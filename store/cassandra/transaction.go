package cassandra

import "../../btree"

type TransactionSession struct{
	TransactionID *btree.UUID
	Started *bool
	StoreMap *map[string]*btree.Btree
}

func (trans *TransactionSession) Begin() error{
	return nil
}

// CommitPhase1 commits all changes to each Btree modified during transaction.
func (trans *TransactionSession) CommitPhase1() error{
	return nil
}
// CommitPhase2 finalize commits of each Btree modified during transaction.
func (trans *TransactionSession) CommitPhase2() error{
	return nil
}

// Rollback undoes any changes done to each Btree modified during transaction.
func (trans *TransactionSession) Rollback() error{
	return nil
}
