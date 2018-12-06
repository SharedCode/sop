package transaction

import "errors"

type UUID [16]byte

type TransactionSession struct{
	TransactionID UUID
	Started bool
	StoreMap map[string]byte
}

type Transaction interface{
	Begin() error
	Commit() error
	Rollback() error
}

func (trans *TransactionSession) Begin() error {
	if trans.Started{
		return errors.New("Transaction is already started.")
	}
	trans.Started = true
	return nil
}

func (trans *TransactionSession) Commit() error{
	if !trans.Started{
		return errors.New("Transaction not started.")
	}
	trans.Started = false
	return nil
}

func (trans *TransactionSession) Rollback() error{
	if !trans.Started{
		return errors.New("Transaction not started.")
	}
	trans.Started = false
	return nil
}
