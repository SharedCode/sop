package transaction

import "errors"
import "../../Sop"
import "../btree"

type UUID [16]byte

type TransactionSession struct{
	TransactionID UUID
	Started bool
	StoreMap map[string]btree.Btree
	UserTransaction Sop.Transaction
}

func (trans *TransactionSession) Begin() error {
	if trans.Started{
		return errors.New("Transaction is already started.")
	}
	if trans.UserTransaction != nil{
		err := trans.UserTransaction.Begin()
		if err != nil {return err}
	}	
	trans.Started = true

	// begin transaction here...

	return nil
}

func (trans *TransactionSession) Commit() error{
	if !trans.Started{
		return errors.New("Transaction not started.")
	}
	if trans.UserTransaction != nil{
		err := trans.UserTransaction.Commit()
		if err != nil {
			// auto rollback when user trans fails.
			var t = trans.UserTransaction
			trans.UserTransaction = nil
			trans.Rollback()
			trans.UserTransaction = t
			return err
		}
	}

	// commit here...

	trans.Started = false
	return nil
}

func (trans *TransactionSession) Rollback() error{
	if !trans.Started{
		return errors.New("Transaction not started.")
	}
	var err error
	if trans.UserTransaction != nil{
		err = trans.UserTransaction.Rollback()
	}
	trans.Started = false

	// rollback here...

	return err
}
