package sop

import "errors"
import "./btree"

type PhasedTransaction interface{
	Begin() error
	CommitPhase1() error
	// CommitPhase2 should be sure to succeed. 
	// SOP will not check result of User Trans phase2 commit.
	CommitPhase2() error
	Rollback() error
}

// Transaction interface
type Transaction interface{
	Begin() error
	Commit() error
	Rollback() error
}

type TransactionSession struct{
	TransactionID btree.UUID
	Started bool
	StoreMap map[string]*btree.Btree
	UserTransaction PhasedTransaction
	btreeTransaction PhasedTransaction
	storeType uint
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

	// begin transaction
	return trans.btreeTransaction.Begin()
}

func (trans *TransactionSession) CommitPhase1() error{
	if !trans.Started{
		return errors.New("Transaction not started.")
	}

	// commit Btrees...
	err := trans.btreeTransaction.CommitPhase1()
	if err != nil {
		trans.Rollback()
		return err
	}

	if trans.UserTransaction != nil{
		err = trans.UserTransaction.CommitPhase1()
		if err != nil {
			// auto rollback when user trans fails.
			trans.Rollback()
			return err
		}
	}

	return nil
}
func (trans *TransactionSession) CommitPhase2() error{
	if !trans.Started{
		return errors.New("Transaction not started.")
	}

	// finalize Btrees...
	err := trans.btreeTransaction.CommitPhase2()
	if err != nil {
		trans.Rollback()
		return err
	}

	if trans.UserTransaction != nil{
		trans.UserTransaction.CommitPhase2()
	}

	trans.Started = false
	return nil
}

func (trans *TransactionSession) Commit() error{
	if !trans.Started{
		return errors.New("Transaction not started.")
	}

	err := trans.CommitPhase1()
	if err == nil {
		err = trans.CommitPhase2()
		if err == nil{
			trans.Started = false
			return nil
		}
	}
	return err
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

	// rollback Btrees here...
	err2 := trans.btreeTransaction.Rollback()
	if err == nil {err = err2}

	return err
}
