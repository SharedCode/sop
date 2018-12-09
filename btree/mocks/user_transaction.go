package mocks

import "errors"

type UserTransaction struct{
	// add your members...
	Started bool
}

func (trans *UserTransaction) Begin() error {
	if trans.Started{
		return errors.New("Transaction is already started.")
	}
	trans.Started = true

	// call your DB BeginTrans here...

	return nil
}

func (trans *UserTransaction) Commit() error{
	if !trans.Started{
		return errors.New("Transaction not started.")
	}
	trans.Started = false

	// call your DB CommitTrans here...

	return nil
}

func (trans *UserTransaction) Rollback() error{
	if !trans.Started{
		return errors.New("Transaction not started.")
	}
	trans.Started = false

	// call your DB RollbackTrans here...

	return nil
}

