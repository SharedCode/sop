package in_cas_s3

import "fmt"

// Transaction interface defines the "enduser facing" transaction methods.
type Transaction interface {
	Begin() error
	Commit() error
	Rollback() error
	HasBegun() bool
}

type transaction struct {
	// stores(or its items) accessed/managed within the transaction session.
	stores []StoreInterface[interface{}, interface{}]
	forWriting bool
	hasBegun bool
}

// NewTransaction will instantiate a transaction object for writing(forWriting=true)
// or for reading(forWriting=false).
func NewTransaction(forWriting bool) Transaction {
	return &transaction{
		forWriting: forWriting,
	}
}

func (t *transaction) Begin() error {
	return nil
}

func (t *transaction) Commit() error {
	if !t.hasBegun {
		return fmt.Errorf("No transaction session to commit, call Begin to start a transaction session.")
	}

	return nil
}

func (t *transaction) Rollback() error {
	return nil
}

func (t *transaction) HasBegun() bool {
	return t.hasBegun
}