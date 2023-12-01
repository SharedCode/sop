package in_aws

// Transaction interface defines the "enduser facing" transaction methods.
type Transaction interface {
	Begin() error
	Commit() error
	Rollback() error
	HasBegun() bool
}

type transaction struct {

}

func NewTransaction() Transaction {
	return &transaction{}
}

func (t *transaction) Begin() error {
	return nil
}

func (t *transaction) Commit() error {
	return nil
}

func (t *transaction) Rollback() error {
	return nil
}

func (t *transaction) HasBegun() bool {
	return false
}
