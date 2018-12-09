package Sop

// Transaction interface
type Transaction interface{
	Begin() error
	Commit() error
	Rollback() error
}
