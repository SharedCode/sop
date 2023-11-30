package in_aws

type Transaction struct {

}

func (t *Transaction) Begin() error {
	return nil
}

func (t *Transaction) Commit() error {
	return nil
}

func (t *Transaction) Rollback() error {
	return nil
}
