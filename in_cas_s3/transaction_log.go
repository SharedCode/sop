package in_cas_s3

type commitFunctions int

// Transaction commit functions.
const (
	unknown = iota
	lockTrackedItems
	commitUpdatedNodes
	commitRemovedNodes
	commitAddedNodes
	finalizeCommit
	unlockTrackedItems
)

type transactionLog struct {
	committedState commitFunctions
}

// Instantiate a transaction logger.
func newTransactionLogger() *transactionLog {
	return &transactionLog{}
}

// Log the committed function state.
func (tl *transactionLog) log(f commitFunctions) {
	tl.committedState = f
}
