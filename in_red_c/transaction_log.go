package in_red_c

type commitFunctions int

// Transaction commit functions.
const (
	unknown = iota
	lockTrackedItems
	commitNewRootNodes
	commitUpdatedNodes
	commitRemovedNodes
	commitAddedNodes
	commitStoreInfo
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
