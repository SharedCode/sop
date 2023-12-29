package in_cas_s3

type commitFunctions int
// Transaction commit functions.
const (
	lockTrackedItems = iota
	commitUpdatedNodes
	commitRemovedNodes
	commitAddedNodes
	commitStoreRepositoryChanges

	activateInactiveNodes
	touchRemovedNodes

	unlockTrackedItems
)

type transactionLog struct {
	commitFunctions []commitFunctions
}

// Instantiate a transaction logger.
func newTransactionLogger() *transactionLog {
	return &transactionLog{}
}

// Log a function call to transaction log to aid in rollback,
// if rollback is invoked implicitly or explicitly.
func (tl *transactionLog) log(f commitFunctions) {
	tl.commitFunctions = append(tl.commitFunctions, f)
}

