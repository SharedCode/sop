package in_cas_s3

type commitFunctions int
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

func newTransactionLogger() *transactionLog {
	return &transactionLog{}
}

func (tl *transactionLog) log(f commitFunctions) {
	tl.commitFunctions = append(tl.commitFunctions, f)
}

