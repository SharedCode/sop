package in_red_ck

import (
	"context"

	"github.com/SharedCode/sop"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
)

type commitFunctions int

// Transaction commit functions.
const (
	unknown = iota
	lockTrackedItems
	commitTrackedItemsValues
	commitNewRootNodes
	areFetchedItemsIntact
	commitUpdatedNodes
	commitRemovedNodes
	commitAddedNodes
	commitStoreInfo
	beforeFinalize
	finalizeCommit
	deleteObsoleteEntries
	deleteTrackedItemsValues
	unlockTrackedItems
)

var commitFunctionsStringLookup map[commitFunctions]string = map[commitFunctions]string{
	lockTrackedItems:                 "lockTrackedItems",
	commitTrackedItemsValues:         "commitTrackedItemsValues",
	commitNewRootNodes:               "commitNewRootNodes",
	areFetchedItemsIntact:            "areFetchedItemsIntact",
	commitUpdatedNodes:               "commitUpdatedNodes",
	commitRemovedNodes:               "commitRemovedNodes",
	commitAddedNodes:                 "commitAddedNodes",
	commitStoreInfo:                  "commitStoreInfo",
	beforeFinalize:                   "beforeFinalize",
	finalizeCommit:                   "finalizeCommit",
	deleteObsoleteEntries:            "deleteObsoleteEntries",
	deleteTrackedItemsValues: "deleteObsoleteTrackedItemsValues",
	unlockTrackedItems:               "unlockTrackedItems",
}
var commitFunctionsLookup map[string]commitFunctions = map[string]commitFunctions{
	"lockTrackedItems":                 lockTrackedItems,
	"commitTrackedItemsValues":         commitTrackedItemsValues,
	"commitNewRootNodes":               commitNewRootNodes,
	"areFetchedItemsIntact":            areFetchedItemsIntact,
	"commitUpdatedNodes":               commitUpdatedNodes,
	"commitRemovedNodes":               commitRemovedNodes,
	"commitAddedNodes":                 commitAddedNodes,
	"commitStoreInfo":                  commitStoreInfo,
	"beforeFinalize":                   beforeFinalize,
	"finalizeCommit":                   finalizeCommit,
	"deleteObsoleteEntries":            deleteObsoleteEntries,
	"deleteObsoleteTrackedItemsValues": deleteTrackedItemsValues,
	"unlockTrackedItems":               unlockTrackedItems,
}

type transactionLog struct {
	committedState commitFunctions
	logger         cas.TransactionLog
	logging bool
	transactionID  sop.UUID
	queuedLogs     []sop.KeyValuePair[commitFunctions, interface{}]
}

var synthesizeErrorOnFunction commitFunctions = unknown
var syntheticError error

// Instantiate a transaction logger.
func newTransactionLogger(logger cas.TransactionLog, logging bool) *transactionLog {
	if logger == nil {
		// TODO: switch to the real TransactionLog when ready.
		logger = cas.NewMockTransactionLog()
	}
	return &transactionLog{
		logger: logger,
		logging: logging,
	}
}

func toString(f commitFunctions) string {
	s, _ := commitFunctionsStringLookup[f]
	return s
}
func toCommitFunction(s string) commitFunctions {
	f, _ := commitFunctionsLookup[s]
	return f
}

// Log the about to be committed function state.
func (tl *transactionLog) log(ctx context.Context, f commitFunctions, payload interface{}) error {
	tl.committedState = f
	if !tl.logging || f == unknown {
		return nil
	}

	// Allow unit test to synthesize error for unit testing.
	if synthesizeErrorOnFunction == f && syntheticError != nil {
		return syntheticError
	}

	if tl.transactionID.IsNil() {
		tl.transactionID = sop.NewUUID()
		tl.logger.Initiate(ctx, tl.transactionID, toString(f), payload)
		return nil
	}
	return tl.logger.Add(ctx, tl.transactionID, toString(f), payload)
}

// removes logs saved to backend. During commit completion, logs need to be cleared.
func (tl *transactionLog) removeLogs(ctx context.Context) error {
	if !tl.logging {
		return nil
	}
	err := tl.logger.Remove(ctx, tl.transactionID)
	tl.transactionID = sop.NilUUID
	return err
}
