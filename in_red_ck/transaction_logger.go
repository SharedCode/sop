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
	deleteObsoleteTrackedItemsValues
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
	deleteObsoleteTrackedItemsValues: "deleteObsoleteTrackedItemsValues",
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
	"deleteObsoleteTrackedItemsValues": deleteObsoleteTrackedItemsValues,
	"unlockTrackedItems":               unlockTrackedItems,
}

type transactionLog struct {
	committedState commitFunctions
	logger         cas.TransactionLog
	transactionId  sop.UUID
	queuedLogs     []sop.KeyValuePair[commitFunctions, interface{}]
}

// Instantiate a transaction logger.
func newTransactionLogger(logger cas.TransactionLog) *transactionLog {
	if logger == nil {
		logger = cas.NewTransactionLog()
	}
	return &transactionLog{
		logger: logger,
	}
}

// enqueue will add a log to the queue, which can be persisted calling saveQueue.
func (tl *transactionLog) enqueue(f commitFunctions, payload interface{}) {
	tl.committedState = f
	if payload == nil {
		return
	}
	tl.queuedLogs = append(tl.queuedLogs, sop.KeyValuePair[commitFunctions, interface{}]{Key: f, Value: payload})
}
func (tl *transactionLog) clearQueue() {
	tl.queuedLogs = nil
}

// save the enqueued logs to backend.
func (tl *transactionLog) saveQueue(ctx context.Context) error {
	for i := range tl.queuedLogs {
		if err := tl.log(ctx, tl.queuedLogs[i].Key, tl.queuedLogs[i].Value); err != nil {
			return err
		}
	}
	return nil
}

// Log the committed function state.
func (tl *transactionLog) log(ctx context.Context, f commitFunctions, payload interface{}) error {
	tl.committedState = f
	if tl.transactionId.IsNil() {
		tl.logger.Initiate(ctx, sop.NilUUID, "", nil)
		tl.transactionId = sop.NewUUID()
		return nil
	}
	if payload == nil {
		return nil
	}
	return tl.logger.Add(ctx, tl.transactionId, commitFunctionsStringLookup[f], payload)
}

// removes logs saved to backend. During commit completion, logs need to be cleared.
func (tl *transactionLog) removeLogs(ctx context.Context) error {
	return tl.logger.Remove(ctx, tl.transactionId)
}
