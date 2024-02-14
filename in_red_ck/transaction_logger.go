package in_red_ck

import (
	"context"
	"encoding/json"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
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

func (tl *transactionLog) processExpiredTransactionLogs(ctx context.Context, t *transaction) error {
	tid, committedFunctionLogs, err := tl.logger.GetOne(ctx)
	if err != nil {
		return err
	}
	if len(committedFunctionLogs) == 0 {
		if !tid.IsNil() {
			return tl.logger.Remove(ctx, tid)
		}
		return nil
	}

	var lastErr error
	lastCommittedFunctionLog := toCommitFunction(committedFunctionLogs[len(committedFunctionLogs)-1].Key)
	for i := len(committedFunctionLogs)-1; i >= 0; i-- {
		if toCommitFunction(committedFunctionLogs[i].Key) == finalizeCommit {
			v := toStruct[sop.Tuple[sop.Tuple[[]cas.RegistryPayload[sop.UUID], []cas.BlobsPayload[sop.UUID]], []sop.Tuple[bool, cas.BlobsPayload[sop.UUID]]]](committedFunctionLogs[i].Value)
			if lastCommittedFunctionLog == deleteTrackedItemsValues {
				if err := t.deleteTrackedItemsValues(ctx, v.Second); err != nil {
					lastErr = err
				}
				if err := t.deleteObsoleteEntries(ctx, v.First.First, v.First.Second); err != nil {
					lastErr = err
				}
			}
			if lastCommittedFunctionLog == deleteObsoleteEntries {
				if err := t.deleteObsoleteEntries(ctx, v.First.First, v.First.Second); err != nil {
					lastErr = err
				}
			}
			return lastErr
		}
		if toCommitFunction(committedFunctionLogs[i].Key) == commitStoreInfo {
			if lastCommittedFunctionLog > commitStoreInfo {
				sis := toStruct[[]btree.StoreInfo](committedFunctionLogs[i].Value)
				if err := t.storeRepository.Update(ctx, sis...); err != nil {
					lastErr = err
				}
			}
			continue
		}
		if toCommitFunction(committedFunctionLogs[i].Key) == commitAddedNodes {
			if lastCommittedFunctionLog > commitAddedNodes {
				bv := toStruct[sop.Tuple[[]cas.RegistryPayload[sop.UUID], []cas.BlobsPayload[sop.UUID]]](committedFunctionLogs[i].Value)
				if err := t.btreesBackend[0].nodeRepository.rollbackAddedNodes(ctx, bv); err != nil {
					lastErr = err
				}
			}
			continue
		}
		if toCommitFunction(committedFunctionLogs[i].Key) == commitRemovedNodes {
			if lastCommittedFunctionLog > commitRemovedNodes {
				vids := toStruct[[]cas.RegistryPayload[sop.UUID]](committedFunctionLogs[i].Value)
				if err := t.btreesBackend[0].nodeRepository.rollbackRemovedNodes(ctx, vids); err != nil {
					lastErr = err
				}
			}
			continue
		}
		if toCommitFunction(committedFunctionLogs[i].Key) == commitUpdatedNodes {
			if lastCommittedFunctionLog > commitUpdatedNodes {
				vids := toStruct[[]cas.RegistryPayload[sop.UUID]](committedFunctionLogs[i].Value)
				if err := t.btreesBackend[0].nodeRepository.rollbackUpdatedNodes(ctx, vids); err != nil {
					lastErr = err
				}
			}
			continue
		}
		if toCommitFunction(committedFunctionLogs[i].Key) == commitNewRootNodes {
			if lastCommittedFunctionLog > commitNewRootNodes {
				bv := toStruct[sop.Tuple[[]cas.RegistryPayload[sop.UUID], []cas.BlobsPayload[sop.UUID]]](committedFunctionLogs[i].Value)
				if err := t.btreesBackend[0].nodeRepository.rollbackNewRootNodes(ctx, bv); err != nil {
					lastErr = err
				}
			}
			continue
		}
		if toCommitFunction(committedFunctionLogs[i].Key) == commitTrackedItemsValues {
			if lastCommittedFunctionLog >= commitTrackedItemsValues {
				ifd := toStruct[[]sop.Tuple[bool, cas.BlobsPayload[sop.UUID]]](committedFunctionLogs[i].Value)
				if err := t.deleteTrackedItemsValues(ctx, ifd); err != nil {
					lastErr = err
				}
			}
		}
	}

	if err := tl.logger.Remove(ctx, tid); err != nil {
		lastErr = err
	}
	return lastErr
}

func toStruct[T any](obj interface{}) T {
	ba, _ := json.Marshal(obj)
	var t T
	json.Unmarshal(ba, &t)
	return t
}
