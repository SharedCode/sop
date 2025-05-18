package common

import (
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"

	"github.com/SharedCode/sop"
)

type commitFunction int

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

	// Pre commit functions.
	addActivelyPersistedItem    = 99
	updateActivelyPersistedItem = addActivelyPersistedItem
)

type transactionLog struct {
	committedState commitFunction
	logger         sop.TransactionLog
	logging        bool
	transactionID  sop.UUID
}

// Instantiate a transaction logger.
func newTransactionLogger(logger sop.TransactionLog, logging bool) *transactionLog {
	return &transactionLog{
		logger:        logger,
		logging:       logging,
		transactionID: logger.NewUUID(),
	}
}

// Assign new UUID to the transactionID field.
func (tl *transactionLog) setNewTID() {
	tl.transactionID = tl.logger.NewUUID()
}

// Log the about to be committed function state.
func (tl *transactionLog) log(ctx context.Context, f commitFunction, payload []byte) error {
	tl.committedState = f
	if !tl.logging || f == unknown {
		return nil
	}

	return tl.logger.Add(ctx, tl.transactionID, int(f), payload)
}

// removes logs saved to backend. During commit completion, logs need to be cleared.
func (tl *transactionLog) removeLogs(ctx context.Context) error {
	if !tl.logging {
		return nil
	}
	err := tl.logger.Remove(ctx, tl.transactionID)
	return err
}

var hourBeingProcessed string

// Consume all Transaction IDs(TIDs) and clean their obsolete, leftover resources that fall within a given hour.
// Using a package level variable(hourBeingProcessed) to keep the "hour" being worked on and the processor function below
// to consume all TIDs of the hour before issuing another GetOne call to fetch the next hour.
func (tl *transactionLog) processExpiredTransactionLogs(ctx context.Context, t *Transaction) error {
	var tid sop.UUID
	var hr string
	var committedFunctionLogs []sop.KeyValuePair[int, []byte]
	var err error
	if hourBeingProcessed == "" {
		tid, hr, committedFunctionLogs, err = tl.logger.GetOne(ctx)
		if err != nil {
			return err
		}
		hourBeingProcessed = hr
	} else {
		tid, committedFunctionLogs, err = tl.logger.GetLogsDetails(ctx, hourBeingProcessed)
		if err != nil {
			return err
		}
	}
	if tid.IsNil() {
		hourBeingProcessed = ""
		return nil
	}
	if len(committedFunctionLogs) == 0 {
		if !tid.IsNil() {
			return tl.logger.Remove(ctx, tid)
		}
		return nil
	}

	var lastErr error
	lastCommittedFunctionLog := committedFunctionLogs[len(committedFunctionLogs)-1].Key
	for i := len(committedFunctionLogs) - 1; i >= 0; i-- {
		// Process pre commit log functions.
		if committedFunctionLogs[i].Key == addActivelyPersistedItem && committedFunctionLogs[i].Value != nil {
			itemsForDelete := toStruct[sop.BlobsPayload[sop.UUID]](committedFunctionLogs[i].Value)
			if err := t.blobStore.Remove(ctx, []sop.BlobsPayload[sop.UUID]{itemsForDelete}); err != nil {
				lastErr = err
			}
			continue
		}

		// Process commit log functions.
		if committedFunctionLogs[i].Key == finalizeCommit {
			if committedFunctionLogs[i].Value == nil {
				if lastCommittedFunctionLog >= deleteObsoleteEntries {
					if err := tl.logger.Remove(ctx, tid); err != nil {
						lastErr = err
					}
					return lastErr
				}
				continue
			}
			v := toStruct[sop.Tuple[sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]], []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]]](committedFunctionLogs[i].Value)
			if lastCommittedFunctionLog == deleteTrackedItemsValues {
				if err := t.deleteTrackedItemsValues(ctx, v.Second); err != nil {
					lastErr = err
				}
			}
			if lastCommittedFunctionLog >= deleteObsoleteEntries {
				if err := t.deleteObsoleteEntries(ctx, v.First.First, v.First.Second); err != nil {
					lastErr = err
				}
				if err := tl.logger.Remove(ctx, tid); err != nil {
					lastErr = err
				}
				return lastErr
			}
			continue
		}
		if committedFunctionLogs[i].Key == commitStoreInfo {
			if lastCommittedFunctionLog > commitStoreInfo && committedFunctionLogs[i].Value != nil {
				sis := toStruct[[]sop.StoreInfo](committedFunctionLogs[i].Value)
				if _, err := t.storeRepository.Update(ctx, sis); err != nil {
					lastErr = err
				}
			}
			continue
		}
		if committedFunctionLogs[i].Key == commitAddedNodes {
			if lastCommittedFunctionLog > commitAddedNodes && committedFunctionLogs[i].Value != nil {
				bv := toStruct[sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]](committedFunctionLogs[i].Value)
				if err := t.btreesBackend[0].nodeRepository.rollbackAddedNodes(ctx, bv); err != nil {
					lastErr = err
				}
			}
			continue
		}
		if committedFunctionLogs[i].Key == commitRemovedNodes {
			if lastCommittedFunctionLog > commitRemovedNodes && committedFunctionLogs[i].Value != nil {
				vids := toStruct[[]sop.RegistryPayload[sop.UUID]](committedFunctionLogs[i].Value)
				if err := t.btreesBackend[0].nodeRepository.rollbackRemovedNodes(ctx, false, vids); err != nil {
					lastErr = err
				}
			}
			continue
		}
		if committedFunctionLogs[i].Key == commitUpdatedNodes {
			if lastCommittedFunctionLog >= commitUpdatedNodes && committedFunctionLogs[i].Value != nil {
				blobsIDs := toStruct[[]sop.BlobsPayload[sop.UUID]](committedFunctionLogs[i].Value)
				log.Info(fmt.Sprintf("about to remove unused Nodes: %v", blobsIDs))
				// In Updated Nodes, removal of left hanging temp Nodes is the task. No need to do anything else as the main data flow,
				// transaction is able to clean up the Handle and kick out the unfinalized InactiveID that refers to the temp Node.
				if err := t.btreesBackend[0].nodeRepository.removeNodes(ctx, blobsIDs); err != nil {
					lastErr = err
				}
			}
			continue
		}
		if committedFunctionLogs[i].Key == commitNewRootNodes {
			if lastCommittedFunctionLog > commitNewRootNodes && committedFunctionLogs[i].Value != nil {
				bv := toStruct[sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]](committedFunctionLogs[i].Value)
				if err := t.btreesBackend[0].nodeRepository.rollbackNewRootNodes(ctx, bv); err != nil {
					lastErr = err
				}
			}
			continue
		}
		if committedFunctionLogs[i].Key == commitTrackedItemsValues {
			if lastCommittedFunctionLog >= commitTrackedItemsValues && committedFunctionLogs[i].Value != nil {
				ifd := toStruct[[]sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]](committedFunctionLogs[i].Value)
				if err := t.deleteTrackedItemsValues(ctx, ifd); err != nil {
					lastErr = err
				}
			}
			continue
		}
	}

	if err := tl.logger.Remove(ctx, tid); err != nil {
		lastErr = err
	}
	return lastErr
}

func toStruct[T any](obj []byte) T {
	var t T
	if obj == nil {
		return t
	}
	json.Unmarshal(obj, &t)
	return t
}

func toByteArray(obj interface{}) []byte {
	ba, _ := json.Marshal(obj)
	return ba
}
