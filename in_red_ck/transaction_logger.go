package in_red_ck

import (
	"context"
	"encoding/json"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
	"github.com/gocql/gocql"
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
)

const (
	// Pre commit log functions. Though these enums map to "lockTrackedItems" int value,
	// they don't conflict because these are pre commit states. Pre commit gets erased
	// during successful initial commit steps where the pre commit logs are no longer needed.
	// Thus, there is no conflict usagewise.
	addActivelyPersistedItem = unknown + 1
	updateActivelyPersistedItem = addActivelyPersistedItem
)

type transactionLog struct {
	committedState commitFunction
	logger         cas.TransactionLog
	logging        bool
	transactionID  gocql.UUID
}

// Instantiate a transaction logger.
func newTransactionLogger(logger cas.TransactionLog, logging bool) *transactionLog {
	if logger == nil {
		logger = cas.NewTransactionLog()
	}

	return &transactionLog{
		logger:        logger,
		logging:       logging,
		transactionID: gocql.UUIDFromTime(Now().UTC()),
	}
}

// Assign new UUID to the transactionID field.
func (tl *transactionLog) setNewTID() {
	tl.transactionID = gocql.UUIDFromTime(Now().UTC())
}

// Log the about to be committed function state.
func (tl *transactionLog) log(ctx context.Context, f commitFunction, payload interface{}) error {
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
func (tl *transactionLog) processExpiredTransactionLogs(ctx context.Context, t *transaction) error {
	var tid gocql.UUID
	var hr string
	var committedFunctionLogs []sop.KeyValuePair[int, interface{}]
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
	if cas.IsNil(tid) {
		hourBeingProcessed = ""
		return nil
	}
	if len(committedFunctionLogs) == 0 {
		if !cas.IsNil(tid) {
			return tl.logger.Remove(ctx, tid)
		}
		return nil
	}

	var lastErr error
	lastCommittedFunctionLog := committedFunctionLogs[len(committedFunctionLogs)-1].Key
	for i := len(committedFunctionLogs) - 1; i >= 0; i-- {
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
			v := toStruct[sop.Tuple[sop.Tuple[[]cas.RegistryPayload[sop.UUID], []cas.BlobsPayload[sop.UUID]], []sop.Tuple[bool, cas.BlobsPayload[sop.UUID]]]](committedFunctionLogs[i].Value)
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
				sis := toStruct[[]btree.StoreInfo](committedFunctionLogs[i].Value)
				if err := t.storeRepository.Update(ctx, sis...); err != nil {
					lastErr = err
				}
			}
			continue
		}
		if committedFunctionLogs[i].Key == commitAddedNodes {
			if lastCommittedFunctionLog > commitAddedNodes && committedFunctionLogs[i].Value != nil {
				bv := toStruct[sop.Tuple[[]cas.RegistryPayload[sop.UUID], []cas.BlobsPayload[sop.UUID]]](committedFunctionLogs[i].Value)
				if err := t.btreesBackend[0].nodeRepository.rollbackAddedNodes(ctx, bv); err != nil {
					lastErr = err
				}
			}
			continue
		}
		if committedFunctionLogs[i].Key == commitRemovedNodes {
			if lastCommittedFunctionLog > commitRemovedNodes && committedFunctionLogs[i].Value != nil {
				vids := toStruct[[]cas.RegistryPayload[sop.UUID]](committedFunctionLogs[i].Value)
				if err := t.btreesBackend[0].nodeRepository.rollbackRemovedNodes(ctx, vids); err != nil {
					lastErr = err
				}
			}
			continue
		}
		if committedFunctionLogs[i].Key == commitUpdatedNodes {
			if lastCommittedFunctionLog > commitUpdatedNodes && committedFunctionLogs[i].Value != nil {
				vids := toStruct[[]cas.RegistryPayload[sop.UUID]](committedFunctionLogs[i].Value)
				if err := t.btreesBackend[0].nodeRepository.rollbackUpdatedNodes(ctx, vids); err != nil {
					lastErr = err
				}
			}
			continue
		}
		if committedFunctionLogs[i].Key == commitNewRootNodes {
			if lastCommittedFunctionLog > commitNewRootNodes && committedFunctionLogs[i].Value != nil {
				bv := toStruct[sop.Tuple[[]cas.RegistryPayload[sop.UUID], []cas.BlobsPayload[sop.UUID]]](committedFunctionLogs[i].Value)
				if err := t.btreesBackend[0].nodeRepository.rollbackNewRootNodes(ctx, bv); err != nil {
					lastErr = err
				}
			}
			continue
		}
		if committedFunctionLogs[i].Key == commitTrackedItemsValues {
			if lastCommittedFunctionLog >= commitTrackedItemsValues && committedFunctionLogs[i].Value != nil {
				ifd := toStruct[[]sop.Tuple[bool, cas.BlobsPayload[sop.UUID]]](committedFunctionLogs[i].Value)
				if err := t.deleteTrackedItemsValues(ctx, ifd); err != nil {
					lastErr = err
				}
			}
			continue
		}

		// Process pre commit log functions.
		if committedFunctionLogs[i].Key == addActivelyPersistedItem && committedFunctionLogs[i].Value != nil {
			itemsForDelete := (committedFunctionLogs[i].Value).(cas.BlobsPayload[sop.UUID])
			if err := t.blobStore.Remove(ctx, itemsForDelete); err != nil {
				return err
			}
		}
	}

	if err := tl.logger.Remove(ctx, tid); err != nil {
		lastErr = err
	}
	return lastErr
}

func toStruct[T any](obj interface{}) T {
	var t T
	if obj == nil {
		return t
	}
	ba, _ := json.Marshal(obj)
	json.Unmarshal(ba, &t)
	return t
}
