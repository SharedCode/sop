package common

import (
	"context"
	"fmt"
	log "log/slog"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
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
	sop.TransactionLog
	committedState commitFunction
	logging        bool
	transactionID  sop.UUID
}

// Instantiate a transaction logger.
func newTransactionLogger(logger sop.TransactionLog, logging bool) *transactionLog {
	return &transactionLog{
		TransactionLog: logger,
		logging:        logging,
		transactionID:  logger.NewUUID(),
	}
}

// Log the about to be committed function state.
func (tl *transactionLog) log(ctx context.Context, f commitFunction, payload []byte) error {
	tl.committedState = f
	if !tl.logging || f == unknown {
		return nil
	}

	return tl.TransactionLog.Add(ctx, tl.transactionID, int(f), payload)
}

// removes logs saved to backend. During commit completion, logs need to be cleared.
func (tl *transactionLog) removeLogs(ctx context.Context) error {
	if !tl.logging {
		return nil
	}
	err := tl.TransactionLog.Remove(ctx, tl.transactionID)
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
		tid, hr, committedFunctionLogs, err = tl.TransactionLog.GetOne(ctx)
		if err != nil {
			return err
		}
		hourBeingProcessed = hr
	} else {
		tid, committedFunctionLogs, err = tl.TransactionLog.GetOneOfHour(ctx, hourBeingProcessed)
		if err != nil {
			return err
		}
	}
	if tid.IsNil() {
		hourBeingProcessed = ""
		return nil
	}
	return tl.rollback(ctx, t, tid, committedFunctionLogs)
}

func (tl *transactionLog) doPriorityRollbacks(ctx context.Context, t *Transaction) (bool, error) {
	lk := t.l2Cache.CreateLockKeys([]string{t.l2Cache.FormatLockKey("Prbs")})
	if ok, _, _ := t.l2Cache.Lock(ctx, 5*time.Minute, lk); ok {
		if ok, _ := t.l2Cache.IsLocked(ctx, lk); !ok {
			return false, nil
		}
		log.Info("Entering doPriorityRollbacks loop(5).")
		defer t.l2Cache.Unlock(ctx, lk)
		start := sop.Now()

		for range 5 {
			if tid, uhAndrh, err := tl.PriorityLog().GetOne(ctx); !tid.IsNil() {
				if err := t.registry.UpdateNoLocks(ctx, false, uhAndrh); err != nil {
					// When Registry is known to be corrupted, we can raise a failover event.
					return false, sop.Error[sop.UUID]{
						Code:     sop.RestoreRegistryFileSectorFailure,
						Err:      err,
						UserData: tid,
					}
				}
				if err := tl.PriorityLog().Remove(ctx, tid); err != nil {
					return false, err
				}
			} else if err != nil {
				return false, err
			} else {
				break
			}
			// Loop through 5 or until timeout if busy.
			if err := sop.TimedOut(ctx, "doPriorityRollbacks", start, time.Duration(4.5*time.Hour.Minutes())); err != nil {
				return true, nil
			}
		}
	}
	return false, nil
}

func (tl *transactionLog) priorityRollback(ctx context.Context, t *Transaction, tid sop.UUID) error {
	if uhAndrh, err := tl.PriorityLog().Get(ctx, tid); uhAndrh == nil || err != nil {
		return err
	} else {
		if err := t.registry.UpdateNoLocks(ctx, false, uhAndrh); err != nil {
			// When Registry is known to be corrupted, we can raise a failover event.
			return sop.Error[sop.UUID]{
				Code:     sop.RestoreRegistryFileSectorFailure,
				Err:      err,
				UserData: tid,
			}
		}
		return tl.PriorityLog().Remove(ctx, tid)
	}
}

func (tl *transactionLog) rollback(ctx context.Context, t *Transaction, tid sop.UUID, committedFunctionLogs []sop.KeyValuePair[int, []byte]) error {
	if len(committedFunctionLogs) == 0 {
		if !tid.IsNil() {
			return tl.TransactionLog.Remove(ctx, tid)
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
					if err := tl.TransactionLog.Remove(ctx, tid); err != nil {
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
				if err := tl.TransactionLog.Remove(ctx, tid); err != nil {
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

	if err := tl.TransactionLog.Remove(ctx, tid); err != nil {
		lastErr = err
	}

	return lastErr
}

func toStruct[T any](obj []byte) T {
	var t T
	if obj == nil {
		return t
	}
	encoding.DefaultMarshaler.Unmarshal(obj, &t)
	return t
}

func toByteArray(obj interface{}) []byte {
	ba, _ := encoding.DefaultMarshaler.Marshal(obj)
	return ba
}
