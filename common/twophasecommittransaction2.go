package common

import (
	"context"
	"fmt"
	log "log/slog"
	"sync"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/inmemory"
)

func (t *Transaction) cleanup(ctx context.Context) error {
	// Cleanup resources not needed anymore.
	if err := t.logger.log(ctx, deleteObsoleteEntries, nil); err != nil {
		return err
	}
	obsoleteEntries := t.getToBeObsoleteEntries()
	t.deleteObsoleteEntries(ctx, obsoleteEntries.First, obsoleteEntries.Second)

	if err := t.logger.log(ctx, deleteTrackedItemsValues, nil); err != nil {
		return err
	}
	t.deleteTrackedItemsValues(ctx, t.getObsoleteTrackedItemsValues())

	// Remove unneeded transaction logs since commit is done.
	t.logger.removeLogs(ctx)
	return nil
}

func (t *Transaction) getToBeObsoleteEntries() sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]] {
	// Cleanup resources not needed anymore.
	unusedNodeIDs := make([]sop.BlobsPayload[sop.UUID], 0, len(t.updatedNodeHandles)+len(t.removedNodeHandles))
	for i := range t.updatedNodeHandles {
		blobsIDs := sop.BlobsPayload[sop.UUID]{
			BlobTable: t.updatedNodeHandles[i].BlobTable,
			Blobs:     make([]sop.UUID, len(t.updatedNodeHandles[i].IDs)),
		}
		for ii := range t.updatedNodeHandles[i].IDs {
			// Since we've flipped the inactive to active, the new inactive ID is to be flushed out of Redis cache.
			blobsIDs.Blobs[ii] = t.updatedNodeHandles[i].IDs[ii].GetInActiveID()
		}
		unusedNodeIDs = append(unusedNodeIDs, blobsIDs)
	}

	// Package the logically deleted IDs for actual physical deletes.
	deletedIDs := make([]sop.RegistryPayload[sop.UUID], len(t.removedNodeHandles))
	for i := range t.removedNodeHandles {
		deletedIDs[i].RegistryTable = t.removedNodeHandles[i].RegistryTable
		deletedIDs[i].IDs = make([]sop.UUID, len(t.removedNodeHandles[i].IDs))
		blobsIDs := sop.BlobsPayload[sop.UUID]{
			BlobTable: t.removedNodeHandles[i].BlobTable,
			Blobs:     make([]sop.UUID, len(t.removedNodeHandles[i].IDs)),
		}
		for ii := range t.removedNodeHandles[i].IDs {
			// Removed nodes are marked deleted, thus, its active node ID can be safely removed.
			deletedIDs[i].IDs[ii] = t.removedNodeHandles[i].IDs[ii].LogicalID
			blobsIDs.Blobs[ii] = t.removedNodeHandles[i].IDs[ii].GetActiveID()
		}
		unusedNodeIDs = append(unusedNodeIDs, blobsIDs)
	}

	return sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{
		First:  deletedIDs,
		Second: unusedNodeIDs,
	}
}

func (t *Transaction) rollback(ctx context.Context, rollbackTrackedItemsValues bool) error {
	var lastErr error

	// Rollback pre commit logged items.
	if t.logger.committedState == addActivelyPersistedItem {
		itemsForDelete := t.getForRollbackTrackedItemsValues()
		if err := t.deleteTrackedItemsValues(ctx, itemsForDelete); err != nil {
			lastErr = err
		}
		// Transaction got rolled back, no need for the logs.
		t.logger.removeLogs(ctx)
		// Rewind the transaction log in case retry will check it.
		t.logger.log(ctx, unknown, nil)
		return lastErr
	}

	// Rollback on commit logged items.
	if t.logger.committedState > finalizeCommit {
		// This state should not be reached and rollback invoked, but return an error about it, in case.
		return fmt.Errorf("transaction got committed, 'can't rollback it")
	}

	if t.logger.committedState >= beforeFinalize {
		// We just need to remove the priority log file in live rollback.
		if err := t.logger.PriorityLog().Remove(ctx, t.GetID()); err != nil {
			lastErr = err
		}
	}

	updatedNodes, removedNodes, addedNodes, _, rootNodes := t.classifyModifiedNodes()

	if t.logger.committedState > commitStoreInfo {
		rollbackStoresInfo := t.getRollbackStoresInfo()
		if _, err := t.StoreRepository.Update(ctx, rollbackStoresInfo); err != nil {
			lastErr = err
		}
	}
	if t.logger.committedState > commitAddedNodes {
		bibs := convertToBlobRequestPayload(addedNodes)
		vids := convertToRegistryRequestPayload(addedNodes)
		bv := sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: vids, Second: bibs}
		if err := t.btreesBackend[0].nodeRepository.rollbackAddedNodes(ctx, bv); err != nil {
			lastErr = err
		}
	}
	if t.logger.committedState > commitRemovedNodes {
		vids := convertToRegistryRequestPayload(removedNodes)
		if err := t.btreesBackend[0].nodeRepository.rollbackRemovedNodes(ctx, t.areNodesKeysLocked(), vids); err != nil {
			lastErr = err
		}
	}
	if t.logger.committedState > commitUpdatedNodes {
		vids := convertToRegistryRequestPayload(updatedNodes)
		if err := t.btreesBackend[0].nodeRepository.rollbackUpdatedNodes(ctx, t.areNodesKeysLocked(), vids); err != nil {
			lastErr = err
		}
	}
	// Safe to release the nodes keys' locks so other(s) waiting can get served.
	t.unlockNodesKeys(ctx)
	if t.logger.committedState > commitNewRootNodes {
		bibs := convertToBlobRequestPayload(rootNodes)
		vids := convertToRegistryRequestPayload(rootNodes)
		bv := sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: vids, Second: bibs}
		if err := t.btreesBackend[0].nodeRepository.rollbackNewRootNodes(ctx, bv); err != nil {
			lastErr = err
		}
	}
	if rollbackTrackedItemsValues && t.logger.committedState >= commitTrackedItemsValues {
		itemsForDelete := t.getForRollbackTrackedItemsValues()
		if err := t.deleteTrackedItemsValues(ctx, itemsForDelete); err != nil {
			lastErr = err
		}
	}
	if t.logger.committedState >= lockTrackedItems {
		if err := t.unlockTrackedItems(ctx); err != nil {
			lastErr = err
		}
	}
	// Transaction got rolled back, no need for the logs.
	t.logger.removeLogs(ctx)
	// Rewind the transaction log in case retry will check it.
	t.logger.log(ctx, unknown, nil)

	return lastErr
}

func (t *Transaction) commitTrackedItemsValues(ctx context.Context) error {
	for i := range t.btreesBackend {
		if err := t.btreesBackend[i].commitTrackedItemsValues(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (t *Transaction) getForRollbackTrackedItemsValues() []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]] {
	r := make([]sop.Tuple[bool, sop.BlobsPayload[sop.UUID]], 0, 5)
	for i := range t.btreesBackend {
		itemsForDelete := t.btreesBackend[i].getForRollbackTrackedItemsValues()
		if itemsForDelete != nil && len(itemsForDelete.Blobs) > 0 {
			r = append(r, sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]{
				First:  t.btreesBackend[i].getStoreInfo().IsValueDataGloballyCached,
				Second: *itemsForDelete,
			})
		}
	}
	return r
}

func (t *Transaction) getObsoleteTrackedItemsValues() []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]] {
	r := make([]sop.Tuple[bool, sop.BlobsPayload[sop.UUID]], 0, 5)
	for i := range t.btreesBackend {
		itemsForDelete := t.btreesBackend[i].getObsoleteTrackedItemsValues()
		if itemsForDelete != nil && len(itemsForDelete.Blobs) > 0 {
			r = append(r, sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]{
				First:  t.btreesBackend[i].getStoreInfo().IsValueDataGloballyCached,
				Second: *itemsForDelete,
			})
		}
	}
	return r
}

func (t *Transaction) deleteTrackedItemsValues(ctx context.Context, itemsForDelete []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]) error {
	var lastErr error
	for i := range itemsForDelete {
		// First field of the Tuple specifies whether we need to delete from Redis cache the blob IDs specified in Second.
		if itemsForDelete[i].First {
			for ii := range itemsForDelete[i].Second.Blobs {
				if _, err := t.l2Cache.Delete(ctx, []string{formatItemKey(itemsForDelete[i].Second.Blobs[ii].String())}); err != nil {
					lastErr = err
				}
			}
		}
		if err := t.blobStore.Remove(ctx, []sop.BlobsPayload[sop.UUID]{itemsForDelete[i].Second}); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Checks if fetched items are intact.
func (t *Transaction) commitForReaderTransaction(ctx context.Context) error {
	if t.mode == sop.ForWriting {
		return nil
	}
	if !t.hasTrackedItems() {
		return nil
	}
	// For a reader transaction, conflict check is enough.
	startTime := sop.Now()
	for {
		log.Debug(fmt.Sprintf("inside reader trans phase 2 commit, tid: %v", t.GetID()))

		if err := t.timedOut(ctx, startTime); err != nil {
			return err
		}
		// Check items if have not changed since fetching.
		_, _, _, fetchedNodes, _ := t.classifyModifiedNodes()
		if ok, err := t.btreesBackend[0].nodeRepository.areFetchedItemsIntact(ctx, fetchedNodes); err != nil {
			return err
		} else if ok {
			log.Debug(fmt.Sprintf("reader trans phase 2 commit succeeded, tid: %v", t.GetID()))
			return nil
		}

		sop.RandomSleep(ctx)

		log.Debug(fmt.Sprintf("reader trans phase 2 commit before 'refetchAndMergeModifications', tid: %v", t.GetID()))

		// Recreate the fetches on latest committed nodes & check if fetched Items are unchanged.
		if err := t.refetchAndMergeModifications(ctx); err != nil {
			return err
		}
	}
}

// Use tracked Items to refetch their Nodes(using B-Tree) and merge the changes in, if there is no conflict.
func (t *Transaction) refetchAndMergeModifications(ctx context.Context) error {
	log.Debug("same node(s) are being modified elsewhere, 'will refetch and re-merge changes in...")
	for i := range t.btreesBackend {
		if err := t.btreesBackend[i].refetchAndMerge(ctx); err != nil {
			return err
		}
	}
	return nil
}

// classifyModifiedNodes will classify modified Nodes into 5 kinds & return them:
// a. updated Nodes, b. removed Nodes, c. added Nodes, d. fetched Nodes, e. root Nodes.
func (t *Transaction) classifyModifiedNodes() ([]sop.Tuple[*sop.StoreInfo, []interface{}],
	[]sop.Tuple[*sop.StoreInfo, []interface{}],
	[]sop.Tuple[*sop.StoreInfo, []interface{}],
	[]sop.Tuple[*sop.StoreInfo, []interface{}],
	[]sop.Tuple[*sop.StoreInfo, []interface{}]) {
	var storesUpdatedNodes, storesRemovedNodes, storesAddedNodes, storesFetchedNodes, storesRootNodes []sop.Tuple[*sop.StoreInfo, []interface{}]
	for i, s := range t.btreesBackend {
		var updatedNodes, removedNodes, addedNodes, fetchedNodes, rootNodes []interface{}
		for _, cacheNode := range s.nodeRepository.localCache {
			// Allow newly created root nodes to get merged between transactions.
			if s.nodeRepository.count == 0 &&
				cacheNode.action == addAction && t.btreesBackend[i].getStoreInfo().RootNodeID == cacheNode.node.(btree.MetaDataType).GetID() {
				rootNodes = append(rootNodes, cacheNode.node)
				continue
			}
			switch cacheNode.action {
			case updateAction:
				updatedNodes = append(updatedNodes, cacheNode.node)
			case removeAction:
				removedNodes = append(removedNodes, cacheNode.node)
			case addAction:
				addedNodes = append(addedNodes, cacheNode.node)
			case getAction:
				fetchedNodes = append(fetchedNodes, cacheNode.node)
			}
		}
		if len(updatedNodes) > 0 {
			storesUpdatedNodes = append(storesUpdatedNodes, sop.Tuple[*sop.StoreInfo, []interface{}]{
				First:  s.getStoreInfo(),
				Second: updatedNodes,
			})
		}
		if len(removedNodes) > 0 {
			storesRemovedNodes = append(storesRemovedNodes, sop.Tuple[*sop.StoreInfo, []interface{}]{
				First:  s.getStoreInfo(),
				Second: removedNodes,
			})
		}
		if len(addedNodes) > 0 {
			storesAddedNodes = append(storesAddedNodes, sop.Tuple[*sop.StoreInfo, []interface{}]{
				First:  s.getStoreInfo(),
				Second: addedNodes,
			})
		}
		if len(fetchedNodes) > 0 {
			storesFetchedNodes = append(storesFetchedNodes, sop.Tuple[*sop.StoreInfo, []interface{}]{
				First:  s.getStoreInfo(),
				Second: fetchedNodes,
			})
		}
		if len(rootNodes) > 0 {
			storesRootNodes = append(storesRootNodes, sop.Tuple[*sop.StoreInfo, []interface{}]{
				First:  s.getStoreInfo(),
				Second: rootNodes,
			})
		}
	}
	return storesUpdatedNodes, storesRemovedNodes, storesAddedNodes, storesFetchedNodes, storesRootNodes
}

func (t *Transaction) commitStores(ctx context.Context) ([]sop.StoreInfo, error) {
	stores := t.getCommitStoresInfo()
	return t.StoreRepository.Update(ctx, stores)
}

func (t *Transaction) getCommitStoresInfo() []sop.StoreInfo {
	stores := make([]sop.StoreInfo, len(t.btreesBackend))
	for i := range t.btreesBackend {
		store := t.btreesBackend[i].getStoreInfo()
		s2 := *store
		// Compute the count delta so Store Repository can reconcile for commit.
		s2.CountDelta = s2.Count - t.btreesBackend[i].nodeRepository.count
		s2.Timestamp = sop.Now().UnixMilli()
		stores[i] = s2
	}
	return stores
}

func (t *Transaction) getRollbackStoresInfo() []sop.StoreInfo {
	stores := make([]sop.StoreInfo, len(t.btreesBackend))
	for i := range t.btreesBackend {
		store := t.btreesBackend[i].getStoreInfo()
		s2 := *store
		// Compute the count delta so Store Repository can reconcile for rollback.
		s2.CountDelta = t.btreesBackend[i].nodeRepository.count - s2.Count
		stores[i] = s2
	}
	return stores
}

func (t *Transaction) hasTrackedItems() bool {
	for _, s := range t.btreesBackend {
		if s.hasTrackedItems() {
			return true
		}
	}
	return false
}

// Check Tracked items for conflict, this pass is to remove any race condition.
func (t *Transaction) checkTrackedItems(ctx context.Context) error {
	for _, s := range t.btreesBackend {
		if err := s.checkTrackedItems(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (t *Transaction) lockTrackedItems(ctx context.Context) error {
	for _, s := range t.btreesBackend {
		if err := s.lockTrackedItems(ctx, t.maxTime); err != nil {
			return err
		}
	}
	return nil
}

func (t *Transaction) unlockTrackedItems(ctx context.Context) error {
	var lastErr error
	for _, s := range t.btreesBackend {
		if err := s.unlockTrackedItems(ctx); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Delete the registry entries and unused node blobs.
func (t *Transaction) deleteObsoleteEntries(ctx context.Context,
	deletedRegistryIDs []sop.RegistryPayload[sop.UUID], unusedNodeIDs []sop.BlobsPayload[sop.UUID]) error {
	var lastErr error
	if len(unusedNodeIDs) > 0 {
		// Delete from Redis & BlobStore the unused/inactive nodes.
		for i := range unusedNodeIDs {
			if _, err := t.l1Cache.DeleteNodes(ctx, unusedNodeIDs[i].Blobs); err != nil {
				lastErr = err
				log.Warn(fmt.Sprintf("Redis delete failed, details: %v", err))
			}
		}
		if err := t.blobStore.Remove(ctx, unusedNodeIDs); err != nil {
			lastErr = err
		}
		// End of block.
	}
	// Delete from registry the deleted Registry IDs (it manages redis cache internally).
	if err := t.registry.Remove(ctx, deletedRegistryIDs); err != nil {
		lastErr = err
	}
	return lastErr
}

func (t *Transaction) timedOut(ctx context.Context, startTime time.Time) error {
	return sop.TimedOut(ctx, "transaction", startTime, t.maxTime)
}

func (t *Transaction) unlockNodesKeys(ctx context.Context) error {
	if t.nodesKeys == nil {
		return nil
	}
	err := t.l2Cache.Unlock(ctx, t.nodesKeys)
	t.nodesKeys = nil
	return err
}

func (t *Transaction) areNodesKeysLocked() bool {
	return t.nodesKeys != nil
}

func (t *Transaction) mergeNodesKeys(ctx context.Context, updatedNodes []sop.Tuple[*sop.StoreInfo, []any], removedNodes []sop.Tuple[*sop.StoreInfo, []any]) {
	// Create lock keys so we can lock updated & removed handles then unlock them later when locks no longer needed.
	// Keys are sorted by UUID as high, low int64 bit pair so we can order the cache lock call in a uniform manner and thus, reduce risk of dead lock.
	if len(updatedNodes) == 0 && len(removedNodes) == 0 {
		// Release the held lock for a node key that we no longer care about.
		t.l2Cache.Unlock(ctx, t.nodesKeys)
		t.nodesKeys = nil
		return
	}

	lids := extractUUIDs(updatedNodes)
	rids := extractUUIDs(removedNodes)
	log.Debug(fmt.Sprintf("mergeNodesKeys: updated lids: %v, removed lids: %v", lids, rids))

	lookupByUUID := inmemory.NewBtree[sop.UUID, *sop.LockKey](true)
	for _, id := range lids {
		lookupByUUID.Add(id, t.l2Cache.CreateLockKeys([]string{id.String()})[0])
	}
	for _, id := range rids {
		lookupByUUID.Add(id, t.l2Cache.CreateLockKeys([]string{id.String()})[0])
	}

	lookupByKeyName := make(map[string]sop.UUID, lookupByUUID.Count())
	lookupByUUID.First()
	for {
		v := lookupByUUID.GetCurrentValue()
		lookupByKeyName[v.Key] = lookupByUUID.GetCurrentKey()
		if !lookupByUUID.Next() {
			break
		}
	}

	for _, nk := range t.nodesKeys {
		if v, ok := lookupByKeyName[nk.Key]; ok {
			lookupByUUID.Update(v, nk)
			continue
		} else {
			// Release the held lock for a node key that we no longer care about.
			t.l2Cache.Unlock(ctx, []*sop.LockKey{nk})
		}
	}

	// Map into an array of LockKeys sorted by UUID high, low int64 bit values.
	lookupByUUID.First()
	keys := make([]*sop.LockKey, 0, lookupByUUID.Count())
	for {
		keys = append(keys, lookupByUUID.GetCurrentValue())
		if !lookupByUUID.Next() {
			break
		}
	}

	t.nodesKeys = keys
}

var lastOnIdleRunTime int64
var locker = sync.Mutex{}

var lastPriorityOnIdleTime int64
var prioritylocker = sync.Mutex{}
var priorityLogFound bool

func (t *Transaction) onIdle(ctx context.Context) {
	// Required to have a backend btree to do cleanup.
	if len(t.btreesBackend) == 0 {
		return
	}

	// Allow only one priority rollback processor.
	// Check every 2.5 minutes if there are any pending rollbacks that "aged" (5min or older).
	interval := 150
	if priorityLogFound {
		interval = 5
	}
	nextRunTime := sop.Now().Add(time.Duration(-interval) * time.Second).UnixMilli()
	if t.logger.PriorityLog().IsEnabled() && lastPriorityOnIdleTime < nextRunTime {
		runTime := false
		prioritylocker.Lock()
		if lastPriorityOnIdleTime < nextRunTime {
			lastPriorityOnIdleTime = sop.Now().UnixMilli()
			runTime = true
		}
		prioritylocker.Unlock()
		if runTime {
			if found, err := t.logger.doPriorityRollbacks(ctx, t); err != nil {
				// Trigger a failover if a handler is registered; otherwise, just log path state.
				if t.HandleReplicationRelatedError != nil {
					t.HandleReplicationRelatedError(ctx, err, nil, true)
				}
				priorityLogFound = false
			} else {
				priorityLogFound = found
			}
		}
	}

	// If it is known that there is nothing to clean up then do 4hr interval polling,
	// otherwise do shorter interval of 5 minutes, to allow faster cleanup.
	// Having "abandoned" commit is a very rare occurrence.
	interval = 4 * 60
	if hourBeingProcessed != "" {
		interval = 5
	}
	nextRunTime = sop.Now().Add(time.Duration(-interval) * time.Minute).UnixMilli()
	if lastOnIdleRunTime < nextRunTime {
		runTime := false
		locker.Lock()
		if lastOnIdleRunTime < nextRunTime {
			lastOnIdleRunTime = sop.Now().UnixMilli()
			runTime = true
		}
		locker.Unlock()
		if runTime {
			t.logger.processExpiredTransactionLogs(ctx, t)
		}
	}
}
