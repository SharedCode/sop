package common

import (
	"context"
	"fmt"
	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/encoding"
	"github.com/sharedcode/sop/redis"
)

// Backend facing Node Repository. Part of where the magic happens.

type cachedNode struct {
	// node is a pointer to btree.Node.
	node   interface{}
	action actionType
}

// nodeRepositoryBackend implementation exposes a standard NodeRepository interface
// that manages b-tree nodes in transaction cache, Redis and File System.
type nodeRepositoryBackend struct {
	transaction *Transaction
	// MRU cache of read but not operated on nodes.
	readNodesCache cache.Cache[sop.UUID, any]
	// cache of all nodes that were operated on, i.e. - created, updated, explitly fetched, removed nodes (and/or its items).
	localCache map[sop.UUID]cachedNode
	// L2 Cache, e.g. - Redis. Used here primarily to allow nodes merging in L2. I.e. - capability to sense
	// changes across different transactions on same or different machines and merge their changes in during transaction commits.
	l2Cache sop.Cache
	// L1 Cache is a virtualized in-memory & L2. Used as a global MRU cache of all
	// B-trees (across transactions) running in this host computer.
	l1Cache   *cache.L1Cache
	storeInfo *sop.StoreInfo
	count     int64
}

const (
	readNodesMruMinCapacity = 8
	readNodesMruMaxCapacity = 12
)

// NewNodeRepository instantiates a NodeRepository.
func newNodeRepository[TK btree.Ordered, TV any](t *Transaction, storeInfo *sop.StoreInfo) *nodeRepositoryFrontEnd[TK, TV] {
	nr := &nodeRepositoryBackend{
		transaction:    t,
		storeInfo:      storeInfo,
		readNodesCache: cache.NewCache[sop.UUID, any](readNodesMruMinCapacity, readNodesMruMaxCapacity),
		localCache:     make(map[sop.UUID]cachedNode),
		l2Cache:        redis.NewClient(),
		l1Cache:        cache.GetGlobalCache(),
		count:          storeInfo.Count,
	}
	return &nodeRepositoryFrontEnd[TK, TV]{
		nodeRepositoryBackend: nr,
	}
}

// Transaction "session" logic(in NodeRepository):
// Get or Fetch:
// - If not found locally(& no remove marker) & found in blobStore, fetch data & populate local cache(& redis).
//   Return not found if found locally & there is a remove marker on it.
// Add:
// - Add to local cache if not yet, for add to blobStore(& redis) on transaction commit.
//   Mark data as new.
// Update:
// - Update to local cache if not yet, for update to blobStore(& redis) on transaction commit.
//   Mark data as modified if not new.
// Remove:
// - If data is new(found in local cache only), then just remove from local cache.
// - Otherwise, mark data as removed, for actual remove from blobStore(& redis) on transaction commit.

// Get will retrieve a node with nodeID from the map.
func (nr *nodeRepositoryBackend) get(ctx context.Context, logicalID sop.UUID, target interface{}) (interface{}, error) {
	if v, ok := nr.localCache[logicalID]; ok {
		if v.action == removeAction {
			return nil, nil
		}
		return v.node, nil
	}
	if v := nr.readNodesCache.Get([]sop.UUID{logicalID}); v[0] != nil {
		return v[0], nil
	}

	// Try to fetch node from L1 cache Nodes MRU prior to "commit" time. On commit time (transaction.phaseDone > 0),
	// we only fetch the Handle from L2 cache to get the "true" record.
	if nr.transaction.phaseDone == 0 {
		if h := nr.l1Cache.Handles.Get([]sop.UUID{logicalID}); len(h) == 1 && !h[0].IsEmpty() {
			if n := nr.l1Cache.GetNodeFromMRU(h[0], target); n != nil {
				target = n
				nr.readNodesCache.Set([]sop.KeyValuePair[sop.UUID, any]{{
					Key: logicalID, Value: target,
				}})
				return target, nil
			}
		}
	}

	h, err := nr.transaction.registry.Get(ctx, []sop.RegistryPayload[sop.UUID]{{
		RegistryTable: nr.storeInfo.RegistryTable,
		CacheDuration: nr.storeInfo.CacheConfig.RegistryCacheDuration,
		IsCacheTTL:    nr.storeInfo.CacheConfig.IsRegistryCacheTTL,
		IDs:           []sop.UUID{logicalID},
	}})
	if err != nil {
		return nil, err
	}
	if len(h) == 0 || len(h[0].IDs) == 0 {
		return nil, nil
	}

	nodeID := h[0].IDs[0].GetActiveID()
	// Fetch node from MRU if it is there.
	var n any
	n, err = nr.l1Cache.GetNode(ctx, h[0].IDs[0], target, nr.storeInfo.CacheConfig.IsNodeCacheTTL, nr.storeInfo.CacheConfig.NodeCacheDuration)
	if err != nil {
		return nil, err
	}
	if n != nil {
		target = n
		target.(btree.MetaDataType).SetVersion(h[0].IDs[0].Version)
		nr.readNodesCache.Set([]sop.KeyValuePair[sop.UUID, any]{{
			Key: logicalID, Value: target,
		}})
		return target, nil
	}

	// Fetch from blobStore and cache to Redis/local.
	var ba []byte
	if ba, err = nr.transaction.blobStore.GetOne(ctx, nr.storeInfo.BlobTable, nodeID); err != nil {
		return nil, err
	}
	encoding.BlobMarshaler.Unmarshal(ba, target)
	target.(btree.MetaDataType).SetVersion(h[0].IDs[0].Version)

	// Put to cache layer this node since it got fetched from blob store.
	nr.l1Cache.SetNode(ctx, nodeID, target, nr.storeInfo.CacheConfig.NodeCacheDuration)
	nr.readNodesCache.Set([]sop.KeyValuePair[sop.UUID, any]{{
		Key: logicalID, Value: target,
	}})
	return target, nil
}

func (nr *nodeRepositoryBackend) add(nodeID sop.UUID, node interface{}) {
	nr.localCache[nodeID] = cachedNode{
		action: addAction,
		node:   node,
	}
}

func (nr *nodeRepositoryBackend) update(nodeID sop.UUID, node interface{}) {
	if n := nr.readNodesCache.Get([]sop.UUID{nodeID}); n[0] != nil {
		nr.localCache[nodeID] = cachedNode{
			action: defaultAction,
			node:   n[0],
		}
		nr.readNodesCache.Delete([]sop.UUID{nodeID})
	}
	if v, ok := nr.localCache[nodeID]; ok {
		// Update the node and keep the "action" marker if new, otherwise update to "update" action.
		v.node = node
		if v.action != addAction {
			v.action = updateAction
		}
		nr.localCache[nodeID] = v
		return
	}
	// Treat as add if not in local cache, because it should be there unless node is new.
	nr.localCache[nodeID] = cachedNode{
		action: addAction,
		node:   node,
	}
}

func (nr *nodeRepositoryBackend) remove(nodeID sop.UUID) {
	if n := nr.readNodesCache.Get([]sop.UUID{nodeID}); n[0] != nil {
		nr.localCache[nodeID] = cachedNode{
			action: defaultAction,
			node:   n[0],
		}
		nr.readNodesCache.Delete([]sop.UUID{nodeID})
	}
	if v, ok := nr.localCache[nodeID]; ok {
		if v.action == addAction {
			delete(nr.localCache, nodeID)
			return
		}
		v.action = removeAction
		nr.localCache[nodeID] = v
	}
	// Code should not reach this point, as B-tree will not issue a remove if node is not cached locally.
}

func (nr *nodeRepositoryBackend) commitNewRootNodes(ctx context.Context, nodes []sop.Tuple[*sop.StoreInfo, []interface{}]) (bool, []sop.RegistryPayload[sop.Handle], error) {
	if len(nodes) == 0 {
		return true, nil, nil
	}
	vids := convertToRegistryRequestPayload(nodes)
	handles, err := nr.transaction.registry.Get(ctx, vids)
	if err != nil {
		return false, nil, err
	}
	blobs := make([]sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]], len(nodes))
	for i := range handles {
		if len(handles[i].IDs) == 0 {
			handles[i].IDs = make([]sop.Handle, len(vids[i].IDs))
		}
		blobs[i].Blobs = make([]sop.KeyValuePair[sop.UUID, []byte], len(handles[i].IDs))
		blobs[i].BlobTable = nodes[i].First.BlobTable
		for ii := range handles[i].IDs {
			// Check if a non-empty root node was found, fail to cause "re-sync & merge".
			if !handles[i].IDs[ii].LogicalID.IsNil() {
				return false, nil, nil
			}
			handles[i].IDs[ii] = sop.NewHandle(vids[i].IDs[ii])
			blobs[i].Blobs[ii].Key = handles[i].IDs[ii].GetActiveID()
			ba, err := encoding.BlobMarshaler.Marshal(nodes[i].Second[ii])
			if err != nil {
				return false, nil, err
			}
			blobs[i].Blobs[ii].Value = ba
		}
	}
	// Persist the nodes blobs to blob store and redis cache.
	if err := nr.transaction.blobStore.Add(ctx, blobs); err != nil {
		return false, nil, err
	}
	for i := range nodes {
		for ii := range nodes[i].Second {
			if err := nr.transaction.l2Cache.SetStruct(ctx, nr.formatKey(handles[i].IDs[ii].GetActiveID().String()),
				nodes[i].Second[ii], nodes[i].First.CacheConfig.NodeCacheDuration); err != nil {
				// Tolerate Redis error, log as Warning.
				log.Warn(fmt.Sprintf("commitNewRootNodes failed redisCache.SetStruct, details: %v", err))
			}
		}
	}
	// Add virtual IDs to registry.
	if err := nr.transaction.registry.Add(ctx, handles); err != nil {
		return false, nil, err
	}
	return true, handles, nil
}

// Save to blob store, save node ID to the alternate(inactive) physical ID(see virtual ID).
func (nr *nodeRepositoryBackend) commitUpdatedNodes(ctx context.Context, nodes []sop.Tuple[*sop.StoreInfo, []interface{}]) (bool, []sop.RegistryPayload[sop.Handle], error) {
	if len(nodes) == 0 {
		return true, nil, nil
	}
	// 1st pass, update the virtual ID registry ensuring the set of nodes are only being modified by us.
	vids := convertToRegistryRequestPayload(nodes)
	handles, err := nr.transaction.registry.Get(ctx, vids)
	if err != nil {
		return false, nil, err
	}

	blobs := make([]sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]], len(nodes))
	for i := range handles {
		blobs[i].BlobTable = nodes[i].First.BlobTable
		blobs[i].Blobs = make([]sop.KeyValuePair[sop.UUID, []byte], len(handles[i].IDs))
		for ii := range handles[i].IDs {
			log.Debug(fmt.Sprintf("inside commitUpdatedNodes(%d:%d) forloop blobTable %s UUID %s trying to AllocateID", i, ii, blobs[i].BlobTable, handles[i].IDs[ii].LogicalID.String()))
			// Node with such ID is marked deleted or had been updated since reading it.
			if (handles[i].IDs[ii].IsDeleted && !handles[i].IDs[ii].IsExpiredInactive()) || handles[i].IDs[ii].Version != nodes[i].Second[ii].(btree.MetaDataType).GetVersion() {
				log.Debug(fmt.Sprintf("inside commitUpdatedNodes(%d:%d), exiting, ID marked deleted or was updated since read", i, ii))
				return false, nil, nil
			}
			if handles[i].IDs[ii].IsDeleted && handles[i].IDs[ii].IsExpiredInactive() {
				// In case the handle was marked deleted by an incomplete transaction then reset it back to undo it.
				handles[i].IDs[ii].IsDeleted = false
			}
			// Create new phys. UUID and auto-assign it to the available phys. ID(A or B) "ID slot".
			id := handles[i].IDs[ii].AllocateID()
			if id == sop.NilUUID {
				if handles[i].IDs[ii].IsExpiredInactive() {
					handles[i].IDs[ii].ClearInactiveID()
					// Allocate a new ID after clearing the unused inactive ID.
					id = handles[i].IDs[ii].AllocateID()
				}
			}
			if id == sop.NilUUID {
				// Return false as there is an ongoing update on node by another transaction.
				log.Debug(fmt.Sprintf("inside commitUpdatedNodes(%d:%d), exiting, as another transaction has ongoing update", i, ii))
				return false, nil, nil
			}
			blobs[i].Blobs[ii].Key = id
			ba, err := encoding.BlobMarshaler.Marshal(nodes[i].Second[ii])
			if err != nil {
				return false, nil, err
			}
			blobs[i].Blobs[ii].Value = ba
		}
	}
	log.Debug("outside commitUpdatedNodes forloop trying to AllocateID")

	if err := nr.transaction.registry.UpdateNoLocks(ctx, false, handles); err != nil {
		log.Debug(fmt.Sprintf("commitUpdatedNodes failed registry.Update, details: %v", err))
		return false, nil, err
	}

	// 2nd pass, persist the nodes blobs to blob store and redis cache.
	if err := nr.transaction.blobStore.Add(ctx, blobs); err != nil {
		log.Debug(fmt.Sprintf("commitUpdatedNodes failed blobStore.Add, details: %v", err))
		return false, nil, err
	}
	for i := range nodes {
		for ii := range nodes[i].Second {
			if err := nr.transaction.l2Cache.SetStruct(ctx, nr.formatKey(handles[i].IDs[ii].GetInActiveID().String()), nodes[i].Second[ii], nodes[i].First.CacheConfig.NodeCacheDuration); err != nil {
				log.Warn(fmt.Sprintf("commitUpdatedNodes failed redisCache.SetStruct, details: %v", err))
			}
		}
	}
	return true, handles, nil
}

// Add the removed Node(s) and their Item(s) Data(if not in node segment) to the recycler
// so they can get serviced for physical delete on schedule in the future.
func (nr *nodeRepositoryBackend) commitRemovedNodes(ctx context.Context, nodes []sop.Tuple[*sop.StoreInfo, []interface{}]) (bool, []sop.RegistryPayload[sop.Handle], error) {
	if len(nodes) == 0 {
		return true, nil, nil
	}
	vids := convertToRegistryRequestPayload(nodes)
	handles, err := nr.transaction.registry.Get(ctx, vids)
	if err != nil {
		return false, nil, err
	}
	rightNow := sop.Now().UnixMilli()
	for i := range handles {
		for ii := range handles[i].IDs {
			// Node with such ID is already marked deleted, is in-flight change or had been updated since reading it,
			// fail it for "refetch" & retry.
			if handles[i].IDs[ii].IsDeleted || handles[i].IDs[ii].Version != nodes[i].Second[ii].(btree.MetaDataType).GetVersion() {
				return false, nil, nil
			}
			// Mark ID as deleted.
			handles[i].IDs[ii].IsDeleted = true
			handles[i].IDs[ii].WorkInProgressTimestamp = rightNow
		}
	}
	// Persist the handles changes.
	if err := nr.transaction.registry.UpdateNoLocks(ctx, false, handles); err != nil {
		return false, nil, err
	}
	return true, handles, nil
}

func (nr *nodeRepositoryBackend) commitAddedNodes(ctx context.Context, nodes []sop.Tuple[*sop.StoreInfo, []interface{}]) ([]sop.RegistryPayload[sop.Handle], error) {
	/* UUID to Virtual ID story:
	   - (on commit) New(added) nodes will have their IDs converted to virtual ID with empty
	     phys IDs(or same ID with active & virtual ID).
	   - On get, 'will read the Node using currently active ID.
	   - (on commit) On update, 'will save and register the node phys ID to the "inactive ID" part of the virtual ID.
	   - On finalization of commit, inactive will be switched to active (node) IDs.
	*/
	if len(nodes) == 0 {
		return nil, nil
	}
	handles := make([]sop.RegistryPayload[sop.Handle], len(nodes))
	blobs := make([]sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]], len(nodes))
	for i := range nodes {
		handles[i].RegistryTable = nodes[i].First.RegistryTable
		handles[i].IDs = make([]sop.Handle, len(nodes[i].Second))
		handles[i].CacheDuration = nodes[i].First.CacheConfig.RegistryCacheDuration
		blobs[i].BlobTable = nodes[i].First.BlobTable
		blobs[i].Blobs = make([]sop.KeyValuePair[sop.UUID, []byte], len(handles[i].IDs))
		for ii := range nodes[i].Second {
			metaData := nodes[i].Second[ii].(btree.MetaDataType)
			// Add node to blob store.
			h := sop.NewHandle(metaData.GetID())
			// Increment version.
			h.Version++
			blobs[i].Blobs[ii].Key = metaData.GetID()
			ba, err := encoding.BlobMarshaler.Marshal(nodes[i].Second[ii])
			if err != nil {
				return nil, err
			}
			blobs[i].Blobs[ii].Value = ba
			handles[i].IDs[ii] = h
			// Add node to Redis cache.
			if err := nr.transaction.l2Cache.SetStruct(ctx, nr.formatKey(metaData.GetID().String()), nodes[i].Second[ii], nodes[i].First.CacheConfig.NodeCacheDuration); err != nil {
				log.Warn(fmt.Sprintf("commitAddedNodes failed redisCache.SetStruct, details: %v", err))
			}
		}
	}
	// Register virtual IDs(a.k.a. handles).
	if err := nr.transaction.registry.Add(ctx, handles); err != nil {
		return nil, err
	}
	// Add nodes to blob store.
	if err := nr.transaction.blobStore.Add(ctx, blobs); err != nil {
		return nil, err
	}
	return handles, nil
}

func (nr *nodeRepositoryBackend) areFetchedItemsIntact(ctx context.Context, nodes []sop.Tuple[*sop.StoreInfo, []interface{}]) (bool, error) {
	if len(nodes) == 0 {
		return true, nil
	}
	// Check if the Items read for each fetchedNode are intact.
	vids := convertToRegistryRequestPayload(nodes)
	handles, err := nr.transaction.registry.Get(ctx, vids)
	if err != nil {
		return false, err
	}
	for i := range handles {
		for ii := range handles[i].IDs {
			// Node with ID had been updated(or deleted) since reading it.
			if handles[i].IDs[ii].Version != nodes[i].Second[ii].(btree.MetaDataType).GetVersion() {
				return false, nil
			}
		}
	}
	return true, nil
}

func (nr *nodeRepositoryBackend) rollbackNewRootNodes(ctx context.Context, rollbackData sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]) error {
	if len(rollbackData.First) == 0 {
		return nil
	}
	vids := rollbackData.First
	bibs := rollbackData.Second
	if len(vids) == 0 {
		return nil
	}

	var lastErr error
	// Undo on blob store & redis.
	if err := nr.transaction.blobStore.Remove(ctx, bibs); err != nil {
		lastErr = fmt.Errorf("unable to undo new root nodes, %v, error: %v", bibs, err)
		log.Error(lastErr.Error())
	}
	for i := range vids {
		for ii := range vids[i].IDs {
			if _, err := nr.transaction.l2Cache.Delete(ctx, []string{nr.formatKey(vids[i].IDs[ii].String())}); err != nil {
				err = fmt.Errorf("unable to undo new root nodes in redis, error: %v", err)
				if lastErr == nil {
					lastErr = err
				}
				log.Warn(err.Error())
			}
		}
	}
	// If we're able to commit roots in registry then they are "ours", we need to unregister.
	if nr.transaction.logger.committedState > commitNewRootNodes {
		if err := nr.transaction.registry.Remove(ctx, vids); err != nil {
			lastErr = fmt.Errorf("unable to undo new root nodes registration, %v, error: %v", vids, err)
			log.Error(lastErr.Error())
		}
	}
	return lastErr
}

func (nr *nodeRepositoryBackend) rollbackAddedNodes(ctx context.Context, rollbackData sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]) error {
	vids := rollbackData.First
	bibs := rollbackData.Second
	if len(vids) == 0 {
		return nil
	}
	var lastErr error
	if err := nr.transaction.blobStore.Remove(ctx, bibs); err != nil {
		lastErr = fmt.Errorf("unable to undo added nodes, %v, error: %v", bibs, err)
		log.Error(lastErr.Error())
	}
	// Unregister nodes IDs.
	if err := nr.transaction.registry.Remove(ctx, vids); err != nil {
		lastErr = fmt.Errorf("unable to undo added nodes registration, %v, error: %v", vids, err)
		log.Error(lastErr.Error())
	}
	// Remove nodes from Redis cache.
	for i := range vids {
		for ii := range vids[i].IDs {
			if _, err := nr.transaction.l2Cache.Delete(ctx, []string{nr.formatKey(vids[i].IDs[ii].String())}); err != nil {
				err = fmt.Errorf("unable to undo added nodes in redis, error: %v", err)
				if lastErr == nil {
					lastErr = err
				}
				log.Warn(err.Error())
			}
		}
	}
	return lastErr
}

// rollback updated Nodes.
func (nr *nodeRepositoryBackend) rollbackUpdatedNodes(ctx context.Context, nodesAreLocked bool, vids []sop.RegistryPayload[sop.UUID]) error {
	if len(vids) == 0 {
		return nil
	}
	handles, err := nr.transaction.registry.Get(ctx, vids)
	if err != nil {
		return err
	}
	blobsIDs := make([]sop.BlobsPayload[sop.UUID], len(vids))
	for i := range handles {
		blobsIDs[i].BlobTable = vids[i].BlobTable
		blobsIDs[i].Blobs = make([]sop.UUID, 0, len(handles[i].IDs))
		for ii := range handles[i].IDs {
			if handles[i].IDs[ii].GetInActiveID().IsNil() {
				handles[i].IDs[ii].WorkInProgressTimestamp = 0
				continue
			}
			blobsIDs[i].Blobs = append(blobsIDs[i].Blobs, handles[i].IDs[ii].GetInActiveID())
			handles[i].IDs[ii].ClearInactiveID()
		}
	}
	var lastErr error
	// Undo the nodes blobs to blob store.
	if err = nr.transaction.blobStore.Remove(ctx, blobsIDs); err != nil {
		lastErr = fmt.Errorf("unable to undo updated nodes, %v, error: %v", blobsIDs, err)
		log.Error(lastErr.Error())
	}
	// Undo changes in virtual ID registry.
	if nodesAreLocked {
		if err = nr.transaction.registry.UpdateNoLocks(ctx, false, handles); err != nil {
			lastErr = fmt.Errorf("unable to undo updated nodes registration, %v, error: %v", handles, err)
			log.Error(lastErr.Error())
		}
	} else {
		if err = nr.transaction.registry.Update(ctx, handles); err != nil {
			lastErr = fmt.Errorf("unable to undo updated nodes registration, %v, error: %v", handles, err)
			log.Error(lastErr.Error())
		}
	}
	// Undo changes in redis.
	for i := range blobsIDs {
		for ii := range blobsIDs[i].Blobs {
			if _, err = nr.transaction.l2Cache.Delete(ctx, []string{nr.formatKey(blobsIDs[i].Blobs[ii].String())}); err != nil {
				err = fmt.Errorf("unable to undo updated nodes in redis, error: %v", err)
				if lastErr == nil {
					lastErr = err
				}
				log.Warn(err.Error())
			}
		}
	}
	return lastErr
}

// Delete a list of Nodes from the Blob store & from cache. Used in "dead" (not in-flight) incomplete transactions data cleanup.
func (nr *nodeRepositoryBackend) removeNodes(ctx context.Context, blobsIDs []sop.BlobsPayload[sop.UUID]) error {
	if len(blobsIDs) == 0 {
		return nil
	}
	var lastErr error
	// Undo the nodes blobs to blob store.
	if err := nr.transaction.blobStore.Remove(ctx, blobsIDs); err != nil {
		lastErr = fmt.Errorf("unable to undo updated nodes, %v, error: %v", blobsIDs, err)
		log.Error(lastErr.Error())
	}
	// Undo changes in redis.
	for i := range blobsIDs {
		for ii := range blobsIDs[i].Blobs {
			if _, err := nr.transaction.l2Cache.Delete(ctx, []string{nr.formatKey(blobsIDs[i].Blobs[ii].String())}); err != nil {
				err = fmt.Errorf("unable to undo updated nodes in redis, error: %v", err)
				if lastErr == nil {
					lastErr = err
				}
				log.Warn(err.Error())
			}
		}
	}
	return lastErr
}

func (nr *nodeRepositoryBackend) rollbackRemovedNodes(ctx context.Context, nodesAreLocked bool, vids []sop.RegistryPayload[sop.UUID]) error {
	if len(vids) == 0 {
		return nil
	}
	handles, err := nr.transaction.registry.Get(ctx, vids)
	if err != nil {
		err = fmt.Errorf("unable to fetch removed nodes from registry, %v, error: %v", vids, err)
		log.Error(err.Error())
		return err
	}
	handlesForRollback := make([]sop.RegistryPayload[sop.Handle], len(handles))
	for i := range handles {
		handlesForRollback[i] = sop.RegistryPayload[sop.Handle]{
			RegistryTable: handles[i].RegistryTable,
			IDs:           make([]sop.Handle, 0, len(handles[i].IDs)),
		}
		for ii := range handles[i].IDs {
			// Undo the deleted mark for ID.
			if handles[i].IDs[ii].IsDeleted || handles[i].IDs[ii].WorkInProgressTimestamp > 0 {
				handles[i].IDs[ii].IsDeleted = false
				handles[i].IDs[ii].WorkInProgressTimestamp = 0
				handlesForRollback[i].IDs = append(handlesForRollback[i].IDs, handles[i].IDs[ii])
			}
		}
	}

	// Persist the handles changes.
	if nodesAreLocked {
		if err := nr.transaction.registry.UpdateNoLocks(ctx, false, handlesForRollback); err != nil {
			err = fmt.Errorf("unable to undo removed nodes in registry, %v, error: %v", handlesForRollback, err)
			log.Error(err.Error())
			return err
		}
	} else {
		if err := nr.transaction.registry.Update(ctx, handlesForRollback); err != nil {
			err = fmt.Errorf("unable to undo removed nodes in registry, %v, error: %v", handlesForRollback, err)
			log.Error(err.Error())
			return err
		}
	}
	return nil
}

// Set to active the inactive nodes.
func (nr *nodeRepositoryBackend) activateInactiveNodes(handles []sop.RegistryPayload[sop.Handle]) ([]sop.RegistryPayload[sop.Handle], error) {
	for i := range handles {
		for ii := range handles[i].IDs {
			// Set the inactive as active ID.
			handles[i].IDs[ii].FlipActiveID()
			// Increment version, we are finalizing the commit for the node.
			handles[i].IDs[ii].Version++
			// Set work in progress timestamp to now as safety. After flipping inactive to active,
			// the previously active ID if not "cleaned up" then this timestamp will allow future
			// transactions to clean it up(self healing).
			handles[i].IDs[ii].WorkInProgressTimestamp = 1
		}
	}
	// All or nothing batch update.
	return handles, nil
}

// Update upsert time of a given set of nodes.
func (nr *nodeRepositoryBackend) touchNodes(handles []sop.RegistryPayload[sop.Handle]) ([]sop.RegistryPayload[sop.Handle], error) {
	if len(handles) == 0 {
		return nil, nil
	}
	for i := range handles {
		for ii := range handles[i].IDs {
			// Update upsert time, we are finalizing the commit for the node.
			handles[i].IDs[ii].Version++
			handles[i].IDs[ii].WorkInProgressTimestamp = 0
		}
	}
	// All or nothing batch update.
	return handles, nil
}

func extractInactiveBlobsIDs(nodesHandles []sop.RegistryPayload[sop.Handle]) []sop.BlobsPayload[sop.UUID] {
	bibs := make([]sop.BlobsPayload[sop.UUID], len(nodesHandles))
	for i := range nodesHandles {
		bibs[i] = sop.BlobsPayload[sop.UUID]{
			BlobTable: nodesHandles[i].BlobTable,
			Blobs:     make([]sop.UUID, 0, len(nodesHandles[i].IDs)),
		}
		for ii := range nodesHandles[i].IDs {
			if nodesHandles[i].IDs[ii].GetInActiveID().IsNil() {
				continue
			}
			bibs[i].Blobs = append(bibs[i].Blobs, nodesHandles[i].IDs[ii].GetInActiveID())
		}
	}
	return bibs
}

func convertToBlobRequestPayload(nodes []sop.Tuple[*sop.StoreInfo, []interface{}]) []sop.BlobsPayload[sop.UUID] {
	bibs := make([]sop.BlobsPayload[sop.UUID], len(nodes))
	for i := range nodes {
		bibs[i] = sop.BlobsPayload[sop.UUID]{
			BlobTable: nodes[i].First.BlobTable,
			Blobs:     make([]sop.UUID, len(nodes[i].Second)),
		}
		for ii := range nodes[i].Second {
			bibs[i].Blobs[ii] = nodes[i].Second[ii].(btree.MetaDataType).GetID()
		}
	}
	return bibs
}

func convertToRegistryRequestPayload(nodes []sop.Tuple[*sop.StoreInfo, []interface{}]) []sop.RegistryPayload[sop.UUID] {
	vids := make([]sop.RegistryPayload[sop.UUID], len(nodes))
	for i := range nodes {
		vids[i] = sop.RegistryPayload[sop.UUID]{
			RegistryTable: nodes[i].First.RegistryTable,
			BlobTable:     nodes[i].First.BlobTable,
			CacheDuration: nodes[i].First.CacheConfig.RegistryCacheDuration,
			IsCacheTTL:    nodes[i].First.CacheConfig.IsRegistryCacheTTL,
			IDs:           make([]sop.UUID, len(nodes[i].Second)),
		}
		for ii := range nodes[i].Second {
			vids[i].IDs[ii] = nodes[i].Second[ii].(btree.MetaDataType).GetID()
		}
	}
	return vids
}

func extractUUIDs(nodes []sop.Tuple[*sop.StoreInfo, []interface{}]) []sop.UUID {
	uuids := make([]sop.UUID, 0, len(nodes))
	for i := range nodes {
		for ii := range nodes[i].Second {
			uuids = append(uuids, nodes[i].Second[ii].(btree.MetaDataType).GetID())
		}
	}
	return uuids
}

func (nr *nodeRepositoryBackend) formatKey(k string) string {
	return fmt.Sprintf("N%s", k)
}
