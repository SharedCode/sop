package in_red_ck

import (
	"context"
	"fmt"
	log "log/slog"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/redis"
)

// nodeRepository implements both frontend and backend facing methods of the NodeRepository.
// Part of where the magic happens.

type cacheNode struct {
	node   interface{}
	action actionType
}

type nodeRepositoryTyped[TK btree.Comparable, TV any] struct {
	realNodeRepository *nodeRepository
}

// Add will upsert node to the map.
func (nr *nodeRepositoryTyped[TK, TV]) Add(n *btree.Node[TK, TV]) {
	nr.realNodeRepository.add(n.ID, n)
}

// Update will upsert node to the map.
func (nr *nodeRepositoryTyped[TK, TV]) Update(n *btree.Node[TK, TV]) {
	nr.realNodeRepository.update(n.ID, n)
}

// Get will retrieve a node with nodeID from the map.
func (nr *nodeRepositoryTyped[TK, TV]) Get(ctx context.Context, nodeID sop.UUID) (*btree.Node[TK, TV], error) {
	var target btree.Node[TK, TV]
	n, err := nr.realNodeRepository.get(ctx, nodeID, &target)
	if n == nil {
		return nil, err
	}
	return n.(*btree.Node[TK, TV]), err
}

func (nr *nodeRepositoryTyped[TK, TV]) Fetched(nodeID sop.UUID) {
	c := nr.realNodeRepository.nodeLocalCache[nodeID]
	if c.action == defaultAction {
		c.action = getAction
		nr.realNodeRepository.nodeLocalCache[nodeID] = c
	}
}

// Remove will remove a node with nodeID from the map.
func (nr *nodeRepositoryTyped[TK, TV]) Remove(nodeID sop.UUID) {
	nr.realNodeRepository.remove(nodeID)
}

// nodeRepository implementation for "cassandra-S3"(in_cas_s3) exposes a standard NodeRepository interface
// but which, manages b-tree nodes in transaction cache, Redis and in Cassandra + S3,
// or File System, for debugging &/or "poor man's" setup(no AWS required!).
type nodeRepository struct {
	transaction *transaction
	// TODO: implement a MRU caching on node local cache so we only retain a handful in memory.
	nodeLocalCache map[sop.UUID]cacheNode
	storeInfo      *sop.StoreInfo
	count          int64
}

// NewNodeRepository instantiates a NodeRepository.
func newNodeRepository[TK btree.Comparable, TV any](t *transaction, storeInfo *sop.StoreInfo) *nodeRepositoryTyped[TK, TV] {
	nr := &nodeRepository{
		transaction:    t,
		nodeLocalCache: make(map[sop.UUID]cacheNode),
		storeInfo:      storeInfo,
		count:          storeInfo.Count,
	}
	return &nodeRepositoryTyped[TK, TV]{
		realNodeRepository: nr,
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
func (nr *nodeRepository) get(ctx context.Context, logicalID sop.UUID, target interface{}) (interface{}, error) {
	if v, ok := nr.nodeLocalCache[logicalID]; ok {
		if v.action == removeAction {
			return nil, nil
		}
		return v.node, nil
	}
	h, err := nr.transaction.registry.Get(ctx, sop.RegistryPayload[sop.UUID]{
		RegistryTable: nr.storeInfo.RegistryTable,
		CacheDuration: nr.storeInfo.CacheConfig.RegistryCacheDuration,
		IsCacheTTL:    nr.storeInfo.CacheConfig.IsRegistryCacheTTL,
		IDs:           []sop.UUID{logicalID},
	})
	if err != nil {
		return nil, err
	}
	if len(h) == 0 || len(h[0].IDs) == 0 {
		return nil, nil
	}
	nodeID := logicalID
	if !h[0].IDs[0].LogicalID.IsNil() {
		// Use active physical ID if in case different.
		nodeID = h[0].IDs[0].GetActiveID()
	}
	if nr.storeInfo.CacheConfig.IsNodeCacheTTL {
		err = nr.transaction.redisCache.GetStructEx(ctx, nr.formatKey(nodeID.String()), target, nr.storeInfo.CacheConfig.NodeCacheDuration)
	} else {
		err = nr.transaction.redisCache.GetStruct(ctx, nr.formatKey(nodeID.String()), target)
	}
	if err != nil {
		if !redis.KeyNotFound(err) {
			return nil, err
		}
		// Fetch from blobStore and cache to Redis/local.
		if err = nr.transaction.blobStore.GetOne(ctx, nr.storeInfo.BlobTable, nodeID, target); err != nil {
			return nil, err
		}
		target.(btree.MetaDataType).SetVersion(h[0].IDs[0].Version)
		if err := nr.transaction.redisCache.SetStruct(ctx, nr.formatKey(nodeID.String()), target, nr.storeInfo.CacheConfig.NodeCacheDuration); err != nil {
			log.Warn(fmt.Sprintf("failed to cache in Redis the newly fetched node with ID: %v, details: %v", nodeID.String(), err))
		}
		nr.nodeLocalCache[logicalID] = cacheNode{
			action: defaultAction,
			node:   target,
		}
		return target, nil
	}
	target.(btree.MetaDataType).SetVersion(h[0].IDs[0].Version)
	nr.nodeLocalCache[logicalID] = cacheNode{
		action: defaultAction,
		node:   target,
	}
	return target, nil
}

func (nr *nodeRepository) add(nodeID sop.UUID, node interface{}) {
	nr.nodeLocalCache[nodeID] = cacheNode{
		action: addAction,
		node:   node,
	}
}

func (nr *nodeRepository) update(nodeID sop.UUID, node interface{}) {
	if v, ok := nr.nodeLocalCache[nodeID]; ok {
		// Update the node and keep the "action" marker if new, otherwise update to "update" action.
		v.node = node
		if v.action != addAction {
			v.action = updateAction
		}
		nr.nodeLocalCache[nodeID] = v
		return
	}
	// Treat as add if not in local cache, because it should be there unless node is new.
	nr.nodeLocalCache[nodeID] = cacheNode{
		action: addAction,
		node:   node,
	}
}

func (nr *nodeRepository) remove(nodeID sop.UUID) {
	if v, ok := nr.nodeLocalCache[nodeID]; ok {
		if v.action == addAction {
			delete(nr.nodeLocalCache, nodeID)
			return
		}
		v.action = removeAction
		nr.nodeLocalCache[nodeID] = v
	}
	// Code should not reach this point, as B-tree will not issue a remove if node is not cached locally.
}

func (nr *nodeRepository) commitNewRootNodes(ctx context.Context, nodes []sop.Tuple[*sop.StoreInfo, []interface{}]) (bool, error) {
	if len(nodes) == 0 {
		return true, nil
	}
	vids := convertToRegistryRequestPayload(nodes)
	handles, err := nr.transaction.registry.Get(ctx, vids...)
	if err != nil {
		return false, err
	}
	blobs := make([]sop.BlobsPayload[sop.KeyValuePair[sop.UUID, interface{}]], len(nodes))
	for i := range handles {
		if len(handles[i].IDs) == 0 {
			handles[i].IDs = make([]sop.Handle, len(vids[i].IDs))
		}
		blobs[i].Blobs = make([]sop.KeyValuePair[sop.UUID, interface{}], len(handles[i].IDs))
		blobs[i].BlobTable = nodes[i].First.BlobTable
		for ii := range handles[i].IDs {
			// Check if a non-empty root node was found, fail to cause "re-sync & merge".
			if !handles[i].IDs[ii].LogicalID.IsNil() {
				return false, nil
			}
			handles[i].IDs[ii] = sop.NewHandle(vids[i].IDs[ii])
			blobs[i].Blobs[ii].Key = handles[i].IDs[ii].GetActiveID()
			blobs[i].Blobs[ii].Value = nodes[i].Second[ii]
		}
	}
	// Persist the nodes blobs to blob store and redis cache.
	if err := nr.transaction.blobStore.Add(ctx, blobs...); err != nil {
		return false, err
	}
	for i := range nodes {
		for ii := range nodes[i].Second {
			if err := nr.transaction.redisCache.SetStruct(ctx, nr.formatKey(handles[i].IDs[ii].GetActiveID().String()),
				nodes[i].Second[ii], nodes[i].First.CacheConfig.NodeCacheDuration); err != nil {
				return false, err
			}
		}
	}
	// Add virtual IDs to registry.
	if err := nr.transaction.registry.Add(ctx, handles...); err != nil {
		return false, err
	}
	return true, nil
}

// Save to blob store, save node ID to the alternate(inactive) physical ID(see virtual ID).
func (nr *nodeRepository) commitUpdatedNodes(ctx context.Context, nodes []sop.Tuple[*sop.StoreInfo, []interface{}]) (bool, []sop.RegistryPayload[sop.Handle], error) {
	if len(nodes) == 0 {
		return true, nil, nil
	}
	// 1st pass, update the virtual ID registry ensuring the set of nodes are only being modified by us.
	vids := convertToRegistryRequestPayload(nodes)
	handles, err := nr.transaction.registry.Get(ctx, vids...)
	if err != nil {
		return false, nil, err
	}
	blobs := make([]sop.BlobsPayload[sop.KeyValuePair[sop.UUID, interface{}]], len(nodes))
	for i := range handles {
		blobs[i].BlobTable = nodes[i].First.BlobTable
		blobs[i].Blobs = make([]sop.KeyValuePair[sop.UUID, interface{}], len(handles[i].IDs))
		for ii := range handles[i].IDs {
			// Node with such ID is marked deleted or had been updated since reading it.
			if (handles[i].IDs[ii].IsDeleted && !handles[i].IDs[ii].IsExpiredInactive()) || handles[i].IDs[ii].Version != nodes[i].Second[ii].(btree.MetaDataType).GetVersion() {
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
				return false, nil, nil
			}
			blobs[i].Blobs[ii].Key = id
			blobs[i].Blobs[ii].Value = nodes[i].Second[ii]
		}
	}
	if err := nr.transaction.registry.Update(ctx, false, handles...); err != nil {
		return false, nil, err
	}

	// 2nd pass, persist the nodes blobs to blob store and redis cache.
	if err := nr.transaction.blobStore.Add(ctx, blobs...); err != nil {
		return false, nil, err
	}
	for i := range nodes {
		for ii := range nodes[i].Second {
			if err := nr.transaction.redisCache.SetStruct(ctx, nr.formatKey(handles[i].IDs[ii].GetInActiveID().String()), nodes[i].Second[ii], nodes[i].First.CacheConfig.NodeCacheDuration); err != nil {
				return false, nil, err
			}
		}
	}
	return true, handles, nil
}

// Add the removed Node(s) and their Item(s) Data(if not in node segment) to the recycler
// so they can get serviced for physical delete on schedule in the future.
func (nr *nodeRepository) commitRemovedNodes(ctx context.Context, nodes []sop.Tuple[*sop.StoreInfo, []interface{}]) (bool, []sop.RegistryPayload[sop.Handle], error) {
	if len(nodes) == 0 {
		return true, nil, nil
	}
	vids := convertToRegistryRequestPayload(nodes)
	handles, err := nr.transaction.registry.Get(ctx, vids...)
	if err != nil {
		return false, nil, err
	}
	rightNow := Now().UnixMilli()
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
	if err := nr.transaction.registry.Update(ctx, false, handles...); err != nil {
		return false, nil, err
	}
	return true, handles, nil
}

func (nr *nodeRepository) commitAddedNodes(ctx context.Context, nodes []sop.Tuple[*sop.StoreInfo, []interface{}]) error {
	/* UUID to Virtual ID story:
	   - (on commit) New(added) nodes will have their IDs converted to virtual ID with empty
	     phys IDs(or same ID with active & virtual ID).
	   - On get, 'will read the Node using currently active ID.
	   - (on commit) On update, 'will save and register the node phys ID to the "inactive ID" part of the virtual ID.
	   - On finalization of commit, inactive will be switched to active (node) IDs.
	*/
	if len(nodes) == 0 {
		return nil
	}
	handles := make([]sop.RegistryPayload[sop.Handle], len(nodes))
	blobs := make([]sop.BlobsPayload[sop.KeyValuePair[sop.UUID, interface{}]], len(nodes))
	for i := range nodes {
		handles[i].RegistryTable = nodes[i].First.RegistryTable
		handles[i].IDs = make([]sop.Handle, len(nodes[i].Second))
		blobs[i].BlobTable = nodes[i].First.BlobTable
		blobs[i].Blobs = make([]sop.KeyValuePair[sop.UUID, interface{}], len(handles[i].IDs))
		for ii := range nodes[i].Second {
			metaData := nodes[i].Second[ii].(btree.MetaDataType)
			// Add node to blob store.
			h := sop.NewHandle(metaData.GetID())
			// Increment version.
			h.Version++
			blobs[i].Blobs[ii].Key = metaData.GetID()
			blobs[i].Blobs[ii].Value = nodes[i].Second[ii]
			handles[i].IDs[ii] = h
			// Add node to Redis cache.
			if err := nr.transaction.redisCache.SetStruct(ctx, nr.formatKey(metaData.GetID().String()), nodes[i].Second[ii], nodes[i].First.CacheConfig.NodeCacheDuration); err != nil {
				return err
			}
		}
	}
	// Register virtual IDs(a.k.a. handles).
	if err := nr.transaction.registry.Add(ctx, handles...); err != nil {
		return err
	}
	// Add nodes to blob store.
	if err := nr.transaction.blobStore.Add(ctx, blobs...); err != nil {
		return err
	}
	return nil
}

func (nr *nodeRepository) areFetchedItemsIntact(ctx context.Context, nodes []sop.Tuple[*sop.StoreInfo, []interface{}]) (bool, error) {
	if len(nodes) == 0 {
		return true, nil
	}
	// Check if the Items read for each fetchedNode are intact.
	vids := convertToRegistryRequestPayload(nodes)
	handles, err := nr.transaction.registry.Get(ctx, vids...)
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

func (nr *nodeRepository) rollbackNewRootNodes(ctx context.Context, rollbackData interface{}) error {
	if rollbackData == nil {
		return nil
	}
	var bibs []sop.BlobsPayload[sop.UUID]
	var vids []sop.RegistryPayload[sop.UUID]
	tup := rollbackData.(sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]])
	vids = tup.First
	bibs = tup.Second
	if len(vids) == 0 {
		return nil
	}

	var lastErr error
	// Undo on blob store & redis.
	if err := nr.transaction.blobStore.Remove(ctx, bibs...); err != nil {
		lastErr = fmt.Errorf("unable to undo new root nodes, %v, error: %v", bibs, err)
		log.Error(lastErr.Error())
	}
	for i := range vids {
		for ii := range vids[i].IDs {
			if err := nr.transaction.redisCache.Delete(ctx, nr.formatKey(vids[i].IDs[ii].String())); err != nil && !redis.KeyNotFound(err) {
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
		if err := nr.transaction.registry.Remove(ctx, vids...); err != nil {
			lastErr = fmt.Errorf("unable to undo new root nodes registration, %v, error: %v", vids, err)
			log.Error(lastErr.Error())
		}
	}
	return lastErr
}

func (nr *nodeRepository) rollbackAddedNodes(ctx context.Context, rollbackData interface{}) error {
	var bibs []sop.BlobsPayload[sop.UUID]
	var vids []sop.RegistryPayload[sop.UUID]
	tup := rollbackData.(sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]])
	vids = tup.First
	bibs = tup.Second
	if len(vids) == 0 {
		return nil
	}
	var lastErr error
	if err := nr.transaction.blobStore.Remove(ctx, bibs...); err != nil {
		lastErr = fmt.Errorf("unable to undo added nodes, %v, error: %v", bibs, err)
		log.Error(lastErr.Error())
	}
	// Unregister nodes IDs.
	if err := nr.transaction.registry.Remove(ctx, vids...); err != nil {
		lastErr = fmt.Errorf("unable to undo added nodes registration, %v, error: %v", vids, err)
		log.Error(lastErr.Error())
	}
	// Remove nodes from Redis cache.
	for i := range vids {
		for ii := range vids[i].IDs {
			if err := nr.transaction.redisCache.Delete(ctx, nr.formatKey(vids[i].IDs[ii].String())); err != nil && !redis.KeyNotFound(err) {
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
func (nr *nodeRepository) rollbackUpdatedNodes(ctx context.Context, vids []sop.RegistryPayload[sop.UUID]) error {
	if len(vids) == 0 {
		return nil
	}
	handles, err := nr.transaction.registry.Get(ctx, vids...)
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
	if err = nr.transaction.blobStore.Remove(ctx, blobsIDs...); err != nil {
		lastErr = fmt.Errorf("unable to undo updated nodes, %v, error: %v", blobsIDs, err)
		log.Error(lastErr.Error())
	}
	// Undo changes in virtual ID registry.
	if err = nr.transaction.registry.Update(ctx, false, handles...); err != nil {
		lastErr = fmt.Errorf("unable to undo updated nodes registration, %v, error: %v", handles, err)
		log.Error(lastErr.Error())
	}
	// Undo changes in redis.
	for i := range blobsIDs {
		for ii := range blobsIDs[i].Blobs {
			if err = nr.transaction.redisCache.Delete(ctx, nr.formatKey(blobsIDs[i].Blobs[ii].String())); err != nil && !redis.KeyNotFound(err) {
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

func (nr *nodeRepository) rollbackRemovedNodes(ctx context.Context, vids []sop.RegistryPayload[sop.UUID]) error {
	if len(vids) == 0 {
		return nil
	}
	handles, err := nr.transaction.registry.Get(ctx, vids...)
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
	if err := nr.transaction.registry.Update(ctx, false, handlesForRollback...); err != nil {
		err = fmt.Errorf("unable to undo removed nodes in registry, %v, error: %v", handlesForRollback, err)
		log.Error(err.Error())
		return err
	}
	return nil
}

// Set to active the inactive nodes.
func (nr *nodeRepository) activateInactiveNodes(handles []sop.RegistryPayload[sop.Handle]) ([]sop.RegistryPayload[sop.Handle], error) {
	if len(handles) == 0 {
		return nil, nil
	}
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
func (nr *nodeRepository) touchNodes(handles []sop.RegistryPayload[sop.Handle]) ([]sop.RegistryPayload[sop.Handle], error) {
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

func (nr *nodeRepository) formatKey(k string) string {
	return fmt.Sprintf("N%s", k)
}
