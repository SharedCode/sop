package in_red_ck

import (
	"context"
	"fmt"
	log "log/slog"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
	"github.com/SharedCode/sop/in_red_ck/kafka"
	"github.com/SharedCode/sop/in_red_ck/redis"
)

type cacheNode struct {
	node   interface{}
	action actionType
}

type nodeRepositoryTyped[TK btree.Comparable, TV any] struct {
	realNodeRepository *nodeRepository
}

// now is a lambda expression that returns the current time in Unix milliseconds.
var now = time.Now().UnixMilli

var nodeCacheDuration time.Duration = time.Duration(1 * time.Hour)

// SetNodeCacheDuration allows node cache duration to get set globally.
func SetNodeCacheDuration(duration time.Duration) {
	if duration < time.Minute {
		duration = time.Duration(20 * time.Minute)
	}
	nodeCacheDuration = duration
}

// Add will upsert node to the map.
func (nr *nodeRepositoryTyped[TK, TV]) Add(n *btree.Node[TK, TV]) {
	nr.realNodeRepository.add(n.Id, n)
}

// Update will upsert node to the map.
func (nr *nodeRepositoryTyped[TK, TV]) Update(n *btree.Node[TK, TV]) {
	nr.realNodeRepository.update(n.Id, n)
}

// Get will retrieve a node with nodeId from the map.
func (nr *nodeRepositoryTyped[TK, TV]) Get(ctx context.Context, nodeId btree.UUID) (*btree.Node[TK, TV], error) {
	var target btree.Node[TK, TV]
	n, err := nr.realNodeRepository.get(ctx, nodeId, &target)
	if n == nil {
		return nil, err
	}
	return n.(*btree.Node[TK, TV]), err
}

func (nr *nodeRepositoryTyped[TK, TV]) Fetched(nodeId btree.UUID) {
	c := nr.realNodeRepository.nodeLocalCache[nodeId]
	if c.action == defaultAction {
		c.action = getAction
		nr.realNodeRepository.nodeLocalCache[nodeId] = c
	}
}

// Remove will remove a node with nodeId from the map.
func (nr *nodeRepositoryTyped[TK, TV]) Remove(nodeId btree.UUID) {
	nr.realNodeRepository.remove(nodeId)
}

// nodeRepository implementation for "cassandra-S3"(in_cas_s3) exposes a standard NodeRepository interface
// but which, manages b-tree nodes in transaction cache, Redis and in Cassandra + S3,
// or File System, for debugging &/or "poor man's" setup(no AWS required!).
type nodeRepository struct {
	transaction *transaction
	// TODO: implement a MRU caching on node local cache so we only retain a handful in memory.
	nodeLocalCache map[btree.UUID]cacheNode
	storeInfo      *btree.StoreInfo
	count          int64
}

// NewNodeRepository instantiates a NodeRepository.
func newNodeRepository[TK btree.Comparable, TV any](t *transaction, storeInfo *btree.StoreInfo) *nodeRepositoryTyped[TK, TV] {
	nr := &nodeRepository{
		transaction:    t,
		nodeLocalCache: make(map[btree.UUID]cacheNode),
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

// Get will retrieve a node with nodeId from the map.
func (nr *nodeRepository) get(ctx context.Context, logicalId btree.UUID, target interface{}) (interface{}, error) {
	if v, ok := nr.nodeLocalCache[logicalId]; ok {
		if v.action == removeAction {
			return nil, nil
		}
		return v.node, nil
	}
	h, err := nr.transaction.registry.Get(ctx, cas.RegistryPayload[btree.UUID]{
		RegistryTable: nr.storeInfo.RegistryTable,
		IDs:           []btree.UUID{logicalId},
	})
	if err != nil {
		return nil, err
	}
	if len(h) == 0 || len(h[0].IDs) == 0 {
		return nil, nil
	}
	nodeId := logicalId
	if !h[0].IDs[0].LogicalId.IsNil() {
		// Use active physical Id if in case different.
		nodeId = h[0].IDs[0].GetActiveId()
	}
	if err := nr.transaction.redisCache.GetStruct(ctx, nr.formatKey(nodeId.ToString()), target); err != nil {
		if !redis.KeyNotFound(err) {
			return nil, err
		}
		// Fetch from blobStore and cache to Redis/local.
		if err = nr.transaction.nodeBlobStore.GetOne(ctx, nr.storeInfo.BlobTable, nodeId, target); err != nil {
			return nil, err
		}
		target.(btree.MetaDataType).SetVersion(h[0].IDs[0].Version)
		if err := nr.transaction.redisCache.SetStruct(ctx, nr.formatKey(nodeId.ToString()), target, nodeCacheDuration); err != nil {
			log.Warn(fmt.Sprintf("Failed to cache in Redis the newly fetched node with Id: %v, details: %v", nodeId, err))
		}
		nr.nodeLocalCache[logicalId] = cacheNode{
			action: defaultAction,
			node:   target,
		}
		return target, nil
	}
	target.(btree.MetaDataType).SetVersion(h[0].IDs[0].Version)
	nr.nodeLocalCache[logicalId] = cacheNode{
		action: defaultAction,
		node:   target,
	}
	return target, nil
}

func (nr *nodeRepository) add(nodeId btree.UUID, node interface{}) {
	nr.nodeLocalCache[nodeId] = cacheNode{
		action: addAction,
		node:   node,
	}
}

func (nr *nodeRepository) update(nodeId btree.UUID, node interface{}) {
	if v, ok := nr.nodeLocalCache[nodeId]; ok {
		// Update the node and keep the "action" marker if new, otherwise update to "update" action.
		v.node = node
		if v.action != addAction {
			v.action = updateAction
		}
		nr.nodeLocalCache[nodeId] = v
		return
	}
	// Treat as add if not in local cache, because it should be there unless node is new.
	nr.nodeLocalCache[nodeId] = cacheNode{
		action: addAction,
		node:   node,
	}
}

func (nr *nodeRepository) remove(nodeId btree.UUID) {
	if v, ok := nr.nodeLocalCache[nodeId]; ok {
		if v.action == addAction {
			delete(nr.nodeLocalCache, nodeId)
			return
		}
		v.action = removeAction
		nr.nodeLocalCache[nodeId] = v
	}
	// Code should not reach this point, as B-tree will not issue a remove if node is not cached locally.
}

func (nr *nodeRepository) commitNewRootNodes(ctx context.Context, nodes []sop.KeyValuePair[*btree.StoreInfo, []interface{}]) (bool, error) {
	if len(nodes) == 0 {
		return true, nil
	}
	vids := nr.convertToRegistryRequestPayload(nodes)
	handles, err := nr.transaction.registry.Get(ctx, vids...)
	if err != nil {
		return false, err
	}
	blobs := make([]cas.BlobsPayload[sop.KeyValuePair[btree.UUID, interface{}]], len(nodes))
	for i := range handles {
		if len(handles[i].IDs) == 0 {
			handles[i].IDs = make([]sop.Handle, len(vids[i].IDs))
		}
		blobs[i].Blobs = make([]sop.KeyValuePair[btree.UUID, interface{}], len(handles[i].IDs))
		blobs[i].BlobTable = nodes[i].Key.BlobTable
		for ii := range handles[i].IDs {
			// Check if a non-empty root node was found, fail to cause "re-sync & merge".
			if !handles[i].IDs[ii].LogicalId.IsNil() {
				return false, nil
			}
			handles[i].IDs[ii] = sop.NewHandle(vids[i].IDs[ii])
			blobs[i].Blobs[ii].Key = handles[i].IDs[ii].GetActiveId()
			blobs[i].Blobs[ii].Value = nodes[i].Value[ii]
		}
	}
	// Persist the nodes blobs to blob store and redis cache.
	if err := nr.transaction.nodeBlobStore.Add(ctx, blobs...); err != nil {
		return false, err
	}
	for i := range nodes {
		for ii := range nodes[i].Value {
			if err := nr.transaction.redisCache.SetStruct(ctx, nr.formatKey(handles[i].IDs[ii].GetActiveId().ToString()),
				nodes[i].Value[ii], nodeCacheDuration); err != nil {
				return false, err
			}
		}
	}
	// Add virtual Ids to registry.
	if err := nr.transaction.registry.Add(ctx, handles...); err != nil {
		return false, err
	}
	return true, nil
}

// Save to blob store, save node Id to the alternate(inactive) physical Id(see virtual Id).
func (nr *nodeRepository) commitUpdatedNodes(ctx context.Context, nodes []sop.KeyValuePair[*btree.StoreInfo, []interface{}]) (bool, error) {
	if len(nodes) == 0 {
		return true, nil
	}
	// 1st pass, update the virtual Id registry ensuring the set of nodes are only being modified by us.
	vids := nr.convertToRegistryRequestPayload(nodes)
	handles, err := nr.transaction.registry.Get(ctx, vids...)
	if err != nil {
		return false, err
	}
	blobs := make([]cas.BlobsPayload[sop.KeyValuePair[btree.UUID, interface{}]], len(nodes))
	inactiveBlobIds := make([]cas.BlobsPayload[btree.UUID], len(nodes))
	for i := range handles {
		blobs[i].BlobTable = nodes[i].Key.BlobTable
		inactiveBlobIds[i].BlobTable = nodes[i].Key.BlobTable
		blobs[i].Blobs = make([]sop.KeyValuePair[btree.UUID, interface{}], len(handles[i].IDs))
		inactiveBlobIds[i].Blobs = make([]btree.UUID, 0, len(handles[i].IDs))
		for ii := range handles[i].IDs {
			// Node with such Id is marked deleted or had been updated since reading it.
			if handles[i].IDs[ii].IsDeleted || handles[i].IDs[ii].Version != nodes[i].Value[ii].(btree.MetaDataType).GetVersion() {
				return false, nil
			}
			// Create new phys. UUID and auto-assign it to the available phys. Id(A or B) "Id slot".
			id := handles[i].IDs[ii].AllocateId()
			if id == btree.NilUUID {
				if handles[i].IDs[ii].IsExpiredInactive() {
					// Collect the inactive Blob Ids so we can issue a delete for them to ensure they will be gone.
					// Kafka based delete service should delete them, but in case that is not running.
					inactiveBlobIds[i].Blobs = append(inactiveBlobIds[i].Blobs, handles[i].IDs[ii].GetInActiveId())
					handles[i].IDs[ii].ClearInactiveId()
					// Allocate a new Id after clearing the unused inactive Id.
					id = handles[i].IDs[ii].AllocateId()
				}
			}
			if id == btree.NilUUID {
				// Return false as there is an ongoing update on node by another transaction.
				return false, nil
			}
			blobs[i].Blobs[ii].Key = id
			blobs[i].Blobs[ii].Value = nodes[i].Value[ii]
		}
	}
	// If it is known that Kafka enqueuing is succeeding then we don't have to issue a delete,
	// as the "delete service" which fetch messages from Kafka will ensure inactive Nodes are deleted.
	// But if such is not working or known not to work then we will issue deletes here in the main path
	// to prevent unusual data growth due to unused Node records.
	if !kafka.LastEnqueueSucceeded() {
		// Deleting blobs is a tolerable error, 'just log the error if there is.
		if err := nr.transaction.nodeBlobStore.Remove(ctx, inactiveBlobIds...); err != nil {
			log.Error(fmt.Sprintf("Error encountered deleting blobs(%v), details: %v", inactiveBlobIds, err))
		}
	}
	if err := nr.transaction.registry.Update(ctx, false, handles...); err != nil {
		return false, err
	}

	// 2nd pass, persist the nodes blobs to blob store and redis cache.
	if err := nr.transaction.nodeBlobStore.Add(ctx, blobs...); err != nil {
		return false, err
	}
	for i := range nodes {
		for ii := range nodes[i].Value {
			if err := nr.transaction.redisCache.SetStruct(ctx, nr.formatKey(handles[i].IDs[ii].GetInActiveId().ToString()), nodes[i].Value[ii], nodeCacheDuration); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

// Add the removed Node(s) and their Item(s) Data(if not in node segment) to the recycler
// so they can get serviced for physical delete on schedule in the future.
func (nr *nodeRepository) commitRemovedNodes(ctx context.Context, nodes []sop.KeyValuePair[*btree.StoreInfo, []interface{}]) (bool, error) {
	if len(nodes) == 0 {
		return true, nil
	}
	vids := nr.convertToRegistryRequestPayload(nodes)
	handles, err := nr.transaction.registry.Get(ctx, vids...)
	if err != nil {
		return false, err
	}
	rightNow := now()
	for i := range handles {
		for ii := range handles[i].IDs {
			// Node with such Id is already marked deleted, is in-flight change or had been updated since reading it,
			// fail it for "refetch" & retry.
			if handles[i].IDs[ii].IsDeleted || handles[i].IDs[ii].Version != nodes[i].Value[ii].(btree.MetaDataType).GetVersion() {
				return false, nil
			}
			// Mark Id as deleted.
			handles[i].IDs[ii].IsDeleted = true
			handles[i].IDs[ii].WorkInProgressTimestamp = rightNow
		}
	}
	// Persist the handles changes.
	if err := nr.transaction.registry.Update(ctx, false, handles...); err != nil {
		return false, err
	}
	return true, nil
}

func (nr *nodeRepository) commitAddedNodes(ctx context.Context, nodes []sop.KeyValuePair[*btree.StoreInfo, []interface{}]) error {
	/* UUID to Virtual Id story:
	   - (on commit) New(added) nodes will have their Ids converted to virtual Id with empty
	     phys Ids(or same Id with active & virtual Id).
	   - On get, 'will read the Node using currently active Id.
	   - (on commit) On update, 'will save and register the node phys Id to the "inactive Id" part of the virtual Id.
	   - On finalization of commit, inactive will be switched to active (node) Ids.
	*/
	if len(nodes) == 0 {
		return nil
	}
	handles := make([]cas.RegistryPayload[sop.Handle], len(nodes))
	blobs := make([]cas.BlobsPayload[sop.KeyValuePair[btree.UUID, interface{}]], len(nodes))
	for i := range nodes {
		handles[i].RegistryTable = nodes[i].Key.RegistryTable
		handles[i].IDs = make([]sop.Handle, len(nodes[i].Value))
		blobs[i].BlobTable = nodes[i].Key.BlobTable
		blobs[i].Blobs = make([]sop.KeyValuePair[btree.UUID, interface{}], len(handles[i].IDs))
		for ii := range nodes[i].Value {
			metaData := nodes[i].Value[ii].(btree.MetaDataType)
			// Add node to blob store.
			h := sop.NewHandle(metaData.GetId())
			// Increment version.
			h.Version++
			blobs[i].Blobs[ii].Key = metaData.GetId()
			blobs[i].Blobs[ii].Value = nodes[i].Value[ii]
			handles[i].IDs[ii] = h
			// Add node to Redis cache.
			if err := nr.transaction.redisCache.SetStruct(ctx, nr.formatKey(metaData.GetId().ToString()), nodes[i].Value[ii], nodeCacheDuration); err != nil {
				return err
			}
		}
	}
	// Register virtual Ids(a.k.a. handles).
	if err := nr.transaction.registry.Add(ctx, handles...); err != nil {
		return err
	}
	// Add nodes to blob store.
	if err := nr.transaction.nodeBlobStore.Add(ctx, blobs...); err != nil {
		return err
	}
	return nil
}

func (nr *nodeRepository) areFetchedItemsIntact(ctx context.Context, nodes []sop.KeyValuePair[*btree.StoreInfo, []interface{}]) (bool, error) {
	if len(nodes) == 0 {
		return true, nil
	}
	// Check if the Items read for each fetchedNode are intact.
	vids := nr.convertToRegistryRequestPayload(nodes)
	handles, err := nr.transaction.registry.Get(ctx, vids...)
	if err != nil {
		return false, err
	}
	for i := range handles {
		for ii := range handles[i].IDs {
			// Node with Id had been updated(or deleted) since reading it.
			if handles[i].IDs[ii].Version != nodes[i].Value[ii].(btree.MetaDataType).GetVersion() {
				return false, nil
			}
		}
	}
	return true, nil
}

func (nr *nodeRepository) rollbackNewRootNodes(ctx context.Context, nodes []sop.KeyValuePair[*btree.StoreInfo, []interface{}]) error {
	if len(nodes) == 0 {
		return nil
	}
	bibs := nr.convertToBlobRequestPayload(nodes)
	vids := nr.convertToRegistryRequestPayload(nodes)
	var lastErr error
	// Undo on blob store & redis.
	if err := nr.transaction.nodeBlobStore.Remove(ctx, bibs...); err != nil {
		lastErr = fmt.Errorf("Unable to undo new root nodes, %v, error: %v", bibs, err)
		log.Error(lastErr.Error())
	}
	for i := range nodes {
		for ii := range nodes[i].Value {
			if err := nr.transaction.redisCache.Delete(ctx, nr.formatKey(vids[i].IDs[ii].ToString())); err != nil && !redis.KeyNotFound(err) {
				err = fmt.Errorf("Unable to undo new root nodes in redis, error: %v", err)
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
			lastErr = fmt.Errorf("Unable to undo new root nodes registration, %v, error: %v", vids, err)
			log.Error(lastErr.Error())
		}
	}
	return lastErr
}

func (nr *nodeRepository) rollbackAddedNodes(ctx context.Context, nodes []sop.KeyValuePair[*btree.StoreInfo, []interface{}]) error {
	if len(nodes) == 0 {
		return nil
	}
	var lastErr error
	vids := nr.convertToRegistryRequestPayload(nodes)
	// Remove nodes from blob store.
	bibs := nr.convertToBlobRequestPayload(nodes)
	if err := nr.transaction.nodeBlobStore.Remove(ctx, bibs...); err != nil {
		lastErr = fmt.Errorf("Unable to undo added nodes, %v, error: %v", bibs, err)
		log.Error(lastErr.Error())
	}
	// Unregister nodes Ids.
	if err := nr.transaction.registry.Remove(ctx, vids...); err != nil {
		lastErr = fmt.Errorf("Unable to undo added nodes registration, %v, error: %v", vids, err)
		log.Error(lastErr.Error())
	}
	// Remove nodes from Redis cache.
	for i := range vids {
		for ii := range vids[i].IDs {
			if err := nr.transaction.redisCache.Delete(ctx, nr.formatKey(vids[i].IDs[ii].ToString())); err != nil && !redis.KeyNotFound(err) {
				err = fmt.Errorf("Unable to undo added nodes in redis, error: %v", err)
				if lastErr == nil {
					lastErr = err
				}
				log.Warn(err.Error())
			}
		}
	}
	return lastErr
}

func (nr *nodeRepository) rollbackUpdatedNodes(ctx context.Context, nodes []sop.KeyValuePair[*btree.StoreInfo, []interface{}]) error {
	if len(nodes) == 0 {
		return nil
	}
	vids := nr.convertToRegistryRequestPayload(nodes)
	handles, err := nr.transaction.registry.Get(ctx, vids...)
	if err != nil {
		return err
	}
	blobsIds := make([]cas.BlobsPayload[btree.UUID], len(nodes))
	for i := range handles {
		blobsIds[i].BlobTable = btree.ConvertToBlobTableName(vids[i].RegistryTable)
		blobsIds[i].Blobs = make([]btree.UUID, len(handles[i].IDs))
		for ii := range handles[i].IDs {
			blobsIds[i].Blobs[ii] = handles[i].IDs[ii].GetInActiveId()
			handles[i].IDs[ii].ClearInactiveId()
		}
	}
	var lastErr error
	// Undo the nodes blobs to blob store.
	if err = nr.transaction.nodeBlobStore.Remove(ctx, blobsIds...); err != nil {
		lastErr = fmt.Errorf("Unable to undo updated nodes, %v, error: %v", blobsIds, err)
		log.Error(lastErr.Error())
	}
	// Undo changes in virtual Id registry.
	if err = nr.transaction.registry.Update(ctx, false, handles...); err != nil {
		lastErr = fmt.Errorf("Unable to undo updated nodes registration, %v, error: %v", handles, err)
		log.Error(lastErr.Error())
	}
	// Undo changes in redis.
	for i := range blobsIds {
		for ii := range blobsIds[i].Blobs {
			if blobsIds[i].Blobs[ii].IsNil() {
				continue
			}
			if err = nr.transaction.redisCache.Delete(ctx, nr.formatKey(blobsIds[i].Blobs[ii].ToString())); err != nil && !redis.KeyNotFound(err) {
				err = fmt.Errorf("Unable to undo updated nodes in redis, error: %v", err)
				if lastErr == nil {
					lastErr = err
				}
				log.Warn(err.Error())
			}
		}
	}
	return lastErr
}

func (nr *nodeRepository) rollbackRemovedNodes(ctx context.Context, nodes []sop.KeyValuePair[*btree.StoreInfo, []interface{}]) error {
	if len(nodes) == 0 {
		return nil
	}
	vids := nr.convertToRegistryRequestPayload(nodes)
	handles, err := nr.transaction.registry.Get(ctx, vids...)
	if err != nil {
		err = fmt.Errorf("Unable to fetch removed nodes from registry, %v, error: %v", vids, err)
		log.Error(err.Error())
		return err
	}
	for i := range handles {
		for ii := range handles[i].IDs {
			// Undo the deleted mark for Id.
			handles[i].IDs[ii].IsDeleted = false
			handles[i].IDs[ii].WorkInProgressTimestamp = 0
		}
	}

	// Persist the handles changes.
	if err := nr.transaction.registry.Update(ctx, false, handles...); err != nil {
		err = fmt.Errorf("Unable to undo removed nodes in registry, %v, error: %v", handles, err)
		log.Error(err.Error())
		return err
	}
	return nil
}

// Set to active the inactive nodes. This is the last persistence step in transaction commit.
func (nr *nodeRepository) activateInactiveNodes(ctx context.Context, nodes []sop.KeyValuePair[*btree.StoreInfo, []interface{}]) ([]cas.RegistryPayload[sop.Handle], error) {
	if len(nodes) == 0 {
		return nil, nil
	}
	vids := nr.convertToRegistryRequestPayload(nodes)
	handles, err := nr.transaction.registry.Get(ctx, vids...)
	if err != nil {
		return nil, err
	}
	for i := range nodes {
		for ii := range nodes[i].Value {
			// Set the inactive as active Id.
			handles[i].IDs[ii].FlipActiveId()
			// Increment version, we are finalizing the commit for the node.
			handles[i].IDs[ii].Version++
			// Set work in progress timestamp to now as safety. After flipping inactive to active,
			// the previously active Id if not "cleaned up" then this timestamp will allow future
			// transactions to clean it up(self healing).
			handles[i].IDs[ii].WorkInProgressTimestamp = 1
		}
	}
	// All or nothing batch update.
	return handles, nil
}

// Update upsert time of a given set of nodes.
func (nr *nodeRepository) touchNodes(ctx context.Context, nodes []sop.KeyValuePair[*btree.StoreInfo, []interface{}]) ([]cas.RegistryPayload[sop.Handle], error) {
	if len(nodes) == 0 {
		return nil, nil
	}
	vids := nr.convertToRegistryRequestPayload(nodes)
	handles, err := nr.transaction.registry.Get(ctx, vids...)
	if err != nil {
		return nil, err
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

func (nr *nodeRepository) convertToBlobRequestPayload(nodes []sop.KeyValuePair[*btree.StoreInfo, []interface{}]) []cas.BlobsPayload[btree.UUID] {
	bibs := make([]cas.BlobsPayload[btree.UUID], len(nodes))
	for i := range nodes {
		bibs[i] = cas.BlobsPayload[btree.UUID]{
			BlobTable: nodes[i].Key.BlobTable,
			Blobs:     make([]btree.UUID, len(nodes[i].Value)),
		}
		for ii := range nodes[i].Value {
			bibs[i].Blobs[ii] = nodes[i].Value[ii].(btree.MetaDataType).GetId()
		}
	}
	return bibs
}

func (nr *nodeRepository) convertToRegistryRequestPayload(nodes []sop.KeyValuePair[*btree.StoreInfo, []interface{}]) []cas.RegistryPayload[btree.UUID] {
	vids := make([]cas.RegistryPayload[btree.UUID], len(nodes))
	for i := range nodes {
		vids[i] = cas.RegistryPayload[btree.UUID]{
			RegistryTable: nodes[i].Key.RegistryTable,
			IDs:           make([]btree.UUID, len(nodes[i].Value)),
		}
		for ii := range nodes[i].Value {
			vids[i].IDs[ii] = nodes[i].Value[ii].(btree.MetaDataType).GetId()
		}
	}
	return vids
}

func (nr *nodeRepository) formatKey(k string) string {
	return fmt.Sprintf("N%s", k)
}
