package in_red_ck

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
)

func (t *itemActionTracker[TK, TV]) commitTrackedValuesToSeparateSegments(ctx context.Context) error {
	if t.storeInfo.IsValueDataInNodeSegment || t.storeInfo.IsValueDataActivelyPersisted {
		return nil
	}
	itemsForAdd := cas.BlobsPayload[sop.KeyValuePair[sop.UUID, interface{}]]{
		BlobTable: t.storeInfo.BlobTable,
		Blobs:     make([]sop.KeyValuePair[sop.UUID, interface{}], 0, 5),
	}
	for uuid, cachedItem := range t.items {
		itemForAdd := t.manage(uuid, cachedItem)
		if itemForAdd != nil {
			itemsForAdd.Blobs = append(itemsForAdd.Blobs, *itemForAdd)
		}
	}
	if len(itemsForAdd.Blobs) > 0 {
		if err := t.blobStore.Add(ctx, itemsForAdd); err != nil {
			return err
		}
	}

	// Add to cache since succeeded to add to the blob store.
	if t.storeInfo.IsValueDataGloballyCached {
		for _, kvp := range itemsForAdd.Blobs {
			t.redisCache.SetStruct(ctx, t.formatKey(kvp.Key.String()), kvp.Value, nodeCacheDuration)
		}
	}
	return nil
}

func (t *itemActionTracker[TK, TV]) manage(uuid sop.UUID, cachedItem cacheItem[TK, TV]) *sop.KeyValuePair[sop.UUID, interface{}] {
	var r *sop.KeyValuePair[sop.UUID, interface{}]
	if cachedItem.Action == updateAction || cachedItem.Action == removeAction{
		if cachedItem.item.ValueNeedsFetch {
			// If there is value data on another segment, mark it for delete.
			t.forDeletionItems = append(t.forDeletionItems, cachedItem.item.ID)
		}
		cachedItem.item.ValueNeedsFetch = false
		if cachedItem.Action == updateAction {
			// Replace the Item ID so we can persist a new one and not touching current one that
			// could be fetched in other transactions.
			if cachedItem.item.Value != nil {
				cachedItem.item.ID = sop.NewUUID()
			}
			t.items[uuid] = cachedItem
		}
	}
	if cachedItem.Action == addAction || cachedItem.Action == updateAction {
		if cachedItem.item.Value != nil {
			r = &sop.KeyValuePair[sop.UUID, interface{}]{
				Key:   cachedItem.item.ID,
				Value: cachedItem.item.Value,
			}
			// nullify Value since we are saving it to a separate partition.
			cachedItem.inflightItemValue = cachedItem.item.Value
			cachedItem.item.Value = nil
			cachedItem.item.ValueNeedsFetch = true
		}
		t.items[uuid] = cachedItem
	}
	return r
}

// TODO: for IsValueDataActivelyPersisted, we need to support both partial & fulle rollback.
// For refetch & merge: partial rollback will not undo saved "values" in data segments and
// will, thus, merge with the "item" keeping these values' data in other segments and not in memory.
// So, it is a light weight merge.
//
// When timeout or conflict occurs, then full rollback will allow undo of entire changes including
// these "values" data segments deletion.

func (t *itemActionTracker[TK, TV]) rollbackTrackedValuesInSeparateSegments(ctx context.Context) error {
	if t.storeInfo.IsValueDataInNodeSegment {
		return nil
	}
	itemsForDelete := cas.BlobsPayload[sop.UUID]{
		BlobTable: t.storeInfo.BlobTable,
		Blobs:     make([]sop.UUID, 0, 5),
	}
	for itemID, cachedItem := range t.items {
		if cachedItem.Action == addAction || cachedItem.Action == updateAction {
			if cachedItem.inflightItemValue != nil {
				cachedItem.item.Value = cachedItem.inflightItemValue
				cachedItem.inflightItemValue = nil
				cachedItem.item.ValueNeedsFetch = false
			}
			if t.storeInfo.IsValueDataGloballyCached {
				t.redisCache.Delete(ctx, t.formatKey(cachedItem.item.ID.String()))
			}
			itemsForDelete.Blobs = append(itemsForDelete.Blobs, cachedItem.item.ID)

			// Restore the Item ID now that the temp got added for deletion.
			cachedItem.item.ID = itemID
			t.items[itemID] = cachedItem
			continue
		}
	}
	t.forDeletionItems = nil
	if len(itemsForDelete.Blobs) > 0 {
		return t.blobStore.Remove(ctx, itemsForDelete)
	}
	return nil
}

func (t *itemActionTracker[TK, TV]) deleteObsoleteTrackedValuesInSeparateSegments(ctx context.Context) error {
	if t.storeInfo.IsValueDataInNodeSegment {
		return nil
	}
	itemsForDelete := cas.BlobsPayload[sop.UUID]{
		BlobTable: t.storeInfo.BlobTable,
		Blobs:     make([]sop.UUID, 0, 5),
	}
	for _, forDeleteID := range t.forDeletionItems {
		if t.storeInfo.IsValueDataGloballyCached {
			t.redisCache.Delete(ctx, t.formatKey(forDeleteID.String()))
		}
		itemsForDelete.Blobs = append(itemsForDelete.Blobs, forDeleteID)
	}
	if len(itemsForDelete.Blobs) > 0 {
		return t.blobStore.Remove(ctx, itemsForDelete)
	}
	return nil
}

func (t *itemActionTracker[TK, TV]) formatKey(k string) string {
	return fmt.Sprintf("V%s", k)
}
