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
		if cachedItem.Action == updateAction || cachedItem.Action == removeAction {
			if cachedItem.item.ValueNeedsFetch {
				// If there is value data on another segment, mark it for delete.
				t.forDeletionItems = append(t.forDeletionItems, cachedItem.item.Id)
			}
			cachedItem.item.ValueNeedsFetch = false
			if cachedItem.Action == updateAction {
				// Replace the Item ID so we can persist a new one and not touching current one that
				// could be fetched in other transactions.
				if cachedItem.item.Value != nil {
					cachedItem.item.Id = sop.NewUUID()
				}
				t.items[uuid] = cachedItem
			}
		}
		if cachedItem.Action == addAction || cachedItem.Action == updateAction {
			if cachedItem.item.Value != nil {
				itemsForAdd.Blobs = append(itemsForAdd.Blobs,
					sop.KeyValuePair[sop.UUID, interface{}]{
						Key:   cachedItem.item.Id,
						Value: cachedItem.item.Value,
					})
				// nullify Value since we are saving it to a separate partition.
				cachedItem.inflightItemValue = cachedItem.item.Value
				cachedItem.item.Value = nil
				cachedItem.item.ValueNeedsFetch = true
			}
			t.items[uuid] = cachedItem
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

func (t *itemActionTracker[TK, TV]) rollbackTrackedValuesInSeparateSegments(ctx context.Context) error {
	if t.storeInfo.IsValueDataInNodeSegment || t.storeInfo.IsValueDataActivelyPersisted {
		return nil
	}
	itemsForDelete := cas.BlobsPayload[sop.UUID]{
		BlobTable: t.storeInfo.BlobTable,
		Blobs:     make([]sop.UUID, 0, 5),
	}
	for itemId, cachedItem := range t.items {
		if cachedItem.Action == addAction || cachedItem.Action == updateAction {
			if cachedItem.inflightItemValue != nil {
				cachedItem.item.Value = cachedItem.inflightItemValue
				cachedItem.inflightItemValue = nil
				cachedItem.item.ValueNeedsFetch = false
			}
			if t.storeInfo.IsValueDataGloballyCached {
				t.redisCache.Delete(ctx, t.formatKey(cachedItem.item.Id.String()))
			}
			itemsForDelete.Blobs = append(itemsForDelete.Blobs, cachedItem.item.Id)

			// Restore the Item Id now that the temp got added for deletion.
			cachedItem.item.Id = itemId
			t.items[itemId] = cachedItem
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
	if t.storeInfo.IsValueDataInNodeSegment || t.storeInfo.IsValueDataActivelyPersisted {
		return nil
	}
	itemsForDelete := cas.BlobsPayload[sop.UUID]{
		BlobTable: t.storeInfo.BlobTable,
		Blobs:     make([]sop.UUID, 0, 5),
	}
	for _, forDeleteId := range t.forDeletionItems {
		if t.storeInfo.IsValueDataGloballyCached {
			t.redisCache.Delete(ctx, t.formatKey(forDeleteId.String()))
		}
		itemsForDelete.Blobs = append(itemsForDelete.Blobs, forDeleteId)
	}
	if len(itemsForDelete.Blobs) > 0 {
		return t.blobStore.Remove(ctx, itemsForDelete)
	}
	return nil
}

func (t *itemActionTracker[TK, TV]) formatKey(k string) string {
	return fmt.Sprintf("V%s", k)
}
