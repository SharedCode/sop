package common

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
)

func (t *itemActionTracker[TK, TV]) commitTrackedItemsValues(ctx context.Context) error {
	if t.storeInfo.IsValueDataInNodeSegment || t.storeInfo.IsValueDataActivelyPersisted {
		return nil
	}
	itemsForAdd := sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
		BlobTable: t.storeInfo.BlobTable,
		Blobs:     make([]sop.KeyValuePair[sop.UUID, []byte], 0, 5),
	}
	itemsForAddValues := make([]*TV, 0, 5)
	for uuid, cachedItem := range t.items {
		iv := cachedItem.item.Value
		itemForAdd, err := t.manage(uuid, cachedItem)
		if err != nil {
			return err
		}
		if itemForAdd != nil {
			itemsForAdd.Blobs = append(itemsForAdd.Blobs, *itemForAdd)
			itemsForAddValues = append(itemsForAddValues, iv)
		}
	}
	if len(itemsForAdd.Blobs) > 0 {
		if err := t.blobStore.Add(ctx, itemsForAdd); err != nil {
			return err
		}
	}

	// Add to cache since succeeded to add to the blob store.
	if t.storeInfo.IsValueDataGloballyCached {
		for i, kvp := range itemsForAdd.Blobs {
			t.cache.SetStruct(ctx, formatItemKey(kvp.Key.String()), itemsForAddValues[i], t.storeInfo.CacheConfig.ValueDataCacheDuration)
		}
	}
	return nil
}

func (t *itemActionTracker[TK, TV]) manage(uuid sop.UUID, cachedItem cacheItem[TK, TV]) (*sop.KeyValuePair[sop.UUID, []byte], error) {
	if cachedItem.persisted {
		return nil, nil
	}
	var r *sop.KeyValuePair[sop.UUID, []byte]
	if cachedItem.Action == updateAction || cachedItem.Action == removeAction {
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
			ba, err := encoding.Marshal(cachedItem.item.Value)
			if err != nil {
				return nil, err
			}
			r = &sop.KeyValuePair[sop.UUID, []byte]{
				Key:   cachedItem.item.ID,
				Value: ba,
			}
			// nullify Value since we are saving it to a separate partition.
			cachedItem.item.Value = nil
			cachedItem.item.ValueNeedsFetch = true
		}
		t.items[uuid] = cachedItem
	}
	return r, nil
}

func (t *itemActionTracker[TK, TV]) getForRollbackTrackedItemsValues() *sop.BlobsPayload[sop.UUID] {
	var itemsForDelete sop.BlobsPayload[sop.UUID]
	if t.storeInfo.IsValueDataInNodeSegment {
		return nil
	}
	itemsForDelete = sop.BlobsPayload[sop.UUID]{
		BlobTable: t.storeInfo.BlobTable,
		Blobs:     make([]sop.UUID, 0, 5),
	}
	for itemID, cachedItem := range t.items {
		if cachedItem.Action == addAction || cachedItem.Action == updateAction {
			itemsForDelete.Blobs = append(itemsForDelete.Blobs, cachedItem.item.ID)
			// Restore the Item ID now that the temp got added for deletion.
			cachedItem.item.ID = itemID
			t.items[itemID] = cachedItem
		}
	}
	t.forDeletionItems = nil
	return &itemsForDelete
}

func (t *itemActionTracker[TK, TV]) getObsoleteTrackedItemsValues() *sop.BlobsPayload[sop.UUID] {
	if t.storeInfo.IsValueDataInNodeSegment {
		return nil
	}
	itemsForDelete := sop.BlobsPayload[sop.UUID]{
		BlobTable: t.storeInfo.BlobTable,
		Blobs:     make([]sop.UUID, 0, 5),
	}
	itemsForDelete.Blobs = append(itemsForDelete.Blobs, t.forDeletionItems...)
	return &itemsForDelete
}

// format Item Key for Redis I/O.
func formatItemKey(k string) string {
	return fmt.Sprintf("V%s", k)
}
