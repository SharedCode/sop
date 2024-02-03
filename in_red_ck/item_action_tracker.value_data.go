package in_red_ck

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
)

func (t *itemActionTracker[TK, TV]) commitTrackedValuesToSeparateSegments(ctx context.Context) error {
	if t.isValueDataInNodeSegment || t.isValueDataActivelyPersisted {
		return nil
	}
	itemsForAdd := cas.BlobsPayload[sop.KeyValuePair[sop.UUID, interface{}]]{
		BlobTable: t.blobTableName,
		Blobs: make([]sop.KeyValuePair[sop.UUID, interface{}], 0, 5),
	}
	for uuid, cachedItem := range t.items {
		// Nothing to save to separate segment/partition if value is nil.
		if cachedItem.item.Value == nil {
			cachedItem.item.ValueNeedsFetch = false
			t.items[uuid] = cachedItem
			continue
		}
		if cachedItem.Action == updateAction || cachedItem.Action == removeAction{
			t.forDeletionItems = append(t.forDeletionItems, cachedItem.item.Id)
			if cachedItem.Action == updateAction {
				// Replace the Item ID so we can persist a new one and not touching current one that
				// could be fetched in other transactions.
				cachedItem.item.Id = sop.NewUUID()
				t.items[uuid] = cachedItem
			}
		}
		if cachedItem.Action == addAction || cachedItem.Action == updateAction {
			itemsForAdd.Blobs = append(itemsForAdd.Blobs,
				sop.KeyValuePair[sop.UUID, interface{}]{
							Key: cachedItem.item.Id,
							Value: cachedItem.item.Value,
						})
			// nullify Value since we are saving it to a separate partition.
			cachedItem.item.Value = nil
			cachedItem.item.ValueNeedsFetch = true
			t.items[uuid] = cachedItem
		}
	}
	if len(itemsForAdd.Blobs) > 0 {
		if err := t.blobStore.Add(ctx, itemsForAdd); err != nil {
			return err
		}
	}

	// TODO: support caching large data ? 'for now, don't.
	// // Add to cache since succeeded to add to the blob store.
	// for _, kvp := range itemsForAdd.Blobs {
	// 	t.redisCache.SetStruct(ctx, t.formatKey(kvp.Key.String()), kvp.Value, nodeCacheDuration)
	// }
	return nil
}

func (t *itemActionTracker[TK, TV]) rollbackTrackedValuesInSeparateSegments(ctx context.Context) error {
	if t.isValueDataInNodeSegment || t.isValueDataActivelyPersisted {
		return nil
	}
	itemsForDelete := cas.BlobsPayload[sop.UUID] {
		BlobTable: t.blobTableName,
		Blobs: make([]sop.UUID, 0, 5),
	}
	for _, cachedItem := range t.items {
		if cachedItem.Action == addAction || cachedItem.Action == updateAction {
			t.redisCache.Delete(ctx, t.formatKey(cachedItem.item.Id.String()))
			itemsForDelete.Blobs = append(itemsForDelete.Blobs, cachedItem.item.Id)
			continue
		}
	}
	t.forDeletionItems = nil
	if len(itemsForDelete.Blobs) > 0 {
		return t.blobStore.Remove(ctx, itemsForDelete)
	}
	return nil
}

func (t *itemActionTracker[TK, TV]) deleteInactiveTrackedValuesInSeparateSegments(ctx context.Context) error {
	if t.isValueDataInNodeSegment || t.isValueDataActivelyPersisted {
		return nil
	}
	itemsForDelete := cas.BlobsPayload[sop.UUID] {
		BlobTable: t.blobTableName,
		Blobs: make([]sop.UUID, 0, 5),
	}
	for _, forDeleteId := range t.forDeletionItems {
		t.redisCache.Delete(ctx, t.formatKey(forDeleteId.String()))
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
