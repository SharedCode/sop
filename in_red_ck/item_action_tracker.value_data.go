package in_red_ck

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
)

func (t *itemActionTracker[TK, TV]) commitTrackedValuesToSeparateSegments(ctx context.Context) error {
		/*
			getAction
			addAction
			updateAction
			removeAction
		*/
	itemsForAdd := make([]cas.BlobsPayload[sop.KeyValuePair[sop.UUID, interface{}]], 0, 5)
	// itemsForDelete := make([]cas.BlobsPayload[sop.UUID], 0, 5)
	for _, cachedItem := range t.items {
		if cachedItem.Action == addAction || cachedItem.Action == updateAction {
			itemsForAdd = append(itemsForAdd,
				cas.BlobsPayload[sop.KeyValuePair[sop.UUID, interface{}]]{
					BlobTable: t.blobTableName,
					Blobs: []sop.KeyValuePair[sop.UUID, interface{}]{{
							Key: cachedItem.item.Id,
							Value: cachedItem.item.Value,
						},
					},
				})
			continue
		}
		if cachedItem.Action == getAction {
			continue
		}
		if cachedItem.Action == removeAction {
		}
	}
	return nil
}

func (t *itemActionTracker[TK, TV]) rollbackTrackedValuesInSeparateSegments(ctx context.Context) error {
	// for uuid, cachedItem := range t.items {
	// 	if cachedItem.Action == addAction {
	// 		continue
	// 	}
	// 	var readItem lockRecord
	// 	if err := itemRedisCache.GetStruct(ctx, redis.FormatLockKey(uuid.String()), &readItem); err != nil {
	// 		return err
	// 	}
	// 	// Item found in Redis.
	// 	if readItem.LockId == cachedItem.LockId {
	// 		continue
	// 	}
	// 	// Lock compatibility check.
	// 	if readItem.Action == getAction && cachedItem.Action == getAction {
	// 		continue
	// 	}
	// 	return fmt.Errorf("lock(item: %v) call detected conflict", uuid)
	// }
	return nil
}

func (t *itemActionTracker[TK, TV]) deleteInactiveTrackedValuesInSeparateSegments(ctx context.Context) error {

	return nil
}

func (t *itemActionTracker[TK, TV]) formatKey(k string) string {
	return fmt.Sprintf("V%s", k)
}
