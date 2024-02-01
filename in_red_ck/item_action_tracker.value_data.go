package in_red_ck

import (
	"context"
)

func (t *itemActionTracker[TK, TV]) commitTrackedValuesToSeparateSegments(ctx context.Context) error {
	for _, cachedItem := range t.items {
		if cachedItem.item.Value == nil {
			continue
		}
		if err := t.manageItemValueData(ctx, cachedItem); err != nil {
			return err
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

func  (t *itemActionTracker[TK, TV])manageItemValueData(ctx context.Context, cachedItem cacheItem[TK, TV]) error {
		/*
			getAction
			addAction
			updateAction
			removeAction
		*/
		// if cachedItem.Action == addAction || cachedItem.Action == updateAction {

		// 	continue
		// }
		// if cachedItem.Action == getAction {

		// 	continue
		// }
		// if cachedItem.Action == removeAction {
		// 	if err := t.redisCache.Delete(ctx, t.formatKey(uuid.String())); err != nil && !redis.KeyNotFound(err) {
		// 		return err
		// 	}

		// }
	return nil
}
