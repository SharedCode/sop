package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"

)

// Create a set of lock records with given set of keys.
func CreateLockRecords(keys []string) []sop.KeyValuePair[string, btree.UUID] {
	lockRecords := make([]sop.KeyValuePair[string, btree.UUID], len(keys))
	for i := range keys {
		lockRecords[i] = sop.KeyValuePair[string, btree.UUID]{
			// Prefix key with "L" to increase uniqueness.
			Key: fmt.Sprintf("L%s", keys[i]),
			Value: btree.NewUUID(),
		}
	}
	return lockRecords
}

// Lock a set of records.
func Lock(ctx context.Context, duration time.Duration, lockRecords ...sop.KeyValuePair[string, btree.UUID]) error {
	redisCache := NewClient()
	for _, kvp := range lockRecords {
		readItem, err := redisCache.Get(ctx, kvp.Key)
		if err != nil {
			if !KeyNotFound(err) {
				return err
			}
			// Item does not exist, upsert it.
			if err := redisCache.Set(ctx, kvp.Key, kvp.Value.ToString(), duration); err != nil {
				return err
			}
			// Use a 2nd "get" to ensure we "won" the lock attempt & fail if not.
			if readItem2, err := redisCache.Get(ctx, kvp.Key); err != nil {
				return err
			} else if readItem2 != kvp.Value.ToString() {
				return fmt.Errorf("lock(item: %v) call detected conflict", kvp.Key)
			}
			continue
		}
		// Item found in Redis.
		if readItem != kvp.Value.ToString() {
			return fmt.Errorf("lock(item: %v) call detected conflict", kvp.Key)
		}
	}
	// Successfully locked.
	return nil
}

// Unlock a set of records.
func Unlock(ctx context.Context, lockRecords ...sop.KeyValuePair[string, btree.UUID]) error {
	redisCache := NewClient()
	var lastErr error
	for _, kvp := range lockRecords {
		if err := redisCache.Delete(ctx, kvp.Key); err != nil {
			if !KeyNotFound(err) {
				lastErr = err
			}
		}
	}
	return lastErr
}
