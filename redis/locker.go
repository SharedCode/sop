package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/SharedCode/sop"
)

// LockKeys contain fields to allow locking and unlocking of a set of redis keys.
type LockKeys struct {
	key         string
	lockID      sop.UUID
	isLockOwner bool
}

// Add prefix to the lock key so it becomes unique.
func FormatLockKey(k string) string {
	return fmt.Sprintf("L%s", k)
}

// Create a set of lock keys.
func CreateLockKeys(keys ...string) []*LockKeys {
	lockKeys := make([]*LockKeys, len(keys))
	for i := range keys {
		lockKeys[i] = &LockKeys{
			// Prefix key with "L" to increase uniqueness.
			key:    FormatLockKey(keys[i]),
			lockID: sop.NewUUID(),
		}
	}
	return lockKeys
}

// Lock a set of keys.
func Lock(ctx context.Context, duration time.Duration, lockKeys ...*LockKeys) error {
	redisCache := NewClient()
	for _, lk := range lockKeys {
		readItem, err := redisCache.Get(ctx, lk.key)
		if err != nil {
			if !KeyNotFound(err) {
				return err
			}
			// Item does not exist, upsert it.
			if err := redisCache.Set(ctx, lk.key, lk.lockID.String(), duration); err != nil {
				return err
			}
			// Use a 2nd "get" to ensure we "won" the lock attempt & fail if not.
			if readItem2, err := redisCache.Get(ctx, lk.key); err != nil {
				return err
			} else if readItem2 != lk.lockID.String() {
				return fmt.Errorf("lock(key: %v) call detected conflict", lk.key)
			}
			// We got the item locked, ensure we can unlock it.
			lk.isLockOwner = true
			continue
		}
		// Item found in Redis.
		if readItem != lk.lockID.String() {
			return fmt.Errorf("lock(key: %v) call detected conflict", lk.key)
		}
	}
	// Successfully locked.
	return nil
}

// Returns true if lockKeys have claimed lock equivalent.
func IsLocked(ctx context.Context, lockKeys ...*LockKeys) error {
	redisCache := NewClient()
	for _, lk := range lockKeys {
		readItem, err := redisCache.Get(ctx, lk.key)
		if err != nil {
			if !KeyNotFound(err) {
				return err
			}
			return fmt.Errorf("IsLocked(key: %v) not found", lk.key)
		}
		// Item found in Redis.
		if readItem != lk.lockID.String() {
			return fmt.Errorf("IsLocked(key: %v) locked by another", lk.key)
		}
	}
	// Successfully locked.
	return nil
}

// Unlock a set of keys.
func Unlock(ctx context.Context, lockKeys ...*LockKeys) error {
	redisCache := NewClient()
	var lastErr error
	for _, lk := range lockKeys {
		if !lk.isLockOwner {
			continue
		}
		// Delete lock key if we own it.
		if err := redisCache.Delete(ctx, lk.key); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
