package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/SharedCode/sop"
)

// Add prefix to the lock key so it becomes unique.
func (c client) FormatLockKey(k string) string {
	return fmt.Sprintf("L%s", k)
}

// Create a set of lock keys.
func (c client) CreateLockKeys(keys ...string) []*sop.LockKey {
	lockKeys := make([]*sop.LockKey, len(keys))
	for i := range keys {
		lockKeys[i] = &sop.LockKey{
			// Prefix key with "L" to increase uniqueness.
			Key:    c.FormatLockKey(keys[i]),
			LockID: sop.NewUUID(),
		}
	}
	return lockKeys
}

// Lock a set of keys.
func (c client) Lock(ctx context.Context, duration time.Duration, lockKeys ...*sop.LockKey) (bool, error) {
	for _, lk := range lockKeys {
		readItem, err := c.Get(ctx, lk.Key)
		if err != nil {
			if !c.KeyNotFound(err) {
				return false, err
			}
			// Item does not exist, upsert it.
			if err := c.Set(ctx, lk.Key, lk.LockID.String(), duration); err != nil {
				return false, err
			}
			// Use a 2nd "get" to ensure we "won" the lock attempt & fail if not.
			if readItem2, err := c.Get(ctx, lk.Key); err != nil {
				return false, err
			} else if readItem2 != lk.LockID.String() {
				// Item found in Redis, lock attempt failed.
				return false, nil
			}
			// We got the item locked, ensure we can unlock it.
			lk.IsLockOwner = true
			continue
		}
		// Item found in Redis, lock attempt failed.
		if readItem != lk.LockID.String() {
			return false, nil
		}
	}
	// Successfully locked.
	return true, nil
}

// Returns true if lockKeys have claimed lock equivalent.
func (c client) IsLocked(ctx context.Context, lockKeys ...*sop.LockKey) (bool, error) {
	for _, lk := range lockKeys {
		readItem, err := c.Get(ctx, lk.Key)
		if err != nil {
			if c.KeyNotFound(err) {
				// Not found means Is locked = false.
				return false, nil
			}
			return false, err
		}
		// Item found in Redis has different value, means key is locked by a different kind of function.
		if readItem != lk.LockID.String() {
			// Not found means Is locked = false.
			return false, nil
		}
	}
	// Is locked = true.
	return true, nil
}

// Returns true if lockKeyNames are all locked.
func (c client) IsLockedByOthers(ctx context.Context, lockKeyNames ...string) (bool, error) {
	if len(lockKeyNames) == 0 {
		return false, nil
	}
	for _, lkn := range lockKeyNames {
		_, err := c.Get(ctx, lkn)
		if err != nil {
			if c.KeyNotFound(err) {
				return false, nil
			}
			return false, err
		}
		// Item found in Redis means other process has a lock on it.
	}
	return true, nil
}

// Unlock a set of keys.
func (c client) Unlock(ctx context.Context, lockKeys ...*sop.LockKey) error {
	var lastErr error
	for _, lk := range lockKeys {
		if !lk.IsLockOwner {
			continue
		}
		// Delete lock key if we own it.
		if err := c.Delete(ctx, lk.Key); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
