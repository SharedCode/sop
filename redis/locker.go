package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/SharedCode/sop"
)

// Add prefix to the lock key so it becomes unique.
func (c client)FormatLockKey(k string) string {
	return fmt.Sprintf("L%s", k)
}

// Create a set of lock keys.
func (c client)CreateLockKeys(keys ...string) []*sop.LockKeys {
	lockKeys := make([]*sop.LockKeys, len(keys))
	for i := range keys {
		lockKeys[i] = &sop.LockKeys{
			// Prefix key with "L" to increase uniqueness.
			Key:    c.FormatLockKey(keys[i]),
			LockID: sop.NewUUID(),
		}
	}
	return lockKeys
}

// Lock a set of keys.
func(c client)Lock(ctx context.Context, duration time.Duration, lockKeys ...*sop.LockKeys) error {
	for _, lk := range lockKeys {
		readItem, err := c.Get(ctx, lk.Key)
		if err != nil {
			if !c.KeyNotFound(err) {
				return err
			}
			// Item does not exist, upsert it.
			if err := c.Set(ctx, lk.Key, lk.LockID.String(), duration); err != nil {
				return err
			}
			// Use a 2nd "get" to ensure we "won" the lock attempt & fail if not.
			if readItem2, err := c.Get(ctx, lk.Key); err != nil {
				return err
			} else if readItem2 != lk.LockID.String() {
				return fmt.Errorf("lock(key: %v) call detected conflict", lk.Key)
			}
			// We got the item locked, ensure we can unlock it.
			lk.IsLockOwner = true
			continue
		}
		// Item found in Redis.
		if readItem != lk.LockID.String() {
			return fmt.Errorf("lock(key: %v) call detected conflict", lk.Key)
		}
	}
	// Successfully locked.
	return nil
}

// Returns true if lockKeys have claimed lock equivalent.
func (c client)IsLocked(ctx context.Context, lockKeys ...*sop.LockKeys) error {
	for _, lk := range lockKeys {
		readItem, err := c.Get(ctx, lk.Key)
		if err != nil {
			if !c.KeyNotFound(err) {
				return err
			}
			// Not found means Is locked = false.
			return fmt.Errorf("IsLocked(key: %v) not found", lk.Key)
		}
		// Item found in Redis has different value, means key is locked by a different kind of function.
		if readItem != lk.LockID.String() {
			return fmt.Errorf("IsLocked(key: %v) locked by another", lk.Key)
		}
	}
	// Is locked = true.
	return nil
}

// Unlock a set of keys.
func (c client)Unlock(ctx context.Context, lockKeys ...*sop.LockKeys) error {
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
