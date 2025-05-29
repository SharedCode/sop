package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/SharedCode/sop"
)

// Lock a set of keys but with TTL attribute, i.e. - everytime IsLocked is invoked, it extends the lifetime
// of the locked key(s). This is useful for long term held locks, e.g. - table level locks that can span
// within entire life of the transaction, not just the commit part.
func (c client) LockTTL(ctx context.Context, duration time.Duration, lockKeys []*sop.LockKey) (bool, error) {
	for _, lk := range lockKeys {
		found, readItem, err := c.GetEx(ctx, lk.Key, duration)
		if !found || err != nil {
			if err != nil {
				return false, err
			}
			// Item does not exist, upsert it.
			if err := c.Set(ctx, lk.Key, lk.LockID.String(), duration); err != nil {
				return false, err
			}
			// Use a 2nd "get" to ensure we "won" the lock attempt & fail if not.
			if found, readItem2, err := c.GetEx(ctx, lk.Key, duration); !found || err != nil {
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

// Returns true if lockKeys have claimed lock equivalent. And extends the lock by another 30 seconds for each call (TTL).
func (c client) IsLockedTTL(ctx context.Context, duration time.Duration, lockKeys []*sop.LockKey) (bool, error) {
	r := true
	var lastErr error
	for _, lk := range lockKeys {
		found, readItem, err := c.GetEx(ctx, lk.Key, duration)
		if !found || err != nil {
			lk.IsLockOwner = false
			r = false
			if err != nil {
				lastErr = err
			}
			continue
		}
		// Item found in Redis has different value, means key is locked by a different kind of function.
		if readItem != lk.LockID.String() {
			lk.IsLockOwner = false
			r = false
			continue
		}
		lk.IsLockOwner = true
	}
	// Is locked = true.
	return r, lastErr
}

// Lock a set of keys.
func (c client) Lock(ctx context.Context, duration time.Duration, lockKeys []*sop.LockKey) (bool, error) {
	for _, lk := range lockKeys {
		found, readItem, err := c.Get(ctx, lk.Key)
		if !found || err != nil {
			if err != nil {
				return false, err
			}
			// Item does not exist, upsert it.
			if err := c.Set(ctx, lk.Key, lk.LockID.String(), duration); err != nil {
				return false, err
			}
			// Use a 2nd "get" to ensure we "won" the lock attempt & fail if not.
			if found, readItem2, err := c.Get(ctx, lk.Key); !found || err != nil {
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
func (c client) IsLocked(ctx context.Context, lockKeys []*sop.LockKey) (bool, error) {
	r := true
	var lastErr error
	for _, lk := range lockKeys {
		found, readItem, err := c.Get(ctx, lk.Key)
		if !found || err != nil {
			lk.IsLockOwner = false
			r = false
			if err != nil {
				lastErr = err
			}
			continue
		}
		// Item found in Redis has different value, means key is locked by a different kind of function.
		if readItem != lk.LockID.String() {
			lk.IsLockOwner = false
			r = false
			continue
		}
		lk.IsLockOwner = true
	}
	// Is locked = true.
	return r, lastErr
}

// Returns true if lockKeyNames are all locked.
func (c client) IsLockedByOthers(ctx context.Context, lockKeyNames []string) (bool, error) {
	if len(lockKeyNames) == 0 {
		return false, nil
	}
	for _, lkn := range lockKeyNames {
		found, _, err := c.Get(ctx, lkn)
		if !found || err != nil {
			return false, err
		}
		// Item found in Redis means other process has a lock on it.
	}
	return true, nil
}

// Unlock a set of keys.
func (c client) Unlock(ctx context.Context, lockKeys []*sop.LockKey) error {
	var lastErr error
	for _, lk := range lockKeys {
		if !lk.IsLockOwner {
			continue
		}
		// Delete lock key if we own it.
		if found, err := c.Delete(ctx, []string{lk.Key}); !found || err != nil {
			// Ignore if key not in cache, not an issue.
			if err == nil {
				continue
			}
			lastErr = err
		}
	}
	return lastErr
}

// Create a set of lock keys.
func (c client) CreateLockKeys(keys []string) []*sop.LockKey {
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

// Add prefix to the lock key so it becomes unique.
func (c client) FormatLockKey(k string) string {
	return fmt.Sprintf("L%s", k)
}
