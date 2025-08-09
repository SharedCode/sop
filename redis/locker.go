package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/sharedcode/sop"
)

// IsLockedTTL reports whether all provided lock keys are owned by this process and
// extends their TTL by the specified duration when owned.
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

// Lock attempts to acquire locks for all provided keys using the given TTL duration.
// If any key is already locked by another owner, it returns false and that owner's UUID.
func (c client) Lock(ctx context.Context, duration time.Duration, lockKeys []*sop.LockKey) (bool, sop.UUID, error) {
	for _, lk := range lockKeys {
		found, readItem, err := c.Get(ctx, lk.Key)
		if err != nil {
			return false, sop.NilUUID, err
		}
		if found {
			// Item found in Redis, check if not ours. Most likely, but check anyway.
			if readItem != lk.LockID.String() {
				id, _ := sop.ParseUUID(readItem)
				return false, id, nil
			}
			continue
		}

		// Item does not exist, upsert it.
		if err := c.Set(ctx, lk.Key, lk.LockID.String(), duration); err != nil {
			return false, sop.NilUUID, err
		}
		// Use a 2nd "get" to ensure we "won" the lock attempt & fail if not.
		if found, readItem2, err := c.Get(ctx, lk.Key); !found || err != nil {
			return false, sop.NilUUID, err
		} else if readItem2 != lk.LockID.String() {
			id, _ := sop.ParseUUID(readItem)
			// Item found in Redis, lock attempt failed.
			return false, id, nil
		}
		// We got the item locked, ensure we can unlock it.
		lk.IsLockOwner = true
	}
	// Successfully locked.
	return true, sop.NilUUID, nil
}

// IsLocked reports whether all provided lock keys are currently owned by this process.
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

// IsLockedByOthers reports whether all given lock key names are currently locked by other processes.
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

// Unlock releases the provided lock keys, deleting only those owned by this process.
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

// CreateLockKeysForIDs builds lock keys from (name, lockID) tuples, applying the lock namespace prefix.
func (c client) CreateLockKeysForIDs(keys []sop.Tuple[string, sop.UUID]) []*sop.LockKey {
	lockKeys := make([]*sop.LockKey, len(keys))
	for i := range keys {
		lockKeys[i] = &sop.LockKey{
			// Prefix key with "L" to increase uniqueness.
			Key:    c.FormatLockKey(keys[i].First),
			LockID: keys[i].Second,
		}
	}
	return lockKeys
}

// CreateLockKeys creates lock keys using newly generated lock IDs for each provided key name.
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

// FormatLockKey prefixes the key with 'L' to form the namespaced Redis key used for locking.
func (c client) FormatLockKey(k string) string {
	return fmt.Sprintf("L%s", k)
}
