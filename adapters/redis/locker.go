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
	conn, err := c.getConnection()
	if err != nil {
		return false, sop.NilUUID, err
	}
	for _, lk := range lockKeys {
		// Try to set the lock using SetNX
		set, err := conn.Client.SetNX(ctx, lk.Key, lk.LockID.String(), duration).Result()
		if err != nil {
			return false, sop.NilUUID, err
		}
		if set {
			lk.IsLockOwner = true
			continue
		}

		// If failed to set, check if we already own it
		found, readItem, err := c.Get(ctx, lk.Key)
		if err != nil {
			return false, sop.NilUUID, err
		}
		if found {
			if readItem == lk.LockID.String() {
				// We already own it.
				lk.IsLockOwner = true
				continue
			}
			// Owned by someone else
			id, _ := sop.ParseUUID(readItem)
			return false, id, nil
		}
		// If not found (expired between SetNX and Get?), return false to let caller retry.
		return false, sop.NilUUID, nil
	}
	// Successfully locked.
	return true, sop.NilUUID, nil
}

// DualLock attempts to acquire locks for all provided keys using the given TTL duration.
// It calls Lock then IsLocked to ensure the lock is acquired and persisted.
func (c client) DualLock(ctx context.Context, duration time.Duration, lockKeys []*sop.LockKey) (bool, sop.UUID, error) {
	ok, owner, err := c.Lock(ctx, duration, lockKeys)
	if err != nil || !ok {
		return ok, owner, err
	}
	// Verify lock acquisition
	isLocked, err := c.IsLocked(ctx, lockKeys)
	if err != nil {
		return false, sop.NilUUID, err
	}
	if !isLocked {
		return false, sop.NilUUID, nil
	}
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
