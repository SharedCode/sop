package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/sharedcode/sop"
)

type item struct {
	data       []byte
	expiration time.Time
}

type lockItem struct {
	lockID     sop.UUID
	expiration time.Time
}

type InMemoryCache struct {
	mu    sync.RWMutex
	mru   Cache[string, item]
	locks map[string]lockItem
}

func NewInMemoryCache() sop.L2Cache {
	return &InMemoryCache{
		mru:   NewCache[string, item](1000, 10000), // Default capacity
		locks: make(map[string]lockItem),
	}
}

func (c *InMemoryCache) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var exp time.Time
	if expiration > 0 {
		exp = time.Now().Add(expiration)
	}

	c.mru.Set([]sop.KeyValuePair[string, item]{
		{
			Key: key,
			Value: item{
				data:       []byte(value),
				expiration: exp,
			},
		},
	})
	return nil
}

func (c *InMemoryCache) Get(ctx context.Context, key string) (bool, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	items := c.mru.Get([]string{key})
	if len(items) == 0 {
		return false, "", nil
	}
	it := items[0]
	if it.data == nil {
		return false, "", nil
	}

	if !it.expiration.IsZero() && time.Now().After(it.expiration) {
		c.mru.Delete([]string{key})
		return false, "", nil
	}

	return true, string(it.data), nil
}

func (c *InMemoryCache) GetEx(ctx context.Context, key string, expiration time.Duration) (bool, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	items := c.mru.Get([]string{key})
	if len(items) == 0 {
		return false, "", nil
	}
	it := items[0]
	if it.data == nil {
		return false, "", nil
	}

	if !it.expiration.IsZero() && time.Now().After(it.expiration) {
		c.mru.Delete([]string{key})
		return false, "", nil
	}

	if expiration > 0 {
		it.expiration = time.Now().Add(expiration)
		c.mru.Set([]sop.KeyValuePair[string, item]{
			{Key: key, Value: it},
		})
	}

	return true, string(it.data), nil
}

func (c *InMemoryCache) IsRestarted(ctx context.Context) bool {
	return false
}

func (c *InMemoryCache) SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var exp time.Time
	if expiration > 0 {
		exp = time.Now().Add(expiration)
	}

	c.mru.Set([]sop.KeyValuePair[string, item]{
		{
			Key: key,
			Value: item{
				data:       data,
				expiration: exp,
			},
		},
	})
	return nil
}

func (c *InMemoryCache) GetStruct(ctx context.Context, key string, target interface{}) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	items := c.mru.Get([]string{key})
	if len(items) == 0 {
		return false, nil
	}
	it := items[0]
	if it.data == nil {
		return false, nil
	}

	if !it.expiration.IsZero() && time.Now().After(it.expiration) {
		c.mru.Delete([]string{key})
		return false, nil
	}

	if err := json.Unmarshal(it.data, target); err != nil {
		return false, err
	}

	return true, nil
}

func (c *InMemoryCache) GetStructEx(ctx context.Context, key string, target interface{}, expiration time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	items := c.mru.Get([]string{key})
	if len(items) == 0 {
		return false, nil
	}
	it := items[0]
	if it.data == nil {
		return false, nil
	}

	if !it.expiration.IsZero() && time.Now().After(it.expiration) {
		c.mru.Delete([]string{key})
		return false, nil
	}

	if expiration > 0 {
		it.expiration = time.Now().Add(expiration)
		c.mru.Set([]sop.KeyValuePair[string, item]{
			{Key: key, Value: it},
		})
	}

	if err := json.Unmarshal(it.data, target); err != nil {
		return false, err
	}

	return true, nil
}

func (c *InMemoryCache) Delete(ctx context.Context, keys []string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mru.Delete(keys)
	return true, nil
}

func (c *InMemoryCache) Ping(ctx context.Context) error {
	return nil
}

func (c *InMemoryCache) Clear(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mru.Clear()
	return nil
}

func (c *InMemoryCache) Info(ctx context.Context, section string) (string, error) {
	return "InMemoryCache", nil
}

// Locking implementation

func (c *InMemoryCache) FormatLockKey(k string) string {
	return fmt.Sprintf("lock:%s", k)
}

func (c *InMemoryCache) CreateLockKeys(keys []string) []*sop.LockKey {
	locks := make([]*sop.LockKey, len(keys))
	for i, k := range keys {
		locks[i] = &sop.LockKey{
			Key:    c.FormatLockKey(k),
			LockID: sop.NewUUID(),
		}
	}
	return locks
}

func (c *InMemoryCache) CreateLockKeysForIDs(keys []sop.Tuple[string, sop.UUID]) []*sop.LockKey {
	locks := make([]*sop.LockKey, len(keys))
	for i, k := range keys {
		locks[i] = &sop.LockKey{
			Key:    c.FormatLockKey(fmt.Sprintf("%s:%v", k.First, k.Second)),
			LockID: sop.NewUUID(),
		}
	}
	return locks
}

func (c *InMemoryCache) IsLockedTTL(ctx context.Context, duration time.Duration, lockKeys []*sop.LockKey) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if all keys are locked by us and valid
	for _, lk := range lockKeys {
		item, exists := c.locks[lk.Key]
		if !exists {
			return false, nil
		}
		if item.lockID != lk.LockID {
			return false, nil
		}
		if time.Now().After(item.expiration) {
			delete(c.locks, lk.Key)
			return false, nil
		}
	}

	// Refresh TTL
	newExp := time.Now().Add(duration)
	for _, lk := range lockKeys {
		c.locks[lk.Key] = lockItem{
			lockID:     lk.LockID,
			expiration: newExp,
		}
	}

	return true, nil
}

func (c *InMemoryCache) Lock(ctx context.Context, duration time.Duration, lockKeys []*sop.LockKey) (bool, sop.UUID, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 1. Check if any key is already locked by someone else
	for _, lk := range lockKeys {
		item, exists := c.locks[lk.Key]
		if exists {
			if time.Now().After(item.expiration) {
				// Expired, clean it up
				delete(c.locks, lk.Key)
				continue
			}
			// Active lock exists
			return false, sop.NilUUID, nil
		}
	}

	// 2. Acquire locks
	lockID := sop.NewUUID()
	exp := time.Now().Add(duration)

	for _, lk := range lockKeys {
		c.locks[lk.Key] = lockItem{
			lockID:     lockID,
			expiration: exp,
		}
		lk.LockID = lockID
		lk.IsLockOwner = true
	}

	return true, lockID, nil
}

func (c *InMemoryCache) DualLock(ctx context.Context, duration time.Duration, lockKeys []*sop.LockKey) (bool, sop.UUID, error) {
	return c.Lock(ctx, duration, lockKeys)
}

func (c *InMemoryCache) IsLocked(ctx context.Context, lockKeys []*sop.LockKey) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, lk := range lockKeys {
		item, exists := c.locks[lk.Key]
		if !exists {
			return false, nil
		}
		if item.lockID != lk.LockID {
			return false, nil
		}
		if time.Now().After(item.expiration) {
			return false, nil
		}
	}
	return true, nil
}

func (c *InMemoryCache) IsLockedByOthers(ctx context.Context, lockKeyNames []string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, key := range lockKeyNames {
		item, exists := c.locks[key]
		if exists {
			if time.Now().After(item.expiration) {
				continue
			}
			return true, nil
		}
	}
	return false, nil
}

func (c *InMemoryCache) Unlock(ctx context.Context, lockKeys []*sop.LockKey) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, lk := range lockKeys {
		item, exists := c.locks[lk.Key]
		if exists && item.lockID == lk.LockID {
			delete(c.locks, lk.Key)
		}
	}
	return nil
}

func init() {
	sop.RegisterCacheFactory(sop.InMemory, NewInMemoryCache)
}
