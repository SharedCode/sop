package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/sharedcode/sop"
)

const (
	shardCount       = 256
	maxItemsPerShard = 1000 // Adjust based on desired total capacity (e.g., 256 * 1000 = 256k items)
)

type shard struct {
	mu    sync.RWMutex
	items map[string]interface{}
}

type shardedMap struct {
	shards [shardCount]*shard
}

func newShardedMap() *shardedMap {
	m := &shardedMap{}
	for i := 0; i < shardCount; i++ {
		m.shards[i] = &shard{items: make(map[string]interface{})}
	}
	return m
}

func (m *shardedMap) getShard(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return m.shards[h.Sum32()%shardCount]
}

func (m *shardedMap) Load(key string) (interface{}, bool) {
	shard := m.getShard(key)
	shard.mu.RLock()
	val, ok := shard.items[key]
	shard.mu.RUnlock()
	return val, ok
}

func (m *shardedMap) Store(key string, value interface{}) {
	shard := m.getShard(key)
	shard.mu.Lock()

	// Eviction logic: If over capacity, remove item with earliest expiration from a random sample
	if len(shard.items) >= maxItemsPerShard {
		const sampleSize = 5
		var victimKey string
		var minExp time.Time
		first := true

		count := 0
		for k, v := range shard.items {
			if count >= sampleSize {
				break
			}
			count++

			var exp time.Time
			switch val := v.(type) {
			case item:
				exp = val.expiration
			case lockItem:
				exp = val.expiration
			default:
				continue
			}

			// Treat Zero expiration as Infinite (do not evict if possible)
			effectiveExp := exp
			if exp.IsZero() {
				effectiveExp = time.Now().Add(365 * 24 * 100 * time.Hour) // +100 years
			}

			if first || effectiveExp.Before(minExp) {
				minExp = effectiveExp
				victimKey = k
				first = false
			}
		}

		if victimKey != "" {
			delete(shard.items, victimKey)
		} else {
			// Fallback: just delete the first one found if we couldn't determine expiration
			for k := range shard.items {
				delete(shard.items, k)
				break
			}
		}
	}

	shard.items[key] = value
	shard.mu.Unlock()
}

func (m *shardedMap) Delete(key string) {
	shard := m.getShard(key)
	shard.mu.Lock()
	delete(shard.items, key)
	shard.mu.Unlock()
}

func (m *shardedMap) LoadOrStore(key string, value interface{}) (actual interface{}, loaded bool) {
	shard := m.getShard(key)
	shard.mu.Lock()
	actual, loaded = shard.items[key]
	if !loaded {
		// Eviction logic
		if len(shard.items) >= maxItemsPerShard {
			const sampleSize = 5
			var victimKey string
			var minExp time.Time
			first := true

			count := 0
			for k, v := range shard.items {
				if count >= sampleSize {
					break
				}
				count++

				var exp time.Time
				switch val := v.(type) {
				case item:
					exp = val.expiration
				case lockItem:
					exp = val.expiration
				default:
					continue
				}

				// Treat Zero expiration as Infinite (do not evict if possible)
				effectiveExp := exp
				if exp.IsZero() {
					effectiveExp = time.Now().Add(365 * 24 * 100 * time.Hour) // +100 years
				}

				if first || effectiveExp.Before(minExp) {
					minExp = effectiveExp
					victimKey = k
					first = false
				}
			}

			if victimKey != "" {
				delete(shard.items, victimKey)
			} else {
				for k := range shard.items {
					delete(shard.items, k)
					break
				}
			}
		}
		actual = value
		shard.items[key] = value
	}
	shard.mu.Unlock()
	return actual, loaded
}

func (m *shardedMap) CompareAndSwap(key string, old, new interface{}) bool {
	shard := m.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if val, ok := shard.items[key]; ok && val == old {
		shard.items[key] = new
		return true
	}
	return false
}

func (m *shardedMap) CompareAndDelete(key string, old interface{}) bool {
	shard := m.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if val, ok := shard.items[key]; ok && val == old {
		delete(shard.items, key)
		return true
	}
	return false
}

func (m *shardedMap) Range(f func(key, value interface{}) bool) {
	for _, shard := range m.shards {
		shard.mu.RLock()
		// Copy items to avoid holding lock during callback if possible,
		// but Range usually allows concurrent access.
		// For safety with long-running callbacks, we might want to copy keys.
		// But standard sync.Map Range holds no locks during callback?
		// Actually sync.Map Range is complex.
		// Here we hold RLock. If callback calls Store, it will deadlock.
		// So we should collect items then callback.
		items := make(map[string]interface{}, len(shard.items))
		for k, v := range shard.items {
			items[k] = v
		}
		shard.mu.RUnlock()

		for k, v := range items {
			if !f(k, v) {
				return
			}
		}
	}
}

type item struct {
	data       []byte
	expiration time.Time
}

type lockItem struct {
	lockID     sop.UUID
	expiration time.Time
}

type L2InMemoryCache struct {
	data  *shardedMap
	locks *shardedMap
}

func NewL2InMemoryCache() sop.L2Cache {
	return &L2InMemoryCache{
		data:  newShardedMap(),
		locks: newShardedMap(),
	}
}

// Returns InMemoryCache as L2Cache type.
func (c *L2InMemoryCache) GetType() sop.L2CacheType {
	return sop.InMemory
}

func (c *L2InMemoryCache) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	var exp time.Time
	if expiration > 0 {
		exp = time.Now().Add(expiration)
	}

	c.data.Store(key, item{
		data:       []byte(value),
		expiration: exp,
	})
	return nil
}

func (c *L2InMemoryCache) Get(ctx context.Context, key string) (bool, string, error) {
	val, ok := c.data.Load(key)
	if !ok {
		return false, "", nil
	}
	it := val.(item)

	if !it.expiration.IsZero() && time.Now().After(it.expiration) {
		c.data.Delete(key)
		return false, "", nil
	}

	return true, string(it.data), nil
}

func (c *L2InMemoryCache) GetEx(ctx context.Context, key string, expiration time.Duration) (bool, string, error) {
	val, ok := c.data.Load(key)
	if !ok {
		return false, "", nil
	}
	it := val.(item)

	if !it.expiration.IsZero() && time.Now().After(it.expiration) {
		c.data.Delete(key)
		return false, "", nil
	}

	if expiration > 0 {
		it.expiration = time.Now().Add(expiration)
		c.data.Store(key, it)
	}

	return true, string(it.data), nil
}

func (c *L2InMemoryCache) IsRestarted(ctx context.Context) bool {
	return false
}

func (c *L2InMemoryCache) SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	var exp time.Time
	if expiration > 0 {
		exp = time.Now().Add(expiration)
	}

	c.data.Store(key, item{
		data:       data,
		expiration: exp,
	})
	return nil
}

func (c *L2InMemoryCache) GetStruct(ctx context.Context, key string, target interface{}) (bool, error) {
	val, ok := c.data.Load(key)
	if !ok {
		return false, nil
	}
	it := val.(item)

	if !it.expiration.IsZero() && time.Now().After(it.expiration) {
		c.data.Delete(key)
		return false, nil
	}

	if err := json.Unmarshal(it.data, target); err != nil {
		return false, err
	}

	return true, nil
}

func (c *L2InMemoryCache) GetStructEx(ctx context.Context, key string, target interface{}, expiration time.Duration) (bool, error) {
	val, ok := c.data.Load(key)
	if !ok {
		return false, nil
	}
	it := val.(item)

	if !it.expiration.IsZero() && time.Now().After(it.expiration) {
		c.data.Delete(key)
		return false, nil
	}

	if expiration > 0 {
		it.expiration = time.Now().Add(expiration)
		c.data.Store(key, it)
	}

	if err := json.Unmarshal(it.data, target); err != nil {
		return false, err
	}

	return true, nil
}

func (c *L2InMemoryCache) Delete(ctx context.Context, keys []string) (bool, error) {
	for _, k := range keys {
		c.data.Delete(k)
	}
	return true, nil
}

func (c *L2InMemoryCache) Ping(ctx context.Context) error {
	return nil
}

func (c *L2InMemoryCache) Clear(ctx context.Context) error {
	c.data.Range(func(key, value interface{}) bool {
		c.data.Delete(key.(string))
		return true
	})
	return nil
}

func (c *L2InMemoryCache) Info(ctx context.Context, section string) (string, error) {
	return "InMemoryCache", nil
}

// Locking implementation

func (c *L2InMemoryCache) FormatLockKey(k string) string {
	return fmt.Sprintf("lock:%s", k)
}

func (c *L2InMemoryCache) CreateLockKeys(keys []string) []*sop.LockKey {
	locks := make([]*sop.LockKey, len(keys))
	for i, k := range keys {
		locks[i] = &sop.LockKey{
			Key:    c.FormatLockKey(k),
			LockID: sop.NewUUID(),
		}
	}
	return locks
}

func (c *L2InMemoryCache) CreateLockKeysForIDs(keys []sop.Tuple[string, sop.UUID]) []*sop.LockKey {
	locks := make([]*sop.LockKey, len(keys))
	for i, k := range keys {
		locks[i] = &sop.LockKey{
			Key:    c.FormatLockKey(fmt.Sprintf("%s:%v", k.First, k.Second)),
			LockID: sop.NewUUID(),
		}
	}
	return locks
}

func (c *L2InMemoryCache) IsLockedTTL(ctx context.Context, duration time.Duration, lockKeys []*sop.LockKey) (bool, error) {
	// 1. Check if all keys are locked by us and valid
	for _, lk := range lockKeys {
		val, ok := c.locks.Load(lk.Key)
		if !ok {
			return false, nil
		}
		item := val.(lockItem)
		if item.lockID != lk.LockID {
			return false, nil
		}
		if time.Now().After(item.expiration) {
			c.locks.CompareAndDelete(lk.Key, val)
			return false, nil
		}
	}

	// 2. Refresh TTL
	newExp := time.Now().Add(duration)
	for _, lk := range lockKeys {
		for {
			val, ok := c.locks.Load(lk.Key)
			if !ok {
				return false, nil
			}
			item := val.(lockItem)
			if item.lockID != lk.LockID {
				return false, nil
			}
			newItem := lockItem{
				lockID:     item.lockID,
				expiration: newExp,
			}
			if c.locks.CompareAndSwap(lk.Key, item, newItem) {
				break
			}
		}
	}

	return true, nil
}

func (c *L2InMemoryCache) Lock(ctx context.Context, duration time.Duration, lockKeys []*sop.LockKey) (bool, sop.UUID, error) {
	if duration <= 0 {
		duration = 15 * time.Minute
	}

	// Sort keys to avoid deadlocks/livelocks (A->B vs B->A)
	sort.Slice(lockKeys, func(i, j int) bool {
		return lockKeys[i].Key < lockKeys[j].Key
	})

	acquired := make([]*sop.LockKey, 0, len(lockKeys))

	for _, lk := range lockKeys {
		newItem := lockItem{
			lockID:     lk.LockID,
			expiration: time.Now().Add(duration),
		}

		// Try to load or store
		val, loaded := c.locks.LoadOrStore(lk.Key, newItem)
		if loaded {
			// Item exists
			existing := val.(lockItem)

			// Check if expired
			if time.Now().After(existing.expiration) {
				// Expired. Try to CAS.
				if c.locks.CompareAndSwap(lk.Key, existing, newItem) {
					// Success
					acquired = append(acquired, lk)
					lk.IsLockOwner = true
					continue
				}
				// CAS failed, someone else took it.
				// Fall through to failure.
			} else {
				// Not expired. Check re-entry.
				if existing.lockID == lk.LockID {
					// Already owned by us.
					lk.IsLockOwner = true
					continue
				}
			}

			// Failed to acquire. Rollback newly acquired locks.
			for _, acquiredLk := range acquired {
				if v, ok := c.locks.Load(acquiredLk.Key); ok {
					if v.(lockItem).lockID == acquiredLk.LockID {
						c.locks.CompareAndDelete(acquiredLk.Key, v)
					}
				}
				acquiredLk.IsLockOwner = false
			}
			return false, existing.lockID, nil
		}

		// Success (Stored)
		acquired = append(acquired, lk)
		lk.IsLockOwner = true
	}

	return true, sop.NilUUID, nil
}

func (c *L2InMemoryCache) DualLock(ctx context.Context, duration time.Duration, lockKeys []*sop.LockKey) (bool, sop.UUID, error) {
	return c.Lock(ctx, duration, lockKeys)
}

func (c *L2InMemoryCache) IsLocked(ctx context.Context, lockKeys []*sop.LockKey) (bool, error) {
	for _, lk := range lockKeys {
		val, ok := c.locks.Load(lk.Key)
		if !ok {
			return false, nil
		}
		item := val.(lockItem)
		if item.lockID != lk.LockID {
			return false, nil
		}
		if time.Now().After(item.expiration) {
			return false, nil
		}
	}
	return true, nil
}

func (c *L2InMemoryCache) IsLockedByOthers(ctx context.Context, lockKeyNames []string) (bool, error) {
	for _, key := range lockKeyNames {
		val, ok := c.locks.Load(key)
		if ok {
			item := val.(lockItem)
			if time.Now().After(item.expiration) {
				continue
			}
			return true, nil
		}
	}
	return false, nil
}

func (c *L2InMemoryCache) Unlock(ctx context.Context, lockKeys []*sop.LockKey) error {
	for _, lk := range lockKeys {
		val, ok := c.locks.Load(lk.Key)
		if ok {
			item := val.(lockItem)
			if item.lockID == lk.LockID {
				c.locks.CompareAndDelete(lk.Key, val)
			}
		}
	}
	return nil
}

func init() {
	sop.RegisterL2CacheFactory(sop.InMemory, NewL2InMemoryCache)
}
