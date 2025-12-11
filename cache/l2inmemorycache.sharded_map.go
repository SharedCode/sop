package cache

import (
	"hash/fnv"
	"sync"
	"time"
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

func (m *shardedMap) load(key string) (interface{}, bool) {
	shard := m.getShard(key)
	shard.mu.RLock()
	val, ok := shard.items[key]
	shard.mu.RUnlock()
	return val, ok
}

func (m *shardedMap) store(key string, value interface{}) {
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

func (m *shardedMap) delete(key string) {
	shard := m.getShard(key)
	shard.mu.Lock()
	delete(shard.items, key)
	shard.mu.Unlock()
}

func (m *shardedMap) loadOrStore(key string, value interface{}) (actual interface{}, loaded bool) {
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

func (m *shardedMap) compareAndSwap(key string, old, new interface{}) bool {
	shard := m.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if val, ok := shard.items[key]; ok && val == old {
		shard.items[key] = new
		return true
	}
	return false
}

func (m *shardedMap) compareAndDelete(key string, old interface{}) bool {
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