package sop

import (
	"sync"
)

// L2CacheType defines the type of cache to use.
type L2CacheType int

const (
	// Default represents no (L2) caching.
	NoCache L2CacheType = iota
	// InMemory represents an in-memory cache.
	InMemory
	// Redis represents a Redis cache.
	Redis
)

// L2CacheFactory defines the function signature for creating a cache client.
type L2CacheFactory func() L2Cache

var cacheRegistry = make(map[L2CacheType]L2Cache)
var l2locker sync.Mutex

// RegisterL2CacheFactory registers a cache factory for a given type.
func RegisterL2CacheFactory(ct L2CacheType, f L2CacheFactory) {
	l2locker.Lock()
	defer l2locker.Unlock()
	var l2c L2Cache
	if f != nil {
		l2c = f()
	}
	cacheRegistry[ct] = l2c
}

// GetL2Cache gets the cache (client) for the specified type.
// It returns nil if no factory is registered for that type.
func GetL2Cache(ct L2CacheType) L2Cache {
	if l2c, ok := cacheRegistry[ct]; ok {
		return l2c
	}

	l2locker.Lock()
	defer l2locker.Unlock()
	if l2c, ok := cacheRegistry[ct]; ok {
		return l2c
	}
	return nil
}
