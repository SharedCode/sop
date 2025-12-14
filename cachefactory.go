package sop

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
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
type L2CacheFactory func(TransactionOptions) L2Cache

var cacheRegistry = make(map[L2CacheType]L2CacheFactory)
var cacheInstances = make(map[string]L2Cache)
var l2locker sync.Mutex

// RegisterL2CacheFactory registers a cache factory for a given type.
func RegisterL2CacheFactory(ct L2CacheType, f L2CacheFactory) {
	l2locker.Lock()
	defer l2locker.Unlock()
	cacheRegistry[ct] = f
}

// GetL2Cache gets the cache (client) for the specified type.
// It returns nil if no factory is registered for that type.
func GetL2Cache(options TransactionOptions) L2Cache {
	l2locker.Lock()
	defer l2locker.Unlock()

	key := getCacheKey(options)
	if instance, ok := cacheInstances[key]; ok {
		return instance
	}

	if f, ok := cacheRegistry[options.CacheType]; ok {
		instance := f(options)
		cacheInstances[key] = instance
		return instance
	}
	return nil
}

func getCacheKey(options TransactionOptions) string {
	if options.CacheType == Redis && options.RedisConfig != nil {
		var address, password string
		var db int
		if options.RedisConfig.URL != "" {
			u, err := url.Parse(options.RedisConfig.URL)
			if err == nil {
				address = u.Host
				password, _ = u.User.Password()
				path := strings.TrimPrefix(u.Path, "/")
				if path != "" {
					db, _ = strconv.Atoi(path)
				}
				return fmt.Sprintf("redis://%s@%s/%d", password, address, db)
			}
			// If parse fails, fall back to raw URL
			return options.RedisConfig.URL
		}
		return fmt.Sprintf("redis://%s@%s/%d", options.RedisConfig.Password, options.RedisConfig.Address, options.RedisConfig.DB)
	}
	return fmt.Sprintf("%d", options.CacheType)
}
