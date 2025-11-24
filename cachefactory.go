package sop

// CacheType defines the type of cache to use.
type CacheType int

const (
	// InMemory represents an in-memory cache.
	InMemory CacheType = iota
	// Redis represents a Redis cache.
	Redis
)

// CacheFactory defines the function signature for creating a cache client.
type CacheFactory func() Cache

var globalCacheFactory CacheFactory
var cacheRegistry = make(map[CacheType]CacheFactory)

// RegisterCache registers a cache factory for a given type.
func RegisterCache(t CacheType, f CacheFactory) {
	cacheRegistry[t] = f
}

// setCacheFactory sets the global cache factory function.
func setCacheFactory(f CacheFactory) {
	globalCacheFactory = f
}

// SetCacheFactory sets the global cache factory based on the provided type.
func SetCacheFactory(t CacheType) {
	if f, ok := cacheRegistry[t]; ok {
		setCacheFactory(f)
	}
}

// NewCacheClient creates a new cache client using the registered factory.
// It returns nil if no factory is registered.
func NewCacheClient() Cache {
	if globalCacheFactory == nil {
		return nil
	}
	return globalCacheFactory()
}
