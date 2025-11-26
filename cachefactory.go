package sop

// CacheType defines the type of cache to use.
type CacheType int

const (
	// Default represents no (L2) caching.
	NoCache CacheType = iota
	// InMemory represents an in-memory cache.
	InMemory
	// Redis represents a Redis cache.
	Redis
)

// CacheFactory defines the function signature for creating a cache client.
type CacheFactory func() Cache

var globalCacheFactory CacheFactory
var globalCacheFactoryType CacheType
var cacheRegistry = make(map[CacheType]CacheFactory)

// RegisterCacheFactory registers a cache factory for a given type.
func RegisterCacheFactory(t CacheType, f CacheFactory) {
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
		globalCacheFactoryType = t
	}
}

// GetCacheFactoryType returns the currently registered cache factory type.
func GetCacheFactoryType() CacheType {
	return globalCacheFactoryType
}

// NewCacheClient creates a new cache client using the registered factory.
// It returns nil if no factory is registered.
func NewCacheClient() Cache {
	if globalCacheFactory == nil {
		return nil
	}
	return globalCacheFactory()
}
