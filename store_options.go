package sop

import "time"

// StoreOptions contains field options settable when constructing a given (B-Tree).
type StoreOptions struct {
	// Short name of this (B-Tree store).
	Name string
	// Count of items that can be stored on a given node.
	SlotLength int
	// IsUnique tells whether key/value pair (items) of this tree should be unique on key.
	IsUnique bool
	// Set to true if you want "Value" data stored in the B-Tree node's data segment persisted together with the Keys.
	// Small size "Value" can benefit getting stored in Node's segment, but bigger data needs to be stored in its own segment(false)
	// not to impact performance.
	//
	// You don't need to bother with "IsValueDataActivelyPersisted" & "IsValueDataGloballyCached" if this is set to true.
	// Because if true, the "Value" is persisted part of the Node and since Node is cached in Redis, you get caching for free.
	// You get the ideal benefits not requiring the other two features which are designed for "Value" being persisted in its own segment.
	IsValueDataInNodeSegment bool
	// If true, each Btree Add(..) method call will persist the item value's data to another partition, then on commit,
	// it will then be a very quick action as item(s) values' data were already saved on backend.
	// This requires 'IsValueDataInNodeSegment' field to be set to false to work.
	IsValueDataActivelyPersisted bool
	// If true, the Value data will be cached in Redis, otherwise not. This is used when 'IsValueDataInNodeSegment'
	// is set to false. Typically set to false if 'IsValueDataActivelyPersisted' is true, as value data is expected
	// to be huge & to affect Redis performance due to the drastic size of data per item.
	IsValueDataGloballyCached bool
	// If true, during node is full scenario, instead of breaking the node in two to create space, item can get distributed
	// to sibling nodes with vacant slot(s). This increases density of the nodes but at the expense of potentially, more I/O.
	// This feature can be turned off if backend is impacted by the "balancing" act, i.e. - distribution can cause changes
	// to sibling nodes, thus, may increase I/O unnecessarily.
	LeafLoadBalancing bool
	// (optional) Description of the Store.
	Description string
	// For use by SOP in File System only. Specifies the base folder path of the blob store.
	BlobStoreBaseFolderPath string
	// Set to true to allow use of the store name as the blob store name. Useful for integrating with systems like AWS S3 where
	// strict bucket naming convention is applied.
	DisableBlobStoreFormatting bool
	// Set to true to allow use of the store name as the registry store name.
	DisableRegistryStoreFormatting bool
	// Redis cache specification for this store's objects(registry, nodes, item value part).
	// Defaults to the global specification and can be overriden for each store.
	CacheConfig *StoreCacheConfig
}

// ValueDataSize enumeration.
type ValueDataSize int

const (
	// SmallData means your item value data is small and can be stored in the Btree node segment together with keys.
	SmallData = iota
	// MediumData means your item value data is medium size and should be stored in separate segment than the Btree node.
	MediumData
	// BigData means your item value data is big in size and like MediumData, stored in separate segment but
	// is actively persisted and not globally cached as caching the big data will impact the local & global cache system(Redis).
	//
	// Is actively persisted means that for each "Add" or "Update" (and their variants) method call, Btree will persist
	// the item value's data to the backend storage & remove it from memory.
	BigData
)

var defaultCacheConfig StoreCacheConfig = StoreCacheConfig{
	StoreInfoCacheDuration: time.Duration(10 * time.Minute),
	RegistryCacheDuration:  time.Duration(15 * time.Minute),
	ValueDataCacheDuration: time.Duration(10 * time.Minute),
	// Nodes are bigger data, thus, we want them minimally cached. You can set to -1 (not cached) if needed.
	// NodeCacheDuration: time.Duration(5 * time.Minute),
}

// Assigns to the global default cache duration config.
func SetDefaultCacheConfig(cacheDuration StoreCacheConfig) {
	defaultCacheConfig = cacheDuration
}

// Returns the global default cache duration config.
func GetDefaulCacheConfig() StoreCacheConfig {
	return defaultCacheConfig
}

// Helper function to easily configure a store. Select the right valueDataSize matching your usage scenario.
// blobStoreBaseFolderPath is only used if storing blobs in File System. This specified the base folder path of the directory to contain the blobs.
//
// Caveat, pls. don't use the incorrect ValueDataSize in your usage scenario. For example, choosing BigData but actual item
// value data size can be small or medium size will cause unnecessary latency as SOP will not use global caching on your items'
// value data. On the contrary, if you use SmallData(or MediumData) but actual item value data size is big, then this will
// impact performance too. As SOP will use global & local cache in your items' value data that occupies huge space, impacting Redis,
// over-allocating it & the local (host) cache.
func ConfigureStore(storeName string, uniqueKey bool, slotLength int, description string, valueDataSize ValueDataSize, blobStoreBaseFolderPath string) StoreOptions {
	so := StoreOptions{
		Name:                     storeName,
		IsUnique:                 uniqueKey,
		SlotLength:               slotLength,
		IsValueDataInNodeSegment: true,
		Description:              description,
		BlobStoreBaseFolderPath:  blobStoreBaseFolderPath,
		CacheConfig: &StoreCacheConfig{
			RegistryCacheDuration:  defaultCacheConfig.RegistryCacheDuration,
			IsRegistryCacheTTL:     defaultCacheConfig.IsRegistryCacheTTL,
			NodeCacheDuration:      defaultCacheConfig.NodeCacheDuration,
			IsNodeCacheTTL:         defaultCacheConfig.IsNodeCacheTTL,
			ValueDataCacheDuration: defaultCacheConfig.ValueDataCacheDuration,
			IsValueDataCacheTTL:    defaultCacheConfig.IsValueDataCacheTTL,
			StoreInfoCacheDuration: defaultCacheConfig.StoreInfoCacheDuration,
			IsStoreInfoCacheTTL:    defaultCacheConfig.IsStoreInfoCacheTTL,
		},
	}
	if valueDataSize == MediumData {
		so.IsValueDataInNodeSegment = false
		so.IsValueDataGloballyCached = true
	}
	if valueDataSize == BigData {
		so.IsValueDataInNodeSegment = false
		so.IsValueDataGloballyCached = false
		so.IsValueDataActivelyPersisted = true
	}
	return so
}
