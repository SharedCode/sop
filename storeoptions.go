package sop

import "time"

// StoreOptions contains configuration fields used when creating a B-Tree store.
type StoreOptions struct {
	// Name is the short name of the store.
	Name string
	// SlotLength is the number of items that can be stored in a node.
	SlotLength int
	// IsUnique enforces uniqueness on keys.
	IsUnique bool
	// IsValueDataInNodeSegment stores Value data within the B-Tree node segment when true.
	// Smaller Value data benefits from this for locality; bigger data should be stored separately.
	// If true, IsValueDataActivelyPersisted and IsValueDataGloballyCached are ignored.
	IsValueDataInNodeSegment bool
	// IsValueDataActivelyPersisted persists Value data to a separate partition on Add/Update and expects
	// IsValueDataInNodeSegment to be false.
	IsValueDataActivelyPersisted bool
	// IsValueDataGloballyCached enables Redis caching for Value data when IsValueDataInNodeSegment is false.
	IsValueDataGloballyCached bool
	// LeafLoadBalancing allows distributing items to sibling nodes when there is capacity to avoid splits.
	LeafLoadBalancing bool
	// Description is an optional text describing the store.
	Description string
	// BlobStoreBaseFolderPath specifies a base folder path when using the filesystem blob store.
	BlobStoreBaseFolderPath string
	// DisableBlobStoreFormatting uses the store name directly as the blob store name (useful for S3-like systems).
	DisableBlobStoreFormatting bool
	// DisableRegistryStoreFormatting uses the store name directly as the registry store name.
	DisableRegistryStoreFormatting bool
	// CacheConfig overrides global cache durations and TTL behavior per store.
	CacheConfig *StoreCacheConfig

	// CELexpression specifies the CEL expression used as comparer for keys.
	CELexpression string
	// MapKeyIndexSpecification contains a CEL or index specification used by the comparer.
	MapKeyIndexSpecification string
	// IsPrimitiveKey hints Python bindings which JSON B-Tree type to instantiate during Open.
	IsPrimitiveKey bool
}

// ValueDataSize categorizes the expected size of Value data to guide configuration helpers.
type ValueDataSize int

const (
	// SmallData indicates small Value data that can be stored within the node segment.
	SmallData ValueDataSize = iota
	// MediumData indicates medium Value data that should be stored in a separate segment.
	MediumData
	// BigData indicates large Value data stored separately, actively persisted and typically not globally cached.
	BigData
)

var defaultCacheConfig StoreCacheConfig = StoreCacheConfig{
	StoreInfoCacheDuration: time.Duration(10 * time.Minute),
	RegistryCacheDuration:  time.Duration(15 * time.Minute),
	ValueDataCacheDuration: time.Duration(10 * time.Minute),
	// Nodes are larger data; keep cache modest while still enabling merge orchestration via Redis.
	NodeCacheDuration: time.Duration(5 * time.Minute),
}

// SetDefaultCacheConfig assigns the global default cache configuration used when a store does not override it.
func SetDefaultCacheConfig(cacheDuration StoreCacheConfig) {
	defaultCacheConfig = cacheDuration
}

// GetDefaultCacheConfig returns the global default cache configuration.
func GetDefaultCacheConfig() StoreCacheConfig {
	return defaultCacheConfig
}

// ConfigureStore returns StoreOptions tuned according to the expected ValueDataSize.
// Choose carefully: mismatched size can hurt performance by over/under caching or persisting.
// blobStoreBaseFolderPath is used only for filesystem blob storage as a base directory.
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
