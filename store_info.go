package sop

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// StoreInfo contains a given (B-Tree) store details.
type StoreInfo struct {
	// Short name of this (B-Tree store).
	Name string `json:"name" minLength:"1" maxLength:"20"`
	// Count of items that can be stored on a given node.
	SlotLength int `json:"slot_length" min:"2" max:"10000"`
	// IsUnique tells whether key/value pair (items) of this tree should be unique on key.
	IsUnique bool `json:"is_unique"`
	// (optional) Description of the Store.
	Description string `json:"description" maxLength:"500"`
	// Virtual ID registry table name.
	RegistryTable string `json:"registry_table" minLength:"1" maxLength:"20"`
	// Blob table name if using a table or (base) file path if storing blobs in File System.
	BlobTable string `json:"blob_table" minLength:"1" maxLength:"300"`
	// RootNodeID is the root node's ID.
	RootNodeID UUID `json:"root_node_id"`
	// Total count of items stored.
	Count int64 `json:"count"`
	// Used internally by SOP. Should be ignored when persisted in the backend.
	CountDelta int64 `json:"-"`
	// Add or update timestamp in milliseconds.
	Timestamp int64 `json:"timestamp"`
	// IsValueDataInNodeSegment is true if "Value" data is stored in the B-Tree node's data segment.
	// Otherwise is false.
	IsValueDataInNodeSegment bool `json:"is_value_data_in_node_segment"`
	// If true, each Btree Add(..) method call will persist the item value's data to another partition, then on commit,
	// it will then be a very quick action as item(s) values' data were already saved on backend.
	// This rquires 'IsValueDataInNodeSegment' field to be set to false to work.
	IsValueDataActivelyPersisted bool `json:"is_value_data_actively_persisted"`
	// If true, the Value data will be cached in Redis, otherwise not. This is used when 'IsValueDataInNodeSegment'
	// is set to false. Typically set to false if 'IsValueDataActivelyPersisted' is true, as value data is expected
	// to be huge rendering caching it in Redis to affect Redis performance due to the drastic size of data per item.
	IsValueDataGloballyCached bool `json:"is_value_data_globally_cached"`
	// If true, node load will be balanced by pushing items to sibling nodes if there are vacant slots,
	// otherwise will not. This feature can be turned off if backend is impacted by the "balancing" act.
	LeafLoadBalancing bool `json:"leaf_load_balancing"`
	// Redis cache specification for this store's objects(registry, nodes, item value part).
	// Defaults to the global specification and can be overriden for each store.
	CacheConfig StoreCacheConfig `json:"cache_config"`

	// MapKey index specification or CEL expression, if/when we support CEL expression based comparer.
	MapKeyIndexSpecification string `json:"mapkey_index_spec"`

	// Hint that tells the Python binding which JSON B-tree type to instantiate on Open method.
	IsPrimitiveKey bool `json:"is_primitive_key"`
}

// Store Cache config specificaiton.
type StoreCacheConfig struct {
	// Specifies this store's Registry Objects' Redis cache duration.
	RegistryCacheDuration time.Duration `json:"registry_cache_duration"`
	// Is RegistryCache sliding time(TTL) or not. If true, needs Redis 6.2.0+.
	IsRegistryCacheTTL bool `json:"is_registry_cache_ttl"`
	// Specifies this store's Node's Redis cache duration.
	NodeCacheDuration time.Duration `json:"node_cache_duration"`
	// Is NodeCache sliding time(TTL) or not. If true, needs Redis 6.2.0+.
	IsNodeCacheTTL bool `json:"is_node_cache_ttl"`
	// Only used if IsValueDataInNodeSegment(false) & IsValueDataGloballyCached(true).
	// Specifies this store's Item Value part Redis cache duration.
	ValueDataCacheDuration time.Duration `json:"value_data_cache_duration"`
	// Is ValueCache sliding time(TTL) or not. If true, needs Redis 6.2.0+.
	IsValueDataCacheTTL bool `json:"is_value_data_cache_ttl"`
	// Specifies this store's details(StoreInfo) Redis cache duration.
	StoreInfoCacheDuration time.Duration `json:"store_info_cache_duration"`
	// Is StoreInfoCache sliding time(TTL) or not. If true, needs Redis 6.2.0+.
	IsStoreInfoCacheTTL bool `json:"is_store_info_cache_ttl"`
}

const minCacheDuration = time.Duration(5 * time.Minute)

// Create a new StoraceCacheConfig with common cache duration(& TTL setting) among its data parts.
func NewStoreCacheConfig(cacheDuration time.Duration, isCacheTTL bool) *StoreCacheConfig {
	if cacheDuration > 0 && cacheDuration < minCacheDuration {
		cacheDuration = minCacheDuration
	}
	if cacheDuration == 0 && isCacheTTL {
		isCacheTTL = false
	}
	return &StoreCacheConfig{
		RegistryCacheDuration:  cacheDuration,
		IsRegistryCacheTTL:     isCacheTTL,
		NodeCacheDuration:      cacheDuration,
		IsNodeCacheTTL:         isCacheTTL,
		StoreInfoCacheDuration: cacheDuration,
		IsStoreInfoCacheTTL:    isCacheTTL,
		ValueDataCacheDuration: cacheDuration,
		IsValueDataCacheTTL:    isCacheTTL,
	}
}

// Enforce SOP minimum rule on caching period. SOP relies on caching for many things including the critically needed "orchestration".
func (scc *StoreCacheConfig) enforceMinimumRule() {
	if scc.NodeCacheDuration > 0 && scc.NodeCacheDuration < minCacheDuration {
		scc.NodeCacheDuration = minCacheDuration
	}
	if scc.NodeCacheDuration == 0 && scc.IsNodeCacheTTL {
		scc.IsNodeCacheTTL = false
	}
	// scc.NodeCacheDuration defaults to -1, meaning no caching.
	if scc.NodeCacheDuration == 0 {
		scc.NodeCacheDuration = -1
	}

	if scc.RegistryCacheDuration > 0 && scc.RegistryCacheDuration < minCacheDuration {
		scc.RegistryCacheDuration = time.Duration(10 * time.Minute)
	}
	if scc.RegistryCacheDuration == 0 && scc.IsRegistryCacheTTL {
		scc.IsRegistryCacheTTL = false
	}
	if scc.RegistryCacheDuration == 0 {
		// Registry cache duration needs to be decent, 15 mins or 10 mins. It has to minimally last
		// entire transaction commit period as Registry entries are used for implementing core engine
		// functionalities like conflict detection & btree contents' auto-merge.
		scc.RegistryCacheDuration = time.Duration(10 * time.Minute)
	}

	if scc.StoreInfoCacheDuration > 0 && scc.StoreInfoCacheDuration < minCacheDuration {
		scc.StoreInfoCacheDuration = minCacheDuration
	}
	if scc.StoreInfoCacheDuration == 0 && scc.IsStoreInfoCacheTTL {
		scc.IsStoreInfoCacheTTL = false
	}
	if scc.StoreInfoCacheDuration == 0 {
		scc.StoreInfoCacheDuration = minCacheDuration
	}

	// Value Data can be set to minimum.
	if scc.ValueDataCacheDuration == 0 && scc.IsValueDataCacheTTL {
		scc.IsValueDataCacheTTL = false
	}
	if scc.ValueDataCacheDuration == 0 {
		scc.ValueDataCacheDuration = minCacheDuration
	}
}

// NewStoreInfoExt instantiates a new Store and offers more parameters configurable to your desire.
// blobStoreBasePath can be left blank("") and SOP will generate a name for it. This parameter is geared so one can specify
// a base path folder for the blob store using the File System. If using Cassandra table, please specify blank("").
func NewStoreInfo(si StoreOptions) *StoreInfo {
	// Only even numbered slot lengths are allowed as we reduced scenarios to simplify logic.
	if si.SlotLength%2 != 0 {
		si.SlotLength--
	}
	// Minimum slot length is 2.
	if si.SlotLength < 2 {
		si.SlotLength = 2
	}

	registryTableName := si.Name
	blobTableName := si.Name

	if !si.DisableRegistryStoreFormatting {
		// auto generate table names based off of store name.
		registryTableName = FormatRegistryTable(si.Name)
	}
	if !si.DisableBlobStoreFormatting {
		blobTableName = fmt.Sprintf("%s_b", si.Name)
		if si.BlobStoreBaseFolderPath != "" {
			// Append the store name as suffix so blob folders will be separated from one another, if not yet.
			if !strings.HasSuffix(si.BlobStoreBaseFolderPath, si.Name) {
				si.BlobStoreBaseFolderPath = fmt.Sprintf("%s%c%s", si.BlobStoreBaseFolderPath, os.PathSeparator, si.Name)
			}
			blobTableName = si.BlobStoreBaseFolderPath
		}
	}

	const maxSlotLength = 10000

	// Maximum slot length is 10,000. It may be ridiculously huge blob if too big.
	// Even 10,000 may be too much, depending on key & value data size you'll store.
	if si.SlotLength > maxSlotLength {
		si.SlotLength = maxSlotLength
	}

	// Enforce some basic rule not to create conflicting setup.
	if si.IsValueDataInNodeSegment {
		si.IsValueDataGloballyCached = false
		si.IsValueDataActivelyPersisted = false
	}

	// Use the SOP default cache config if the parameter received is not set.
	if si.CacheConfig == nil {
		cc := GetDefaulCacheConfig()
		si.CacheConfig = &cc
	}
	// Apply SOP minimum caching rule if needed.
	si.CacheConfig.enforceMinimumRule()

	// Turn off global caching flag if user specifies "no caching" (-1) on value data cache duration.
	if si.CacheConfig.ValueDataCacheDuration < 0 {
		si.IsValueDataGloballyCached = false
	}

	return &StoreInfo{
		Name:                         si.Name,
		SlotLength:                   si.SlotLength,
		IsUnique:                     si.IsUnique,
		Description:                  si.Description,
		RegistryTable:                registryTableName,
		BlobTable:                    blobTableName,
		IsValueDataInNodeSegment:     si.IsValueDataInNodeSegment,
		IsValueDataActivelyPersisted: si.IsValueDataActivelyPersisted,
		IsValueDataGloballyCached:    si.IsValueDataGloballyCached,
		LeafLoadBalancing:            si.LeafLoadBalancing,
		CacheConfig:                  *si.CacheConfig,
		MapKeyIndexSpecification:     si.CELexpression,
		IsPrimitiveKey:               si.IsPrimitiveKey,
	}
}

// Returns true if this StoreInfo is empty, false otherwise.
// Empty StoreInfo signifies B-Tree does not exist yet.
func (s StoreInfo) IsEmpty() bool {
	var zero StoreInfo
	return s == zero
}

// Returns true if another store is compatible with this one spec wise.
func (s StoreInfo) IsCompatible(b StoreInfo) bool {
	return s.SlotLength == b.SlotLength &&
		s.IsUnique == b.IsUnique &&
		s.BlobTable == b.BlobTable &&
		s.RegistryTable == b.RegistryTable &&
		s.IsValueDataInNodeSegment == b.IsValueDataInNodeSegment &&
		s.IsValueDataActivelyPersisted == b.IsValueDataActivelyPersisted &&
		s.IsValueDataGloballyCached == b.IsValueDataGloballyCached &&
		s.LeafLoadBalancing == b.LeafLoadBalancing &&
		s.IsPrimitiveKey == b.IsPrimitiveKey &&
		s.MapKeyIndexSpecification == b.MapKeyIndexSpecification
}

// Format a given name into a registry table name by adding suffix.
func FormatRegistryTable(name string) string {
	return fmt.Sprintf("%s_r", name)
}
