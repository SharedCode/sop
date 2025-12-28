package sop

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// StoreInfo describes a B-Tree store configuration and runtime state persisted in the backend.
type StoreInfo struct {
	// Name is the short store name.
	Name string `json:"name" minLength:"1" maxLength:"128"`
	// SlotLength is the number of items per node.
	SlotLength int `json:"slot_length" min:"2" max:"10000"`
	// IsUnique enforces uniqueness on the key of key/value items.
	IsUnique bool `json:"is_unique"`
	// Description optionally describes the store.
	Description string `json:"description" maxLength:"500"`
	// RegistryTable is the registry table name.
	RegistryTable string `json:"registry_table" minLength:"1" maxLength:"140"`
	// BlobTable is the blob table name or base filesystem path.
	BlobTable string `json:"blob_table" minLength:"1" maxLength:"300"`
	// RootNodeID is the root B-Tree node identifier.
	RootNodeID UUID `json:"root_node_id"`
	// Count is the total number of items persisted.
	Count int64 `json:"count"`
	// CountDelta is used internally to reconcile Count updates; it should not be persisted.
	CountDelta int64 `json:"-"`
	// Timestamp is the add/update time in milliseconds.
	Timestamp int64 `json:"timestamp"`
	// IsValueDataInNodeSegment stores the Value within the node segment when true.
	IsValueDataInNodeSegment bool `json:"is_value_data_in_node_segment"`
	// IsValueDataActivelyPersisted persists Value separately on Add/Update when true.
	IsValueDataActivelyPersisted bool `json:"is_value_data_actively_persisted"`
	// IsValueDataGloballyCached enables Redis caching of Value data when true.
	IsValueDataGloballyCached bool `json:"is_value_data_globally_cached"`
	// LeafLoadBalancing enables distribution to sibling nodes when capacity allows.
	LeafLoadBalancing bool `json:"leaf_load_balancing"`
	// CacheConfig overrides global cache settings per store.
	CacheConfig StoreCacheConfig `json:"cache_config"`

	// MapKeyIndexSpecification contains a CEL or index specification used by the comparer.
	MapKeyIndexSpecification string `json:"mapkey_index_spec"`

	// LegacyCELexpression is used for backward compatibility to retrieve index spec from old stores.
	LegacyCELexpression string `json:"cel_expression,omitempty"`

	// IsPrimitiveKey hints the Python binding which JSON B-Tree to instantiate on open.
	IsPrimitiveKey bool `json:"is_primitive_key"`
}

// StoreCacheConfig declares cache durations and TTL flags for store artifacts.
type StoreCacheConfig struct {
	// RegistryCacheDuration controls caching for registry objects.
	RegistryCacheDuration time.Duration `json:"registry_cache_duration"`
	// IsRegistryCacheTTL enables sliding TTL for registry cache.
	IsRegistryCacheTTL bool `json:"is_registry_cache_ttl"`
	// NodeCacheDuration controls caching for nodes.
	NodeCacheDuration time.Duration `json:"node_cache_duration"`
	// IsNodeCacheTTL enables sliding TTL for node cache.
	IsNodeCacheTTL bool `json:"is_node_cache_ttl"`
	// ValueDataCacheDuration controls caching for the item Value part when globally cached.
	ValueDataCacheDuration time.Duration `json:"value_data_cache_duration"`
	// IsValueDataCacheTTL enables sliding TTL for value data cache.
	IsValueDataCacheTTL bool `json:"is_value_data_cache_ttl"`
	// StoreInfoCacheDuration controls caching for StoreInfo records.
	StoreInfoCacheDuration time.Duration `json:"store_info_cache_duration"`
	// IsStoreInfoCacheTTL enables sliding TTL for store info cache.
	IsStoreInfoCacheTTL bool `json:"is_store_info_cache_ttl"`
}

const minCacheDuration = time.Duration(5 * time.Minute)

// NewStoreCacheConfig returns a StoreCacheConfig with uniform cache durations and TTL settings applied.
// If cacheDuration is between 1ns and 5 minutes, it will be clamped to 5 minutes. TTL is disabled when duration is zero.
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

// enforceMinimumRule applies SOP minimum caching policy to ensure orchestrations remain effective.
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

// NewStoreInfo creates and normalizes a StoreInfo based on StoreOptions, applying default naming and cache policy.
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

	spec := si.MapKeyIndexSpecification
	if spec == "" {
		spec = si.CELexpression
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
		MapKeyIndexSpecification:     spec,
		IsPrimitiveKey:               si.IsPrimitiveKey,
	}
}

// IsEmpty reports whether the StoreInfo has zero values; an empty StoreInfo means the B-Tree does not yet exist.
func (s StoreInfo) IsEmpty() bool {
	var zero StoreInfo
	return s == zero
}

// IsCompatible reports whether two StoreInfo configurations are compatible for merge/attach semantics.
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

// FormatRegistryTable formats a store name into a registry table name by adding an _r suffix.
func FormatRegistryTable(name string) string {
	return fmt.Sprintf("%s_r", name)
}
