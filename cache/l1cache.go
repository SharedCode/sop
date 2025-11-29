// Package cache contains the L1 (MRU) cache implementation and L2 integration for B-tree nodes and handles.
package cache

import (
	"context"
	"fmt"
	log "log/slog"
	"sync"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/encoding"
)

type l1CacheEntry struct {
	nodeVersion int32
	nodeData    []byte
	dllNode     *node[sop.UUID]
}

// L1Cache is an in-memory MRU cache for B-tree nodes and handle objects.
// It optionally integrates with an L2 cache (e.g., Redis) for cross-process sharing and TTL.
type L1Cache struct {
	lookup       map[sop.UUID]*l1CacheEntry
	mru          *l1_mru
	l2CacheNodes sop.L2Cache
	locker       sync.Mutex
	Handles      Cache[sop.UUID, sop.Handle]
}

const (
	// DefaultMinCapacity is the default minimum number of entries to retain before evictions are considered.
	DefaultMinCapacity = 1000
	// DefaultMaxCapacity is the default hard limit of entries allowed in the L1 cache.
	DefaultMaxCapacity = 1350
)

// Global is the singleton L1 cache instance used by GetGlobalCache and NewGlobalCache.
var Global *L1Cache

// NewGlobalCache initializes or replaces the global L1 cache singleton.
// It reuses the existing instance when capacities match; otherwise it creates a new one.
func NewGlobalCache(l2CacheNodes sop.L2Cache, minCapacity, maxCapacity int) *L1Cache {
	if Global == nil || Global.mru.minCapacity != minCapacity || Global.mru.maxCapacity != maxCapacity {
		Global = NewL1Cache(l2CacheNodes, minCapacity, maxCapacity)
	} else {
		Global.l2CacheNodes = l2CacheNodes
	}
	return Global
}

// GetGlobalCache returns the global L1 cache singleton, creating one on first use
// with a Redis L2 cache and default capacities when necessary.
func GetGlobalCache() *L1Cache {
	if Global == nil {
		c := sop.NewCacheClient()
		NewGlobalCache(c, DefaultMinCapacity, DefaultMaxCapacity)
	}
	return Global
}

// NewL1Cache constructs a new L1Cache with the given L2 cache and capacity bounds.
func NewL1Cache(l2cn sop.L2Cache, minCapacity, maxCapacity int) *L1Cache {
	l1c := &L1Cache{
		lookup:       make(map[sop.UUID]*l1CacheEntry, maxCapacity),
		l2CacheNodes: l2cn,
		Handles:      NewSynchronizedCache[sop.UUID, sop.Handle](minCapacity, maxCapacity),
	}
	l1c.mru = newL1Mru(l1c, minCapacity, maxCapacity)
	return l1c
}

// SetNode caches the provided node in the L1 MRU and also in the L2 cache with the given duration.
func (c *L1Cache) SetNode(ctx context.Context, nodeID sop.UUID, node any, nodeCacheDuration time.Duration) {
	c.SetNodeToMRU(ctx, nodeID, node, nodeCacheDuration)
	if c.l2CacheNodes == nil {
		return
	}
	if err := c.l2CacheNodes.SetStruct(ctx, FormatNodeKey(nodeID.String()), node, nodeCacheDuration); err != nil {
		log.Warn(fmt.Sprintf("failed to cache in Redis node with ID: %v, details: %v", nodeID.String(), err))
	}
}

// SetNodeToMRU caches the provided node only in the L1 MRU without touching the L2 cache.
func (c *L1Cache) SetNodeToMRU(ctx context.Context, nodeID sop.UUID, node any, nodeCacheDuration time.Duration) {
	// Update the lookup entry's node value w/ incoming.
	ba, _ := encoding.BlobMarshaler.Marshal(node)
	nv := node.(btree.MetaDataType).GetVersion()
	c.locker.Lock()
	if v, ok := c.lookup[nodeID]; ok {
		v.nodeData = ba
		v.nodeVersion = nv
		c.mru.remove(v.dllNode)
		v.dllNode = c.mru.add(nodeID)
		c.locker.Unlock()
		return
	}
	// Add to MRU cache.
	n := c.mru.add(nodeID)
	c.lookup[nodeID] = &l1CacheEntry{
		nodeData:    ba,
		nodeVersion: nv,
		dllNode:     n,
	}
	c.locker.Unlock()

	// Evict LRU items if MRU is full.
	c.Evict()
}

// GetNodeFromMRU returns the node from L1 if the cached version matches the handle version; otherwise it returns nil.
func (c *L1Cache) GetNodeFromMRU(handle sop.Handle, nodeTarget any) any {
	nodeID := handle.GetActiveID()
	// Get node from MRU if same version as requested.
	c.locker.Lock()
	if v, ok := c.lookup[nodeID]; ok && v.nodeVersion == handle.Version {
		c.mru.remove(v.dllNode)
		v.dllNode = c.mru.add(nodeID)
		encoding.BlobMarshaler.Unmarshal(v.nodeData, nodeTarget)
		c.locker.Unlock()
		return nodeTarget
	}
	c.locker.Unlock()
	return nil
}

// GetNode loads the node by handle either from L1 (if fresh) or from L2, honoring TTL semantics,
// and refreshes the entry in L1 before returning it.
func (c *L1Cache) GetNode(ctx context.Context, handle sop.Handle, nodeTarget any, isNodeCacheTTL bool, nodeCacheTTLDuration time.Duration) (any, error) {
	nodeID := handle.GetActiveID()

	// Get node from MRU if same version as requested.
	c.locker.Lock()
	if v, ok := c.lookup[nodeID]; ok && v.nodeVersion == handle.Version {
		c.mru.remove(v.dllNode)
		v.dllNode = c.mru.add(nodeID)
		encoding.BlobMarshaler.Unmarshal(v.nodeData, nodeTarget)
		c.locker.Unlock()
		return nodeTarget, nil
	}
	c.locker.Unlock()

	if c.l2CacheNodes == nil {
		return nil, nil
	}

	// Get node from L2 cache.
	if isNodeCacheTTL {
		if found, err := c.l2CacheNodes.GetStructEx(ctx, FormatNodeKey(nodeID.String()), nodeTarget, nodeCacheTTLDuration); !found || err != nil {
			return nil, err
		}
	} else {
		if found, err := c.l2CacheNodes.GetStruct(ctx, FormatNodeKey(nodeID.String()), nodeTarget); !found || err != nil {
			return nil, err
		}
	}
	// Found in L2, put in MRU.
	c.SetNodeToMRU(ctx, nodeID, nodeTarget, nodeCacheTTLDuration)

	return nodeTarget, nil
}

// DeleteNodes removes the given node IDs from both the L1 MRU and the L2 cache.
// It returns true if any entries were removed and the last error encountered when deleting from L2.
func (c *L1Cache) DeleteNodes(ctx context.Context, nodesIDs []sop.UUID) (bool, error) {
	var result bool
	var lastErr error

	// Delete from MRU if it is there.
	c.locker.Lock()
	for _, nodeID := range nodesIDs {
		if v, ok := c.lookup[nodeID]; ok {
			c.mru.remove(v.dllNode)
			v.nodeData = nil
			v.dllNode = nil
			delete(c.lookup, nodeID)
			result = true
		}
	}
	c.locker.Unlock()

	if c.l2CacheNodes == nil {
		return result, nil
	}

	// Delete from L2 cache if it is there.
	for _, nodeID := range nodesIDs {
		if found, err := c.l2CacheNodes.Delete(ctx, []string{FormatNodeKey(nodeID.String())}); !found || err != nil {
			if err != nil {
				log.Debug(err.Error())
				lastErr = err

			}
		} else {
			result = true
		}
	}
	return result, lastErr
}

// Count returns the number of entries currently stored in the L1 cache.
func (c *L1Cache) Count() int {
	return len(c.lookup)
}

// IsFull reports whether the L1 cache has reached its maximum capacity.
func (c *L1Cache) IsFull() bool {
	return c.mru.isFull()
}

// Evict removes least-recently-used entries until the cache is within capacity.
func (c *L1Cache) Evict() {
	c.mru.evict()
}

// FormatNodeKey prefixes the key with 'N' to form the cache key for a node.
func FormatNodeKey(k string) string {
	return fmt.Sprintf("N%s", k)
}
