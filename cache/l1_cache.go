// Package contains the L1 (MRU) Cache implementtion.
package cache

import (
	"context"
	"fmt"
	log "log/slog"
	"sync"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/encoding"
	"github.com/SharedCode/sop/redis"
)

type entry struct {
	node    any
	dllNode *node[sop.UUID]
}

type L1Cache struct {
	lookup       map[sop.UUID]*entry
	mru          *mru
	l2CacheNodes sop.Cache
	locker       *sync.Mutex
}

const (
	DefaultMinCapacity = 1000
	DefaultMaxCapacity = 1350
)

// Global cache
var Global *L1Cache

// Instantiate the global cache.
func CreateGlobalCache(l2CacheNodes sop.Cache, minCapacity, maxCapacity int) *L1Cache {
	if Global == nil || Global.mru.minCapacity != minCapacity || Global.mru.maxCapacity != maxCapacity {
		Global = NewL1Cache(l2CacheNodes, minCapacity, maxCapacity)
	}
	return Global
}

// Returns the singleton global cache.
func GetGlobalCache() *L1Cache {
	if Global == nil {
		c := redis.NewClient()
		CreateGlobalCache(c, DefaultMinCapacity, DefaultMaxCapacity)
	}
	return Global
}

// Instantiate a new instance of this L1 Cache management logic.
func NewL1Cache(l2cn sop.Cache, minCapacity, maxCapacity int) *L1Cache {
	l1c := &L1Cache{
		lookup:       make(map[sop.UUID]*entry, maxCapacity),
		l2CacheNodes: l2cn,
		locker:       &sync.Mutex{},
	}
	l1c.mru = newMru(l1c, minCapacity, maxCapacity)
	return l1c
}

// The L1 Cache getters (get handles & get node) are able to check if there is newer version in L2 cache
// and fetch it if there is.

func (c *L1Cache) SetNode(ctx context.Context, nodeID sop.UUID, node any, nodeCacheDuration time.Duration) {
	c.SetNodeMRU(ctx, nodeID, node, nodeCacheDuration)
	if err := c.l2CacheNodes.SetStruct(ctx, FormatNodeKey(nodeID.String()), node, nodeCacheDuration); err != nil {
		log.Warn(fmt.Sprintf("failed to cache in Redis node with ID: %v, details: %v", nodeID.String(), err))
	}
}
func (c *L1Cache) SetNodeMRU(ctx context.Context, nodeID sop.UUID, node any, nodeCacheDuration time.Duration) {
	// Update the lookup entry's node value w/ incoming.
	ba, _ := encoding.BlobMarshaler.Marshal(node)
	c.locker.Lock()
	if v, ok := c.lookup[nodeID]; ok {
		v.node = ba //node
		c.mru.remove(v.dllNode)
		v.dllNode = c.mru.add(nodeID)
		c.locker.Unlock()
		return
	} else {
		// Add to MRU cache.
		n := c.mru.add(nodeID)
		c.lookup[nodeID] = &entry{
			node:    ba, //node,
			dllNode: n,
		}
	}
	c.locker.Unlock()

	// Evict LRU items if MRU is full.
	c.Evict()
}

func (c *L1Cache) GetNode(ctx context.Context, handle sop.Handle, nodeTarget any, isNodeCacheTTL bool, nodeCacheTTLDuration time.Duration) (any, error) {
	nodeID := handle.GetActiveID()

	fetchFromL2 := false

	// Get node from MRU if same version as requested.
	c.locker.Lock()
	if v, ok := c.lookup[nodeID]; ok {
		c.mru.remove(v.dllNode)
		v.dllNode = c.mru.add(nodeID)

		encoding.BlobMarshaler.Unmarshal(v.node.([]byte), nodeTarget)
		fetchFromL2 = nodeTarget.(btree.MetaDataType).GetVersion() != handle.Version
		//nodeTarget = v.node

		if !fetchFromL2 {
			c.locker.Unlock()
			return nodeTarget, nil
		}
	}
	c.locker.Unlock()

	// Get node from L2 cache.
	if isNodeCacheTTL {
		if err := c.l2CacheNodes.GetStructEx(ctx, FormatNodeKey(nodeID.String()), nodeTarget, nodeCacheTTLDuration); err != nil {
			if c.l2CacheNodes.KeyNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
	} else {
		if err := c.l2CacheNodes.GetStruct(ctx, FormatNodeKey(nodeID.String()), nodeTarget); err != nil {
			if c.l2CacheNodes.KeyNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
	}
	// Found in L2, put in MRU.
	c.SetNodeMRU(ctx, nodeID, nodeTarget, nodeCacheTTLDuration)

	return nodeTarget, nil
}

func (c *L1Cache) DeleteNode(ctx context.Context, nodeID sop.UUID) (bool, error) {
	var result bool
	var lastErr error

	// Delete from MRU if it is there.
	c.locker.Lock()
	if v, ok := c.lookup[nodeID]; ok {
		c.mru.remove(v.dllNode)
		v.node = nil
		v.dllNode = nil
		delete(c.lookup, nodeID)
		result = true
	}
	c.locker.Unlock()

	// Delete from L2 cache if it is there.
	if err := c.l2CacheNodes.Delete(ctx, []string{FormatNodeKey(nodeID.String())}); err != nil {
		if !c.l2CacheNodes.KeyNotFound(err) {
			log.Debug(err.Error())
			lastErr = err
		}
	} else {
		result = true
	}
	return result, lastErr
}

// Returns the count of items store in this L1 Cache.
func (c *L1Cache) Count() int {
	return len(c.lookup)
}

func (c *L1Cache) IsFull() bool {
	return c.mru.isFull()
}
func (c *L1Cache) Evict() {
	c.mru.evict()
}

// FormatNodeKey just appends a prefix('N') to the key.
func FormatNodeKey(k string) string {
	return fmt.Sprintf("N%s", k)
}
