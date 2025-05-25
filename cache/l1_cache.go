// Package contains the L1 (MRU) Cache implementtion. The cache management logic handles
// both L1(local MRU) & L2(Redis) caches. It flows and fetches data between the two layers.
// L1 limits the caching to MRU max capacity which defaults to 1,350.
//
// It is written to be thread safe so it can get used as a global cache that can provide data
// to different transaction B-tree instances.
//
// You can instantiate the global cache using the "CreateGlobalCache" package function, or simply
// call GetGlobalCache() to return a default global cache using DefaultMinCapacity, DefaultMaxCapacity
// and Redis as L2 Cache, using the package "redis" global connection.
package cache

import (
	"context"
	"fmt"
	log "log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/redis"
)

type entry struct {
	handle  sop.Handle
	node    any
	dllNode *node[sop.UUID]
}

type L1Cache struct {
	handles      map[sop.UUID]*entry
	mru          *mru
	l2Cache      sop.Cache
	l2CacheNodes sop.Cache
	fetchIfNewer atomic.Bool
	locker       *sync.Mutex
}

const (
	DefaultMinCapacity = 1000
	DefaultMaxCapacity = 1350
)

// Global cache
var Global *L1Cache

// For Unit Test injection needs only.
var getFromMRUOnly bool

// Instantiate the global cache.
func CreateGlobalCache(l2Cache sop.Cache, l2CacheNodes sop.Cache, minCapacity, maxCapacity int) *L1Cache {
	if Global == nil || Global.mru.minCapacity != minCapacity || Global.mru.maxCapacity != maxCapacity {
		Global = NewL1Cache(l2Cache, l2CacheNodes, minCapacity, maxCapacity)
	}
	return Global
}

// Returns the singleton global cache.
func GetGlobalCache() *L1Cache {
	if Global == nil {
		c := redis.NewClient()
		CreateGlobalCache(c, c, DefaultMinCapacity, DefaultMaxCapacity)
	}
	return Global
}

// Instantiate a new instance of this L1 Cache management logic.
func NewL1Cache(l2c sop.Cache, l2cn sop.Cache, minCapacity, maxCapacity int) *L1Cache {
	l1c := &L1Cache{
		handles:      make(map[sop.UUID]*entry, maxCapacity),
		l2Cache:      l2c,
		l2CacheNodes: l2cn,
		locker:       &sync.Mutex{},
	}
	l1c.mru = newMru(l1c, minCapacity, maxCapacity)
	return l1c
}

// Set flag to detect if L2 cache (or registry) has newer data & refresh local copy if there is.
// The fetcher compares the Handle's version # if L2 cache (or registry) has newer Node.
func (c *L1Cache) SetFetchIfNewer(flag bool) {
	c.fetchIfNewer.Store(flag)
}

// The L1 Cache getters (get handles & get node) are able to check if there is newer version in L2 cache
// and fetch it if there is.

func (c *L1Cache) GetHandles(ctx context.Context, ids []sop.RegistryPayload[sop.UUID]) ([]sop.RegistryPayload[sop.Handle], error) {

	results := make([]sop.RegistryPayload[sop.Handle], len(ids))
	var lastErr error
	for i, lid := range ids {
		results[i].RegistryTable = lid.RegistryTable
		results[i].CacheDuration = lid.CacheDuration
		results[i].IsCacheTTL = lid.IsCacheTTL
		results[i].IDs = make([]sop.Handle, 0, len(lid.IDs))
		for _, id := range lid.IDs {

			if !c.fetchIfNewer.Load() {
				// Get from MRU cache.
				c.locker.Lock()
				if v, ok := c.handles[id]; ok {
					results[i].IDs = append(results[i].IDs, v.handle)
					c.mru.remove(v.dllNode)
					c.mru.add(v.handle.LogicalID)
					c.locker.Unlock()
					continue
				}
				c.locker.Unlock()
			}

			// For unit testing only.
			if getFromMRUOnly {
				continue
			}

			// Get from L2 cache.
			var h sop.Handle
			if lid.IsCacheTTL {
				if err := c.l2Cache.GetStructEx(ctx, id.String(), &h, lid.CacheDuration); err != nil {
					if c.l2Cache.KeyNotFound(err) {
						continue
					}
					log.Warn(fmt.Sprintf("l1Cache.GetHandles (redis GetStructEx) failed, details: %v", err))
					lastErr = err
					continue
				}
			} else {
				if err := c.l2Cache.GetStruct(ctx, id.String(), &h); err != nil {
					log.Warn(fmt.Sprintf("l1Cache.GetHandles (redis GetStruct) failed, details: %v", err))
					lastErr = err
					continue
				}
			}

			if h.IsEmpty() {
				continue
			}

			// Put in MRU top if found in L2.
			c.locker.Lock()
			if v, ok := c.handles[h.LogicalID]; ok {
				c.mru.remove(v.dllNode)
				c.mru.add(v.handle.LogicalID)
			} else {
				n := c.mru.add(h.LogicalID)
				c.handles[h.LogicalID] = &entry{
					handle:  h,
					dllNode: n,
				}
			}
			c.locker.Unlock()

			results[i].IDs = append(results[i].IDs, h)
		}
	}

	return results, lastErr
}

func (c *L1Cache) GetNode(ctx context.Context, id sop.RegistryPayload[sop.UUID], isNodeCacheTTL bool, nodeCacheTTLDuration time.Duration, nodeTarget any) (sop.Handle, any, error) {

	if !c.fetchIfNewer.Load() {
		// Get from MRU cache.
		c.locker.Lock()
		if v, ok := c.handles[id.IDs[0]]; ok {
			// Put to the MRU top the found entry.
			c.mru.remove(v.dllNode)
			c.mru.add(v.handle.LogicalID)
			c.locker.Unlock()
			return v.handle, v.node, nil
		}
		c.locker.Unlock()
	}

	// Get handle from L2 cache.
	var h sop.Handle
	if id.IsCacheTTL {
		if err := c.l2Cache.GetStructEx(ctx, id.IDs[0].String(), &h, id.CacheDuration); err != nil {
			if c.l2Cache.KeyNotFound(err) {
				return h, nodeTarget, nil
			}
			return h, nodeTarget, err
		}
	} else {
		if err := c.l2Cache.GetStruct(ctx, id.IDs[0].String(), &h); err != nil {
			if c.l2Cache.KeyNotFound(err) {
				return h, nodeTarget, nil
			}
			return h, nodeTarget, err
		}
	}

	// Just return nil if not found from L2.
	if h.IsEmpty() {
		return h, nodeTarget, nil
	}

	// Get Handle from MRU cache so we can compare w/ from L2.
	c.locker.Lock()
	if v, ok := c.handles[id.IDs[0]]; ok {
		c.mru.remove(v.dllNode)
		// Just return the L1 copy if node in backend has not changed.
		if h.Version == v.handle.Version {
			// Put to the MRU top the found entry.
			c.mru.add(v.handle.LogicalID)
			c.locker.Unlock()
			return v.handle, v.node, nil
		}
		// Fully remove the entry from MRU since there is a newer version in L2.
		delete(c.handles, id.IDs[0])
	}
	c.locker.Unlock()

	// Get node from L2 cache.
	if isNodeCacheTTL {
		if err := c.l2Cache.GetStructEx(ctx, FormatNodeKey(h.GetActiveID().String()), nodeTarget, nodeCacheTTLDuration); err != nil {
			if c.l2Cache.KeyNotFound(err) {
				return h, nodeTarget, nil
			}
			return h, nodeTarget, err
		}
	} else {
		if err := c.l2Cache.GetStruct(ctx, FormatNodeKey(h.GetActiveID().String()), nodeTarget); err != nil {
			if c.l2Cache.KeyNotFound(err) {
				return h, nodeTarget, nil
			}
			return h, nodeTarget, err
		}
	}

	// Add to MRU cache the l2 cache discovered node & its handle.
	c.locker.Lock()
	n := c.mru.add(h.LogicalID)
	c.handles[h.LogicalID] = &entry{
		handle:  h,
		dllNode: n,
		node:    nodeTarget,
	}
	c.locker.Unlock()
	// Auto prune if over the max capacity.
	if c.IsFull() {
		c.Prune()
	}

	return h, nodeTarget, nil
}

func (c *L1Cache) GetHandleByID(ctx context.Context, id sop.UUID) (sop.Handle, error) {
	if h, err := c.GetHandles(ctx, []sop.RegistryPayload[sop.UUID]{
		{
			IDs: []sop.UUID{id},
		},
	}); err != nil {
		return sop.Handle{}, err
	} else {
		if len(h[0].IDs) == 0 {
			return sop.Handle{}, nil
		}
		return h[0].IDs[0], nil
	}
}

// Shortcut to GetNode useful for unit testing, case where code does NOT care whether to respect TTL or not
// of the Node needed to be fetched. Just fetch it given this node (logical) ID.
func (c *L1Cache) GetNodeByID(ctx context.Context, lid sop.UUID, nodeTarget any) (sop.Handle, any, error) {
	return c.GetNode(ctx, sop.RegistryPayload[sop.UUID]{
		IDs: []sop.UUID{lid},
	}, false, 5*time.Second, nodeTarget)
}

func (c *L1Cache) SetHandles(ctx context.Context, handles []sop.RegistryPayload[sop.Handle]) error {
	if c.IsFull() {
		// Self pruning if capacity is at full (max).
		c.Prune()
	}

	var lastErr error
	for _, sh := range handles {
		for _, h := range sh.IDs {
			// Set to L2 cache.
			if err := c.l2Cache.SetStruct(ctx, h.LogicalID.String(), &h, sh.CacheDuration); err != nil {
				// Tolerate Redis cache failure.
				log.Warn(fmt.Sprintf("l1Cache.SetHandles (redis setstruct) failed, details: %v", err))
				lastErr = err
			}

			// Add to MRU cache.
			c.locker.Lock()
			n := c.mru.add(h.LogicalID)
			c.handles[h.LogicalID] = &entry{
				handle:  h,
				dllNode: n,
			}
			c.locker.Unlock()
		}
	}

	return lastErr
}

func (c *L1Cache) SetNode(ctx context.Context, nodeLogicalID sop.UUID, nodePhysicalID sop.UUID, node any, nodeCacheDuration time.Duration) error {
	// Update the lookup entry's node value w/ incoming.
	c.locker.Lock()
	if v, ok := c.handles[nodeLogicalID]; ok {
		v.node = node
		c.mru.remove(v.dllNode)
		c.mru.add(v.handle.LogicalID)
	} else {
		log.Debug(fmt.Sprintf("l1Cache.SetNode didn't find from handles lookup the entry w/ logical ID %v", nodeLogicalID))
	}
	c.locker.Unlock()

	// Put to L2 cache the Node data.
	if err := c.l2CacheNodes.SetStruct(ctx, FormatNodeKey(nodePhysicalID.String()), node, nodeCacheDuration); err != nil {
		log.Debug(fmt.Sprintf("l1Cache.SetNode failed redisCache.SetStruct, details: %v", err))
		return err
	}
	return nil
}

func (c *L1Cache) DeleteHandles(ctx context.Context, ids []sop.RegistryPayload[sop.UUID]) error {
	var lastErr error
	for _, sh := range ids {
		for _, id := range sh.IDs {
			if _, err := c.DeleteNode(ctx, id, sop.NilUUID); err != nil {
				lastErr = err
			}
		}
	}
	return lastErr
}

func (c *L1Cache) DeleteNode(ctx context.Context, nodeLogicalID sop.UUID, nodePhysicalID sop.UUID) (bool, error) {
	var result bool
	var lastErr error
	if nodeLogicalID != sop.NilUUID {
		// Delete the lookup entry's node value w/ incoming.
		c.locker.Lock()
		if v, ok := c.handles[nodeLogicalID]; ok {
			c.mru.remove(v.dllNode)
			v.node = nil
			v.dllNode = nil
			delete(c.handles, nodeLogicalID)
			result = true
		} else {
			log.Debug(fmt.Sprintf("l1Cache.DeleteNode didn't find from handles lookup the entry w/ logical ID %v", nodeLogicalID))
		}
		c.locker.Unlock()
		// Delete from L2 cache the Handle data.
		if err := c.l2Cache.Delete(ctx, []string{nodeLogicalID.String()}); err != nil {
			if !c.l2Cache.KeyNotFound(err) {
				log.Debug(fmt.Sprintf("l1Cache.DeleteNode (redis delete) failed, details: %v", err))
				lastErr = err
			}
		} else {
			result = true
		}
	}

	// Delete from L2 cache the Node data.
	if nodePhysicalID != sop.NilUUID {
		if err := c.l2CacheNodes.Delete(ctx, []string{FormatNodeKey(nodePhysicalID.String())}); err != nil {
			if !c.l2CacheNodes.KeyNotFound(err) {
				log.Debug(err.Error())
				lastErr = err
			}
		} else {
			result = true
		}
	}
	return result, lastErr
}

// Returns the count of items store in this L1 Cache.
func (c *L1Cache) Count() int {
	return len(c.handles)
}

func (c *L1Cache) IsFull() bool {
	return c.mru.isFull()
}
func (c *L1Cache) Prune() {
	c.mru.prune()
}

// FormatNodeKey just appends a prefix('N') to the key.
func FormatNodeKey(k string) string {
	return fmt.Sprintf("N%s", k)
}
