package l1_cache

import (
	"context"

	"github.com/SharedCode/sop"
)

type entry struct {
	handle  *sop.Handle
	node any
	dllNode *node[sop.UUID]
}

type l1Cache struct {
	handles map[sop.UUID]*entry
	mru     *mru
	l2Cache sop.Cache
}

func NewL1Cache(l2c sop.Cache, minCapacity, maxCapacity int) *l1Cache {
	l1c := &l1Cache{
		handles: make(map[sop.UUID]*entry, maxCapacity),
		l2Cache: l2c,
	}
	l1c.mru = newMru(l1c, minCapacity, maxCapacity)
	return l1c
}

func (c *l1Cache) SetHandles(ctx context.Context, handles []sop.RegistryPayload[sop.Handle]) error {	//}, nodes []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) {
	// for _, h := range hs {
	// 	for i := range h.IDs {
	// 		c.handles[h.IDs[i].LogicalID] = &entry{
	// 			handle: &h.IDs[i],
	// 		}
	// 	}
	// }
	return nil
}
func (c *l1Cache) GetHandles(ctx context.Context, ids []sop.RegistryPayload[sop.UUID]) ([]sop.RegistryPayload[sop.Handle], error) {
	return nil, nil
}

func (c *l1Cache) GetNode(ctx context.Context, nodeID sop.UUID) (any, error) {
	return nil, nil
}
func (c *l1Cache) SetNode(ctx context.Context, nodeID sop.UUID, node any) error {
	return nil
}

func (c *l1Cache) Delete(ctx context.Context, ids []sop.RegistryPayload[sop.UUID]) (bool, error) {
	return false, nil
}

func (c *l1Cache) IsFull() bool {
	return c.mru.isFull()
}
func (c *l1Cache) Prune() {
	c.mru.prune()
}
