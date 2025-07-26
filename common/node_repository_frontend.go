package common

import (
	"context"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
)

// Frontend facing Node Repository. Implements the NodeRepository interface CRUD methods.

type nodeRepositoryFrontEnd[TK btree.Ordered, TV any] struct {
	*nodeRepositoryBackend
}

// Add will upsert node to the map.
func (nr *nodeRepositoryFrontEnd[TK, TV]) Add(n *btree.Node[TK, TV]) {
	nr.add(n.ID, n)
}

// Update will upsert node to the map.
func (nr *nodeRepositoryFrontEnd[TK, TV]) Update(n *btree.Node[TK, TV]) {
	nr.update(n.ID, n)
}

// Get will retrieve a node with nodeID from the map.
func (nr *nodeRepositoryFrontEnd[TK, TV]) Get(ctx context.Context, nodeID sop.UUID) (*btree.Node[TK, TV], error) {
	var target btree.Node[TK, TV]
	n, err := nr.get(ctx, nodeID, &target)
	if n == nil {
		return nil, err
	}
	return n.(*btree.Node[TK, TV]), err
}

func (nr *nodeRepositoryFrontEnd[TK, TV]) Fetched(nodeID sop.UUID) {
	n := nr.readNodesCache.Get([]sop.UUID{nodeID})
	if n[0] != nil {
		nr.localCache[nodeID] = cachedNode{
			action: getAction,
			node:   n[0],
		}
		// Remove now from MRU since node got migrated to local cache and is now "tracked".
		nr.readNodesCache.Delete([]sop.UUID{nodeID})
	}
}

// Remove will remove a node with nodeID from the map.
func (nr *nodeRepositoryFrontEnd[TK, TV]) Remove(nodeID sop.UUID) {
	nr.remove(nodeID)
}
