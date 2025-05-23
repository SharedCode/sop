package common

import (
	"context"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
)

// Frontend facing Node Repository. Implements the NodeRepository interface CRUD methods.

type nodeRepositoryFrontEnd[TK btree.Ordered, TV any] struct {
	backendNodeRepository *nodeRepositoryBackend
}

// Add will upsert node to the map.
func (nr *nodeRepositoryFrontEnd[TK, TV]) Add(n *btree.Node[TK, TV]) {
	nr.backendNodeRepository.add(n.ID, n)
}

// Update will upsert node to the map.
func (nr *nodeRepositoryFrontEnd[TK, TV]) Update(n *btree.Node[TK, TV]) {
	nr.backendNodeRepository.update(n.ID, n)
}

// Get will retrieve a node with nodeID from the map.
func (nr *nodeRepositoryFrontEnd[TK, TV]) Get(ctx context.Context, nodeID sop.UUID) (*btree.Node[TK, TV], error) {
	var target btree.Node[TK, TV]
	n, err := nr.backendNodeRepository.get(ctx, nodeID, &target)
	if n == nil {
		return nil, err
	}
	return n.(*btree.Node[TK, TV]), err
}

func (nr *nodeRepositoryFrontEnd[TK, TV]) Fetched(nodeID sop.UUID) {
	c := nr.backendNodeRepository.localCache[nodeID]
	if c.action == defaultAction {
		c.action = getAction
		nr.backendNodeRepository.localCache[nodeID] = c
	}
}

// Remove will remove a node with nodeID from the map.
func (nr *nodeRepositoryFrontEnd[TK, TV]) Remove(nodeID sop.UUID) {
	nr.backendNodeRepository.remove(nodeID)
}
