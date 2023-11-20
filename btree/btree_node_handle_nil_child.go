package btree

import (
	"fmt"
)

// removeItemOnNodeWithNilChild will manage these remove item cases.
// - remove item on a node slot with nil left child
// - remove item on a node slot with nil right child
// - remove item on the right edge node slot with nil right child
func (node *Node[TK, TV]) removeItemOnNodeWithNilChild(btree *Btree[TK, TV], index int) (bool, error) {
	if !node.hasChildren() || (node.childrenIds[index] != NilUUID && node.childrenIds[index+1] != NilUUID) {
		return false, nil
	}
	if node.childrenIds[index] == NilUUID {
		if index < node.Count {
			itemsToMove := node.Count - index
			moveArrayElements(node.Slots, index, index+1, itemsToMove)
			moveArrayElements(node.childrenIds, index, index+1, itemsToMove+1)
		}
	} else if node.childrenIds[index+1] == NilUUID {
		if index < node.Count {
			itemsToMove := node.Count - index
			moveArrayElements(node.Slots, index, index+1, itemsToMove)
			moveArrayElements(node.childrenIds, index+1, index+2, itemsToMove+1)
		}
	}
	// Set to nil the last item & its child.
	node.Slots[node.Count-1] = nil
	node.childrenIds[node.Count] = NilUUID
	node.Count--

	if node.Count == 0 && node.childrenIds[0] != NilUUID {
		if node.isRootNode() {
			// Copy contents of the child to this root node.
			nc, err := node.getChild(btree, 0)
			if err != nil {
				return false, err
			}
			if nc == nil {
				return false, fmt.Errorf("Can't get child (Id='%v') of this root node.", node.childrenIds[0])
			}
			copy(node.Slots, nc.Slots)
			node.Count = nc.Count
			if nc.hasChildren() {
				copy(node.childrenIds, nc.childrenIds)
				if err = node.updateChildrenParent(btree); err != nil {
					return false, err
				}
			} else {
				// Nilify the child because we've merged its contents to root node.
				node.childrenIds[0] = NilUUID
				if node.isNilChildren() {
					node.childrenIds = nil
				}
			}
			if err = btree.removeNode(nc); err != nil {
				return false, err
			}
			if err = btree.saveNode(node); err != nil {
				return false, err
			}
			return true, nil
		}

		// Promote the single child as parent's new child instead of this node.
		return node.promoteSingleChildAsParentChild(btree)
	}

	if node.Count == 0 {
		if err := node.unlink(btree); err != nil {
			return false, err
		}
		return true, nil
	}

	if err := btree.saveNode(node); err != nil {
		return false, err
	}
	return true, nil
}

func (node *Node[TK, TV]) unlinkNodeWithNilChild(btree *Btree[TK, TV]) (bool, error) {
	if node.isNilChildren() {
		return false, nil
	}
	return node.promoteSingleChildAsParentChild(btree)
}

func (node *Node[TK, TV]) promoteSingleChildAsParentChild(btree *Btree[TK, TV]) (bool, error) {
	// Promote the single child as parent's new child instead of this node.
	p, err := node.getParent(btree)
	if err != nil {
		return false, err
	}
	if p == nil {
		return false, fmt.Errorf("Can't get parent (Id='%v') of this node.", node.ParentId)
	}
	ion := p.getIndexOfChild(node)
	p.childrenIds[ion] = node.childrenIds[0]
	nc, err := node.getChild(btree, 0)
	if err != nil {
		return false, err
	}
	nc.ParentId = p.Id
	// Save changes to the modified nodes.
	if err = btree.saveNode(nc); err != nil {
		return false, err
	}
	if err = btree.saveNode(p); err != nil {
		return false, err
	}
	// Remove this node since it is now empty.
	err = btree.removeNode(node)
	if err != nil {
		return false, err
	}
	return true, nil
}

// addItemOnNodeWithNilChild handles insert/distribute item on a full node with a nil child, 'should occupy nil child.
func (node *Node[TK, TV]) addItemOnNodeWithNilChild(btree *Btree[TK, TV], item *Item[TK, TV], index int) (bool, error) {
	if node.childrenIds[index] != NilUUID {
		return false, nil
	}
	// Create a new Child node & populate it with the item.
	child := newNode[TK, TV](btree.getSlotLength())
	child.newId(node.Id)
	node.childrenIds[index] = child.Id
	child.Slots[0] = item
	child.Count = 1
	if err := btree.saveNode(node); err != nil {
		return false, err
	}
	if err := btree.saveNode(child); err != nil {
		return false, err
	}
	return true, nil
}

// goRightUpItemOnNodeWithNilChild will point the current item ref to the item to the right or up a parent.
// Applicable when child at index position is nil.
func (node *Node[TK, TV]) goRightUpItemOnNodeWithNilChild(btree *Btree[TK, TV], index int) (bool, error) {
	if node.childrenIds[index] != NilUUID {
		return false, nil
	}
	n := node
	i := index
	for {
		if n == nil {
			btree.setCurrentItemId(NilUUID, 0)
			return false, nil
		}
		// Check if there is an item on the right slot.
		if i < n.Count {
			btree.setCurrentItemId(n.Id, i)
			return true, nil
		}
		// Check if this is not the root node. (Root nodes don't have parent node).
		if n.isRootNode() {
			// this is root node. set to null the current item(End of Btree is reached).
			btree.setCurrentItemId(NilUUID, 0)
			return false, nil
		}
		p, err := n.getParent(btree)
		if err != nil {
			return false, err
		}
		i = p.getIndexOfChild(n)
		n = p
	}
}

// goLeftUpItemOnNodeWithNilChild will point the current item ref to the item to the left or up a parent.
// Applicable when child at index position is nil.
func (node *Node[TK, TV]) goLeftUpItemOnNodeWithNilChild(btree *Btree[TK, TV], index int) (bool, error) {
	if node.childrenIds[index] != NilUUID {
		return false, nil
	}
	n := node
	i := index - 1
	for {
		// Check if SlotIndex is within the maximum slot items and if it is, will index an occupied slot.
		if i >= 0 {
			btree.setCurrentItemId(n.Id, i)
			return true, nil
		}
		if n.isRootNode() {
			// Set to null the current item, end of Btree is reached.
			btree.setCurrentItemId(NilUUID, 0)
			return false, nil
		}
		p, err := n.getParent(btree)
		if err != nil {
			return false, err
		}
		i = p.getIndexOfChild(n) - 1
		n = p
	}
}

// nodeHasNilChild returns true if a node has nil child.
func (node *Node[TK, TV]) nodeHasNilChild(btree *Btree[TK, TV]) bool {
	if !node.hasChildren() {
		return false
	}
	for i := 0; i <= node.Count; i++ {
		if node.childrenIds[i] == NilUUID {
			return true
		}
	}
	return false
}

// distributeItemOnNodeWithNilChild is used to balance load among nodes of a given branch.
func (node *Node[TK, TV]) distributeItemOnNodeWithNilChild(btree *Btree[TK, TV], item *Item[TK, TV]) (bool, error) {
	if !node.hasChildren() {
		return false, nil
	}
	i := 0
	for ; i <= node.Count; i++ {
		if node.childrenIds[i] == NilUUID {
			break
		}
	}
	if i > node.Count {
		return false, nil
	}
	// Create a new Child node & populate it with the item.
	child := newNode[TK, TV](btree.getSlotLength())
	child.newId(node.Id)
	node.childrenIds[i] = child.Id
	child.Slots[0] = item
	child.Count = 1
	if err := btree.saveNode(node); err != nil {
		return false, err
	}
	if err := btree.saveNode(child); err != nil {
		return false, err
	}
	return true, nil
}