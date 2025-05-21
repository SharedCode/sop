package btree

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop"
)

// removeItemOnNodeWithNilChild will manage these remove item cases.
// - remove item on a node slot with nil left child
// - remove item on a node slot with nil right child
// - remove item on the right edge node slot with nil right child
func (node *Node[TK, TV]) removeItemOnNodeWithNilChild(ctx context.Context, btree *Btree[TK, TV], index int) (bool, error) {
	if !node.hasChildren() || (node.ChildrenIDs[index] != sop.NilUUID && node.ChildrenIDs[index+1] != sop.NilUUID) {
		return false, nil
	}
	if node.ChildrenIDs[index] == sop.NilUUID {
		if index < node.Count {
			itemsToMove := node.Count - index
			moveArrayElements(node.Slots, index, index+1, itemsToMove)
			moveArrayElements(node.ChildrenIDs, index, index+1, itemsToMove+1)
		}
	} else if node.ChildrenIDs[index+1] == sop.NilUUID {
		if index < node.Count {
			itemsToMove := node.Count - index
			moveArrayElements(node.Slots, index, index+1, itemsToMove)
			moveArrayElements(node.ChildrenIDs, index+1, index+2, itemsToMove+1)
		}
	}
	// Set to nil the last item & its child.
	node.Slots[node.Count-1] = nil
	node.ChildrenIDs[node.Count] = sop.NilUUID
	node.Count--

	if node.Count == 0 && node.ChildrenIDs[0] != sop.NilUUID {
		if node.isRootNode() {
			// Copy contents of the child to this root node.
			nc, err := node.getChild(ctx, btree, 0)
			if err != nil {
				return false, err
			}
			if nc == nil {
				return false, fmt.Errorf("can't get child (ID='%v') of this root node", node.ChildrenIDs[0])
			}
			copy(node.Slots, nc.Slots)
			node.Count = nc.Count
			if nc.hasChildren() {
				copy(node.ChildrenIDs, nc.ChildrenIDs)
				if err = node.updateChildrenParent(ctx, btree); err != nil {
					return false, err
				}
			} else {
				// Nilify the child because we've merged its contents to root node.
				node.ChildrenIDs[0] = sop.NilUUID
				if node.isNilChildren() {
					node.ChildrenIDs = nil
				}
			}
			btree.removeNode(nc)
			btree.saveNode(node)
			return true, nil
		}

		// Promote the single child as parent's new child instead of this node.
		return node.promoteSingleChildAsParentChild(ctx, btree)
	}

	if node.Count == 0 {
		if err := node.unlink(ctx, btree); err != nil {
			return false, err
		}
		return true, nil
	}

	btree.saveNode(node)
	return true, nil
}

func (node *Node[TK, TV]) unlinkNodeWithNilChild(ctx context.Context, btree *Btree[TK, TV]) (bool, error) {
	if node.isNilChildren() {
		return false, nil
	}
	return node.promoteSingleChildAsParentChild(ctx, btree)
}

func (node *Node[TK, TV]) promoteSingleChildAsParentChild(ctx context.Context, btree *Btree[TK, TV]) (bool, error) {
	// Promote the single child as parent's new child instead of this node.
	p, err := node.getParent(ctx, btree)
	if err != nil {
		return false, err
	}
	if p == nil {
		return false, fmt.Errorf("can't get parent (ID='%v') of this node", node.ParentID)
	}
	ion := p.getIndexOfChild(node)
	p.ChildrenIDs[ion] = node.ChildrenIDs[0]
	nc, err := node.getChild(ctx, btree, 0)
	if err != nil {
		return false, err
	}
	nc.ParentID = p.ID
	// Save changes to the modified nodes.
	btree.saveNode(nc)
	btree.saveNode(p)
	// Remove this node since it is now empty.
	btree.removeNode(node)
	return true, nil
}

// addItemOnNodeWithNilChild handles insert/distribute item on a full node with a nil child, 'should occupy nil child.
func (node *Node[TK, TV]) addItemOnNodeWithNilChild(btree *Btree[TK, TV], item *Item[TK, TV], index int) (bool, error) {
	if node.ChildrenIDs[index] != sop.NilUUID {
		return false, nil
	}
	// Create a new Child node & populate it with the item.
	child := newNode[TK, TV](btree.getSlotLength())
	child.newID(node.ID)
	node.ChildrenIDs[index] = child.ID
	child.Slots[0] = item
	child.Count = 1
	btree.saveNode(node)
	btree.saveNode(child)
	return true, nil
}

// goRightUpItemOnNodeWithNilChild will point the current item ref to the item to the right or up a parent.
// Applicable when child at index position is nil.
func (node *Node[TK, TV]) goRightUpItemOnNodeWithNilChild(ctx context.Context, btree *Btree[TK, TV], index int) (bool, error) {
	if node.ChildrenIDs[index] != sop.NilUUID {
		return false, nil
	}
	n := node
	i := index
	for {
		if n == nil {
			btree.setCurrentItemID(sop.NilUUID, 0)
			return false, nil
		}
		// Check if there is an item on the right slot.
		if i < n.Count {
			btree.setCurrentItemID(n.ID, i)
			return true, nil
		}
		// Check if this is not the root node. (Root nodes don't have parent node).
		if n.isRootNode() {
			// this is root node. set to null the current item(End of Btree is reached).
			btree.setCurrentItemID(sop.NilUUID, 0)
			return false, nil
		}
		p, err := n.getParent(ctx, btree)
		if err != nil {
			return false, err
		}
		i = p.getIndexOfChild(n)
		n = p
	}
}

// goLeftUpItemOnNodeWithNilChild will point the current item ref to the item to the left or up a parent.
// Applicable when child at index position is nil.
func (node *Node[TK, TV]) goLeftUpItemOnNodeWithNilChild(ctx context.Context, btree *Btree[TK, TV], index int) (bool, error) {
	if node.ChildrenIDs[index] != sop.NilUUID {
		return false, nil
	}
	n := node
	i := index - 1
	for {
		// Check if SlotIndex is within the maximum slot items and if it is, will index an occupied slot.
		if i >= 0 {
			btree.setCurrentItemID(n.ID, i)
			return true, nil
		}
		if n.isRootNode() {
			// Set to null the current item, end of Btree is reached.
			btree.setCurrentItemID(sop.NilUUID, 0)
			return false, nil
		}
		p, err := n.getParent(ctx, btree)
		if err != nil {
			return false, err
		}
		i = p.getIndexOfChild(n) - 1
		n = p
	}
}

// nodeHasNilChild returns true if a node has nil child.
func (node *Node[TK, TV]) nodeHasNilChild() bool {
	if !node.hasChildren() {
		return false
	}
	for i := 0; i <= node.Count; i++ {
		if node.ChildrenIDs[i] == sop.NilUUID {
			return true
		}
	}
	return false
}

// distributeItemOnNodeWithNilChild is used to balance load among nodes of a given branch.
func (node *Node[TK, TV]) distributeItemOnNodeWithNilChild(btree *Btree[TK, TV], item *Item[TK, TV]) bool {
	if !node.hasChildren() {
		return false
	}
	i := 0
	for ; i <= node.Count; i++ {
		if node.ChildrenIDs[i] == sop.NilUUID {
			break
		}
	}
	if i > node.Count {
		return false
	}
	// Create a new Child node & populate it with the item.
	child := newNode[TK, TV](btree.getSlotLength())
	child.newID(node.ID)
	node.ChildrenIDs[i] = child.ID
	child.Slots[0] = item
	child.Count = 1
	btree.saveNode(node)
	btree.saveNode(child)
	return true
}
