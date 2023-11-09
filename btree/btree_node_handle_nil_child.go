package btree

// removeItemOnNodeWithNilChild will manage these remove item cases.
// - remove item on a node slot with nil left child
// - remove item on a node slot with nil right child
// - remove item on the right edge node slot with nil right child
func (node *Node[TK, TV]) removeItemOnNodeWithNilChild(btree *Btree[TK, TV], index int) (bool, error) {
	if node.childrenIds[index] != NilUUID {
		return false, nil
	}
	if index < node.Count-1 {
		itemsToMove := node.Count - index
		moveArrayElements(node.Slots, index+1, index, itemsToMove)
		moveArrayElements(node.childrenIds, index+1, index, itemsToMove+1)
	}
	// Set to nil the last item & its child.
	node.Slots[node.Count-1] = nil
	node.childrenIds[node.Count] = NilUUID
	node.Count--
	return true, btree.saveNode(node)
}

// - insert/distribute item on a full node with a nil child, 'should occupy nil child
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

// - moveToNext on a node with nil child
func (node *Node[TK, TV]) moveToNextItemOnNodeWithNilChild(btree *Btree[TK, TV], index int) (bool, error) {
	if node.childrenIds[index] != NilUUID {
		return false, nil
	}
	n := node
	for {
		if n == nil {
			btree.setCurrentItemId(NilUUID, 0)
			return false, nil
		}
		// Check if SlotIndex is within the maximum slot items and if it is, will index an occupied slot.
		if index < n.Count {
			btree.setCurrentItemId(n.Id, index)
			return true, nil
		}
		// Check if this is not the root node. (Root nodes don't have parent node.)
		if !n.isRootNode() {
			var err error
			index, err = n.getIndexOfNode(btree)
			if err != nil {
				return false, err
			}
			n, err = n.getParent(btree)
			if err != nil {
				return false, err
			}
		} else {
			// this is root node. set to null the current item(End of Btree is reached)
			btree.setCurrentItemId(NilUUID, 0)
			return false, nil
		}
	}
}

// - moveToPrevious on a node with nil child
func (node *Node[TK, TV]) moveToPreviousItemOnNodeWithNilChild(btree *Btree[TK, TV], index int) (bool, error) {
	if node.childrenIds[index] != NilUUID {
		return false, nil
	}
	n := node
	index--
	for {
		// Check if SlotIndex is within the maximum slot items and if it is, will index an occupied slot.
		if index >= 0 {
			btree.setCurrentItemId(n.Id, index)
			return true, nil
		}
		if n.isRootNode() {
			// Set to null the current item, end of Btree is reached.
			btree.setCurrentItemId(NilUUID, 0)
			return false, nil
		}
		i, err := n.getIndexOfNode(btree)
		if err != nil {
			return false, err
		}
		n, err = n.getParent(btree)
		if err != nil {
			return false, err
		}
		index = i - 1
	}
}

// nodeHasNilChild returns true if a node has
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
