package btree

// Cases need to manage:
// - remove item on a node slot with nil left child
// - remove item on a node slot with nil right child
// - remove item on the right edge node slot with nil right child
// - insert/distribute item on a full node with a nil child, 'should occupy nil child
// - moveToXx on a node with nil child
// - find on a node with nil child

// TODO:
func (node *Node[TK, TV]) handleRemoveItemWithNilChild(btree *Btree[TK, TV]) (bool, error) {
	return false, nil
}

// TODO:
func (node *Node[TK, TV]) handleAddItemWithNilChild(btree *Btree[TK, TV], item *Item[TK, TV], index int) (bool, error) {
	return false, nil
}

// TODO:
func (node *Node[TK, TV]) handleMoveToNextItemWithNilChild(btree *Btree[TK, TV], index int) (bool, error) {
	return false, nil
}
// TODO:
func (node *Node[TK, TV]) handleMoveToPreviousItemWithNilChild(btree *Btree[TK, TV], index int) (bool, error) {
	return false, nil
}

// TODO:
func (node *Node[TK, TV]) hasItemWithNilChild(btree *Btree[TK, TV]) bool {
	return false
}
// TODO:
func (node *Node[TK, TV]) handleDistributeItemWithNilChild(btree *Btree[TK, TV], item *Item[TK, TV]) (bool, error) {
	return false, nil
}

// TODO:
func (node *Node[TK, TV]) handleFindItemWithNilChild(btree *Btree[TK, TV], index int) (bool, error) {
	return false, nil
}

// Sample code to handle nil left child on "item remove":
// // Handle remove of an item with nil left child.
// func (node *Node[TK, TV]) handle_nil_left_child(btree *Btree[TK, TV]) (bool, error) {
// 	index := btree.currentItemRef.getNodeItemIndex()
// 	if node.childrenIds[index] == NilUUID {
// 		if index < node.Count-1 {
// 			itemsToMove := node.Count-index
// 			moveArrayElements(node.Slots, index + 1, index, itemsToMove)
// 			moveArrayElements(node.childrenIds, index + 1, index, itemsToMove+1)
// 		}
// 		node.Slots[node.Count-1] = nil
// 		node.childrenIds[node.Count] = NilUUID
// 		node.Count--
// 		return true, btree.saveNode(node)
// 	}
// 	return false, nil
// }
