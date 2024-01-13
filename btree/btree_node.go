package btree

import (
	"context"
	"fmt"
	"sort"
)

// MetaDataType specifies that an object has meta data such as Id & Version fields.
type MetaDataType interface {
	GetId() UUID
	GetVersion() int
	SetVersion(v int)
}

// Item contains key & value pair, plus the version number.
type Item[TK Comparable, TV any] struct {
	// (Internal) Id is the Item's UUID. Id is needed for two reasons:
	// 1. so B-Tree can identify or differentiate item(s) with duplicated Key.
	// 2. used as the Value "data" Id if item's value data is persisted in another
	// data segment, separate from the Node segment(IsValueDataInNodeSegment=false).
	Id UUID
	// Key is the key part in key/value pair.
	Key TK
	// Value is saved nil if data is to be persisted in the "data segment"(& ValueId set to a valid UUID),
	// otherwise it should point to the actual data and persisted in B-Tree Node segment together with the Key.
	Value *TV
	// Version is used for conflict resolution among (in-flight) transactions.
	Version         int
	valueNeedsFetch bool
}

func newItem[TK Comparable, TV any](key TK, value TV) *Item[TK, TV] {
	return &Item[TK, TV]{
		Key:   key,
		Value: &value,
		Id:    NewUUID(),
	}
}

// Node contains a B-Tree node's data.
type Node[TK Comparable, TV any] struct {
	Id          UUID
	ParentId    UUID
	Slots       []*Item[TK, TV]
	Count       int
	Version     int
	indexOfNode int
	ChildrenIds []UUID
}

func (n *Node[TK, TV]) GetId() UUID {
	return n.Id
}
func (n *Node[TK, TV]) GetVersion() int {
	return n.Version
}
func (n *Node[TK, TV]) SetVersion(v int) {
	n.Version = v
}

// newNode creates a new node.
func newNode[TK Comparable, TV any](slotCount int) *Node[TK, TV] {
	return &Node[TK, TV]{
		Slots:       make([]*Item[TK, TV], slotCount),
		indexOfNode: -1,
	}
}

// add an item to the b-tree, will traverse the tree and find the leaf node where to
// properly add the item to, according to the sort order.
// Actual add of items on target node is handled by addOnLeaf method.
func (node *Node[TK, TV]) add(ctx context.Context, btree *Btree[TK, TV], item *Item[TK, TV]) (bool, error) {
	var currentNode = node
	var index int
	for {
		var itemExists bool
		index, itemExists = currentNode.getIndexToInsertTo(btree, item)
		// itemExists will be true if and only if btree.IsUnique() is true, thus,
		// will prevent insert of duplicated key item.
		if itemExists {
			// set the Current item pointer to the duplicate item.
			btree.setCurrentItemId(currentNode.Id, index)
			return false, nil
		}
		if currentNode.hasChildren() {
			ok, err := currentNode.addItemOnNodeWithNilChild(ctx, btree, item, index)
			if err != nil || ok {
				return ok, err
			}
			// if not an outermost node let next lower level node do the 'Add'.
			currentNode, err = currentNode.getChild(ctx, btree, index)
			if err != nil || currentNode == nil {
				return false, err
			}
		} else {
			break
		}
	}
	if btree.isUnique() && currentNode.Count > 0 {
		var currItemIndex = index
		if index > 0 && index >= currentNode.Count {
			currItemIndex--
		}
		if compare(currentNode.Slots[currItemIndex].Key, item.Key) == 0 {
			// set the Current item pointer to the discovered existing item.
			btree.setCurrentItemId(currentNode.Id, currItemIndex)
			return false, nil
		}
	}
	if err := currentNode.addOnLeaf(ctx, btree, item, index); err != nil {
		return false, err
	}
	return true, nil
}

// Add item on the outermost(a.k.a. leaf) node, the end of the recursive traversing thru all inner nodes of the Btree.
// Correct Node to add item to is reached at this point.
func (node *Node[TK, TV]) addOnLeaf(ctx context.Context, btree *Btree[TK, TV], item *Item[TK, TV], index int) error {
	// If node is not yet full.
	if node.Count < btree.getSlotLength() {
		// Insert the Item to target position & "skud" over the items to the right.
		node.insertSlotItem(item, index)
		// Save this TreeNode
		btree.saveNode(node)
		return nil
	}

	// Node is full, distribute or breakup the node (use temp slots in the process).
	copy(btree.tempSlots, node.Slots)

	// Index now contains the correct array element number to insert item into.
	// Skud over then assign the item to the vacatad slot.
	copy(btree.tempSlots[index+1:], btree.tempSlots[index:])
	// Set the item to the newly vacated slot.
	btree.tempSlots[index] = item

	var isVacantSlotInLeft, isVacantSlotInRight bool
	var err error

	slotsHalf := btree.getSlotLength() >> 1
	var isUnBalanced bool
	if !node.isRootNode() {
		isVacantSlotInLeft, err = node.isThereVacantSlotInLeft(ctx, btree, &isUnBalanced)
		if err != nil {
			return err
		}
		isVacantSlotInRight, err = node.isThereVacantSlotInRight(ctx, btree, &isUnBalanced)
		if err != nil {
			return err
		}

		if isVacantSlotInLeft || isVacantSlotInRight {
			// Distribute to either left or right sibling the overflowed item.
			// Copy temp buffer contents to the actual slots.
			var b int16 = 1
			if isVacantSlotInLeft {
				b = 0
			}
			copy(node.Slots, btree.tempSlots[b:])
			// Save this node.
			btree.saveNode(node)

			btree.distributeAction.sourceNode = node
			if isVacantSlotInLeft {
				btree.distributeAction.item = btree.tempSlots[btree.getSlotLength()]
				clear(btree.tempSlots)

				// Vacant in left, create a distribution action request to B-Tree.
				// Logic is: "skud over" the leftmost node's item to parent and the item
				// on parent to left sibling node (recursively).
				btree.distributeAction.distributeToLeft = true
				return nil
			}
			btree.distributeAction.item = btree.tempSlots[0]
			clear(btree.tempSlots)
			// Vacant in right, move the rightmost node item into the vacant slot in right.
			btree.distributeAction.distributeToLeft = false
			return nil
		}

		if isUnBalanced {
			// If this branch is unbalanced, break the "full" node to create new slots.
			// Description :
			// -copy the left half of the slots
			// -copy the right half of the slots
			// -zero out the current slot.
			// -copy the middle slot
			// -allocate memory for children node *s
			// -assign the new children nodes.

			// Initialize should throw an exception if in error.
			rightNode := newNode[TK, TV](btree.getSlotLength())
			rightNode.newId(node.Id)
			leftNode := newNode[TK, TV](btree.getSlotLength())
			leftNode.newId(node.Id)
			copyArrayElements(leftNode.Slots, btree.tempSlots, slotsHalf)
			leftNode.Count = slotsHalf
			copyArrayElements(rightNode.Slots, btree.tempSlots[slotsHalf+1:], slotsHalf)

			rightNode.Count = slotsHalf
			clear(node.Slots)
			node.Slots[0] = btree.tempSlots[slotsHalf]

			// Save this Node, Left & Right Nodes.
			btree.saveNode(leftNode)
			btree.saveNode(rightNode)
			node.ChildrenIds = make([]UUID, btree.getSlotLength()+1)
			node.ChildrenIds[0] = leftNode.Id
			node.ChildrenIds[1] = rightNode.Id
			btree.saveNode(node)

			clear(btree.tempSlots)
			return nil
		}
		// All slots are occupied in this and other siblings' nodes..

		// Prepare this and the right node sibling and promote the temporary parent node(pTempSlot).
		rightNode := newNode[TK, TV](btree.getSlotLength())
		rightNode.newId(node.ParentId)
		// Zero out the node slots in preparation to make it the left sibling.
		clear(node.Slots)

		// Copy the left half of the slots to left sibling(node).
		copyArrayElements(node.Slots, btree.tempSlots, slotsHalf)
		node.Count = slotsHalf
		// Copy the right half of the slots to right sibling.
		copyArrayElements(rightNode.Slots, btree.tempSlots[slotsHalf+1:], slotsHalf)
		rightNode.Count = slotsHalf

		// Copy the middle slot to temp parent slot.
		btree.tempParent = btree.tempSlots[slotsHalf]

		// Assign the new children nodes.
		btree.tempParentChildren[0] = node.Id
		btree.tempParentChildren[1] = rightNode.Id

		p, err := btree.getNode(ctx, node.ParentId)
		if err != nil {
			return err
		}
		if p == nil {
			return fmt.Errorf("Can't get parent (Id='%v') of this Node", node.ParentId)
		}

		//  Save this and Right Node.
		btree.saveNode(node)
		btree.saveNode(rightNode)

		btree.promoteAction.targetNode = p
		btree.promoteAction.slotIndex = p.getIndexOfChild(node)
		return nil
	}

	// Break this node to create available slots.
	// Description :
	// -copy the left half of the temp slots
	// -copy the right half of the temp slots
	// -zero out the current slot.
	// -copy the middle of temp slot to 1st elem of current slot
	// -allocate memory for children node *s
	// -assign the new children nodes.
	rightNode := newNode[TK, TV](btree.getSlotLength())
	rightNode.newId(node.Id)
	leftNode := newNode[TK, TV](btree.getSlotLength())
	leftNode.newId(node.Id)

	copyArrayElements(leftNode.Slots, btree.tempSlots, slotsHalf)
	leftNode.Count = slotsHalf
	copyArrayElements(rightNode.Slots, btree.tempSlots[slotsHalf+1:], slotsHalf)
	rightNode.Count = slotsHalf
	clear(node.Slots)
	node.Slots[0] = btree.tempSlots[slotsHalf]

	node.Count = 1

	// Save Left and Right Nodes.
	btree.saveNode(leftNode)
	btree.saveNode(rightNode)

	node.ChildrenIds = make([]UUID, btree.getSlotLength()+1)
	node.ChildrenIds[0] = leftNode.Id
	node.ChildrenIds[1] = rightNode.Id

	// Save this Node.
	btree.saveNode(node)
	clear(btree.tempSlots)

	return nil
}

func (node *Node[TK, TV]) find(ctx context.Context, btree *Btree[TK, TV], key TK, firstItemWithKey bool) (bool, error) {
	n := node
	foundItemIndex := 0
	foundNodeId := NilUUID
	var err error
	index := 0
	for n != nil {
		index = 0
		if n.Count > 0 {
			index = sort.Search(n.Count, func(index int) bool {
				return compare(n.Slots[index].Key, key) >= 0
			})
			// If key is found in node n.
			if index < n.Count && compare(n.Slots[index].Key, key) == 0 {
				// Make the found node & item index the "current item" of btree.
				foundNodeId = n.Id
				foundItemIndex = index
				if !firstItemWithKey {
					break
				}
			}
		}
		// Check children if there are.
		if n.hasChildren() {
			// Short circuit if child is nil as there is no more duplicate on left side.
			if n.ChildrenIds[index] == NilUUID {
				break
			}
			n, err = n.getChild(ctx, btree, index)
			if err != nil {
				return false, err
			}
			continue
		}
		// Short circuit loop if there are no more children.
		break
	}
	if !foundNodeId.IsNil() {
		btree.setCurrentItemId(foundNodeId, foundItemIndex)
		return true, nil
	}
	// This must be the outermost node
	// This block will make this item the current one to give chance to the Btree
	// caller the chance to check the items having the nearest key to the one it is interested at.
	if index == n.Count {
		// make sure i points to valid item
		index--
	}
	if n.Slots[index] != nil {
		btree.setCurrentItemId(n.Id, index)
	} else {
		index--
		// Update Current Item of this Node and nearest to the Key in sought Slot index
		btree.setCurrentItemId(n.Id, index)
		// Make the next item the current item. This has the effect of positioning making the next greater item the current item.
		_, err = n.moveToNext(ctx, btree)
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

func (node *Node[TK, TV]) moveToFirst(ctx context.Context, btree *Btree[TK, TV]) (bool, error) {
	n := node
	var prev *Node[TK, TV]
	var err error
	for n.ChildrenIds != nil {
		prev = n
		cid := n.ChildrenIds[0]
		// If nil Child, then we've reached the 1st item's node, stop the walk.
		if cid == NilUUID {
			break
		}
		n, err = btree.getNode(ctx, cid)
		if err != nil {
			return false, err
		}
		if n == nil {
			break
		}
	}
	if n != nil {
		prev = n
	}
	btree.setCurrentItemId(prev.Id, 0)
	return true, nil
}

func (node *Node[TK, TV]) moveToLast(ctx context.Context, btree *Btree[TK, TV]) (bool, error) {
	n := node
	var err error
	for n.ChildrenIds != nil {
		cid := n.ChildrenIds[n.Count]
		// If nil Child, then we've reached the last item's node, stop the walk.
		if cid == NilUUID {
			break
		}
		n, err = btree.getNode(ctx, cid)
		if err != nil {
			return false, err
		}
	}
	btree.setCurrentItemId(n.Id, n.Count-1)
	return n.Id != NilUUID, nil
}

func (node *Node[TK, TV]) moveToNext(ctx context.Context, btree *Btree[TK, TV]) (bool, error) {
	n := node
	slotIndex := btree.currentItemRef.getNodeItemIndex()
	slotIndex++
	goRightDown := n.hasChildren()
	var err error
	if goRightDown {
		for {
			if n == nil {
				btree.setCurrentItemId(NilUUID, 0)
				return false, nil
			}
			if n.hasChildren() {
				if ok, err := n.goRightUpItemOnNodeWithNilChild(ctx, btree, slotIndex); ok || err != nil {
					return ok, err
				}
				n, err = n.getChild(ctx, btree, slotIndex)
				if err != nil {
					return false, err
				}
				slotIndex = 0
			} else {
				btree.setCurrentItemId(n.Id, 0)
				return true, nil
			}
		}
	}
	for {
		if n == nil {
			btree.setCurrentItemId(NilUUID, 0)
			return false, nil
		}
		// Check if SlotIndex is within the maximum slot items and if it is, will index an occupied slot.
		if slotIndex < n.Count {
			btree.setCurrentItemId(n.Id, slotIndex)
			return true, nil
		}
		// Check if this is the root node. (Root nodes don't have parent node.)
		if n.isRootNode() {
			// this is root node. set to null the current item(End of Btree is reached)
			btree.setCurrentItemId(NilUUID, 0)
			return false, nil
		}
		p, err := n.getParent(ctx, btree)
		if err != nil {
			return false, err
		}
		slotIndex = p.getIndexOfChild(n)
		n = p
	}
}

func (node *Node[TK, TV]) moveToPrevious(ctx context.Context, btree *Btree[TK, TV]) (bool, error) {
	n := node
	slotIndex := btree.currentItemRef.getNodeItemIndex()
	goLeftDown := n.hasChildren()
	var err error
	if goLeftDown {
		for {
			if n.hasChildren() {
				if ok, err := n.goLeftUpItemOnNodeWithNilChild(ctx, btree, slotIndex); ok || err != nil {
					return ok, err
				}
				n, err = n.getChild(ctx, btree, slotIndex)
				if err != nil {
					return false, err
				}
				if n == nil {
					// Set to null the current item, end of Btree is reached.
					btree.setCurrentItemId(NilUUID, 0)
					return false, nil
				}
				slotIndex = n.Count
			} else {
				// 'SlotIndex -1' since we are now using SlotIndex as index to pSlots.
				btree.setCurrentItemId(n.Id, slotIndex-1)
				return true, nil
			}
		}
	}
	slotIndex--
	for {
		// Check if SlotIndex is within the maximum slot items and if it is, will index an occupied slot.
		if slotIndex >= 0 {
			btree.setCurrentItemId(n.Id, slotIndex)
			return true, nil
		}
		if n.isRootNode() {
			// Set to null the current item, end of Btree is reached.
			btree.setCurrentItemId(NilUUID, 0)
			return false, nil
		}
		p, err := n.getParent(ctx, btree)
		if err != nil {
			return false, err
		}
		slotIndex = p.getIndexOfChild(n) - 1
		n = p
	}
}

func (node *Node[TK, TV]) fixVacatedSlot(ctx context.Context, btree *Btree[TK, TV]) error {
	// If there are more than 1 items in slot then we move the items 1 slot to omit deleted item slot.
	if node.Count > 1 {
		position := btree.currentItemRef.getNodeItemIndex()
		if position < node.Count-1 {
			moveArrayElements(node.Slots,
				position,
				position+1,
				node.Count-position-1)
		}
		// Nullify the last slot.
		node.Count--
		node.Slots[node.Count] = nil
		// We don't fix the children since there are no children at this scenario.
		btree.saveNode(node)
		return nil
	}
	if node.isRootNode() {
		// Delete the single item in root node.
		node.Count = 0
		node.Slots[0] = nil
		btree.setCurrentItemId(NilUUID, 0)
		btree.saveNode(node)
		return nil
	}
	if ok, err := node.unlinkNodeWithNilChild(ctx, btree); ok || err != nil {
		return err
	}
	return node.unlink(ctx, btree)
}

func (node *Node[TK, TV]) isNilChildren() bool {
	for _, id := range node.ChildrenIds {
		if id != NilUUID {
			return false
		}
	}
	return true
}

// Returns true if a slot is available in left side siblings of this node modified to suit possible unbalanced branch.
func (node *Node[TK, TV]) isThereVacantSlotInLeft(ctx context.Context, btree *Btree[TK, TV], isUnBalanced *bool) (bool, error) {
	*isUnBalanced = false
	if !btree.StoreInfo.LeafLoadBalancing {
		return false, nil
	}
	// Start from this node.
	temp := node
	for temp != nil {
		if temp.nodeHasNilChild(btree) {
			return true, nil
		}
		if temp.ChildrenIds != nil {
			*isUnBalanced = true
			return false, nil
		}
		if !temp.isFull() {
			return true, nil
		}
		var err error
		temp, err = temp.getLeftSibling(ctx, btree)
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

// Returns true if a slot is available in right side siblings of this node modified to suit possible unbalanced branch.
func (node *Node[TK, TV]) isThereVacantSlotInRight(ctx context.Context, btree *Btree[TK, TV], isUnBalanced *bool) (bool, error) {
	*isUnBalanced = false
	if !btree.StoreInfo.LeafLoadBalancing {
		return false, nil
	}
	// Start from this node.
	temp := node
	for temp != nil {
		if temp.nodeHasNilChild(btree) {
			return true, nil
		}
		if temp.ChildrenIds != nil {
			*isUnBalanced = true
			return false, nil
		}
		if !temp.isFull() {
			return true, nil
		}
		var err error
		temp, err = temp.getRightSibling(ctx, btree)
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

// Returns left sibling or nil if finished traversing left side nodes.
func (node *Node[TK, TV]) getLeftSibling(ctx context.Context, btree *Btree[TK, TV]) (*Node[TK, TV], error) {
	index, err := node.getIndexOfNode(ctx, btree)
	if err != nil {
		return nil, err
	}
	p, err := node.getParent(ctx, btree)
	if err != nil {
		return nil, err
	}
	if p != nil {
		// If we are not at the leftmost sibling yet..
		if index > 0 && index <= p.Count {
			return p.getChild(ctx, btree, index-1)
		}
	}
	// Leftmost was already reached..
	return nil, nil
}

// Returns right sibling or nil if finished traversing right side nodes.
func (node *Node[TK, TV]) getRightSibling(ctx context.Context, btree *Btree[TK, TV]) (*Node[TK, TV], error) {
	index, err := node.getIndexOfNode(ctx, btree)
	if err != nil {
		return nil, err
	}
	p, err := node.getParent(ctx, btree)
	if err != nil {
		return nil, err
	}
	if p != nil && index >= 0 {
		// If we are not at the rightmost sibling yet..
		if index < p.Count {
			return p.getChild(ctx, btree, index+1)
		}
	}
	// Rightmost was already reached..
	return nil, nil
}

func (node *Node[TK, TV]) getIndexOfChild(child *Node[TK, TV]) int {
	parent := node
	// Make sure we don't access an invalid node item.
	if parent.ChildrenIds != nil &&
		(child.indexOfNode == -1 || child.Id != parent.ChildrenIds[child.indexOfNode]) {
		for child.indexOfNode = 0; child.indexOfNode <= len(parent.Slots); child.indexOfNode++ {
			if parent.ChildrenIds[child.indexOfNode].IsNil() {
				continue
			}
			if parent.ChildrenIds[child.indexOfNode] == child.Id {
				break
			}
		}
	}
	return child.indexOfNode
}

// Returns index of this node relative to parent.
func (node *Node[TK, TV]) getIndexOfNode(ctx context.Context, btree *Btree[TK, TV]) (int, error) {
	parent, err := node.getParent(ctx, btree)
	if err != nil {
		return -1, err
	}
	if parent != nil {
		return parent.getIndexOfChild(node), nil
	}
	// Just return 0 if called in the root node, anyway,
	// the caller code should check if it is the root node and not call this function if it is!
	return 0, nil
}

func (node *Node[TK, TV]) getParent(ctx context.Context, btree *Btree[TK, TV]) (*Node[TK, TV], error) {
	if node.ParentId.IsNil() {
		return nil, nil
	}
	return btree.getNode(ctx, node.ParentId)
}

func (node *Node[TK, TV]) isFull() bool {
	return node.Count >= len(node.Slots)
}

func (node *Node[TK, TV]) insertSlotItem(item *Item[TK, TV], position int) {
	copy(node.Slots[position+1:], node.Slots[position:])
	node.Slots[position] = item
	node.Count++
}

func (node *Node[TK, TV]) getIndexToInsertTo(btree *Btree[TK, TV], item *Item[TK, TV]) (int, bool) {
	if node.Count == 0 {
		// Empty node.
		return 0, false
	}
	index := sort.Search(node.Count, func(index int) bool {
		return compare(node.Slots[index].Key, item.Key) >= 0
	})
	if btree.isUnique() {
		i := index
		if i >= node.Count {
			i--
		}
		// Returns index in slot that is available for insert to.
		// Also returns true if an existing item with such key is found.
		return index, compare(node.Slots[i].Key, item.Key) == 0
	}
	// Returns index in slot that is available for insert to.
	return index, false
}

// Transaction will resolve story of fetching Nodes via logical Id vs. physical Id. Example, in a transaction,
// like when adding an item, newly created nodes need to be using UUID that then becomes logical Id
// during commit. When working with Children logical Ids(saved in backend!), we need to convert logical to physical Id.
func (node *Node[TK, TV]) getChild(ctx context.Context, btree *Btree[TK, TV], childSlotIndex int) (*Node[TK, TV], error) {
	id := node.getChildId(childSlotIndex)
	if id == NilUUID {
		return nil, nil
	}
	return btree.getNode(ctx, id)
}

func (node *Node[TK, TV]) getChildren(ctx context.Context, btree *Btree[TK, TV]) ([]*Node[TK, TV], error) {
	children := make([]*Node[TK, TV], len(node.ChildrenIds))
	var err error
	for i, id := range node.ChildrenIds {
		if id == NilUUID {
			continue
		}
		children[i], err = btree.getNode(ctx, id)
		if err != nil {
			return nil, err
		}
	}
	return children, nil
}

// hasChildren returns true if node has children or not.
func (node *Node[TK, TV]) hasChildren() bool {
	return node.ChildrenIds != nil && len(node.ChildrenIds) > 0
}

// isRootNode returns true if node has no parent.
func (node *Node[TK, TV]) isRootNode() bool {
	return node.ParentId == NilUUID
}

func (node *Node[TK, TV]) distributeToLeft(ctx context.Context, btree *Btree[TK, TV], item *Item[TK, TV]) error {
	if ok := node.distributeItemOnNodeWithNilChild(btree, item); ok {
		return nil
	}
	if node.isFull() {
		// counter-clockwise rotation..
		//	----
		//	|  |
		//	-> |
		// NOTE: we don't check for null returns as this method is called only when there is vacant in left
		parent, err := node.getParent(ctx, btree)
		if err != nil {
			return err
		}

		indexOfNode := parent.getIndexOfChild(node)
		if indexOfNode > parent.Count {
			return nil
		}

		// Let controller to make another call to distribute item to left action.
		btree.distributeAction.sourceNode, err = node.getLeftSibling(ctx, btree)
		if err != nil {
			return err
		}

		btree.distributeAction.item = parent.Slots[indexOfNode-1]
		btree.distributeAction.distributeToLeft = true

		// Update Parent (remove node and add updated one).
		parent.Slots[indexOfNode-1] = node.Slots[0]
		btree.saveNode(parent)
		moveArrayElements(node.Slots, 0, 1, btree.getSlotLength()-1)
	} else {
		node.Count++
	}
	node.Slots[node.Count-1] = item
	btree.saveNode(node)
	return nil
}

func (node *Node[TK, TV]) distributeToRight(ctx context.Context, btree *Btree[TK, TV], item *Item[TK, TV]) error {
	if ok := node.distributeItemOnNodeWithNilChild(btree, item); ok {
		return nil
	}
	if node.isFull() {
		// clockwise rotation..
		//	----
		//	|  |
		//	| <-
		parent, err := node.getParent(ctx, btree)
		if err != nil {
			return nil
		}
		i := parent.getIndexOfChild(node)

		// Let controller to make another call to distribute item to right action.
		btree.distributeAction.sourceNode, err = node.getRightSibling(ctx, btree)
		if err != nil {
			return nil
		}
		btree.distributeAction.item = parent.Slots[i]
		btree.distributeAction.distributeToLeft = false

		parent.Slots[i] = node.Slots[node.Count-1]
		btree.saveNode(parent)
	} else {
		node.Count++
	}
	moveArrayElements(node.Slots, 1, 0, btree.getSlotLength()-1)
	node.Slots[0] = item
	btree.saveNode(node)
	return nil
}

func (node *Node[TK, TV]) promote(ctx context.Context, btree *Btree[TK, TV], indexPosition int) error {
	noOfOccupiedSlots := node.Count
	index := indexPosition
	if noOfOccupiedSlots < btree.getSlotLength() {
		// Node is not yet full.. insert the parent.
		shiftSlots(node.Slots, index, noOfOccupiedSlots)
		if index > noOfOccupiedSlots {
			index = noOfOccupiedSlots
		}
		node.Slots[index] = btree.tempParent

		// Insert the left child.
		node.ChildrenIds[index] = btree.tempParentChildren[0]
		// Insert the right child.
		shiftSlots(node.ChildrenIds, index+1, noOfOccupiedSlots+1)
		node.Count++
		node.ChildrenIds[index+1] = btree.tempParentChildren[1]
		btree.saveNode(node)
		return nil
	}

	// Insert to temp slots.. node is full, use TempSlots
	// NOTE: ensure node & its children being promoted will point to the correct
	// new ParentId as recursive node breakup occurs...
	copyArrayElements(btree.tempSlots, node.Slots, btree.getSlotLength())
	shiftSlots(btree.tempSlots, index, btree.getSlotLength())
	btree.tempSlots[index] = btree.tempParent
	copyArrayElements(btree.tempChildren, node.ChildrenIds, btree.getSlotLength()+1)

	// Insert the left child.
	btree.tempChildren[index] = btree.tempParentChildren[0]
	// Insert the right child.
	shiftSlots(btree.tempChildren, index+1, noOfOccupiedSlots+1)
	btree.tempChildren[index+1] = btree.tempParentChildren[1]

	// Try to break up the node into 2 siblings.
	slotsHalf := btree.getSlotLength() >> 1

	if node.isRootNode() {
		// No parent, break up this node into two children & keep node as root.
		leftNode := newNode[TK, TV](btree.getSlotLength())
		leftNode.newId(node.Id)

		rightNode := newNode[TK, TV](btree.getSlotLength())
		rightNode.newId(node.Id)

		// Copy the left half of the slots
		copyArrayElements(leftNode.Slots, btree.tempSlots, slotsHalf)
		leftNode.Count = slotsHalf
		// Copy the right half of the slots
		copyArrayElements(rightNode.Slots, btree.tempSlots[slotsHalf+1:], slotsHalf)
		rightNode.Count = slotsHalf
		leftNode.ChildrenIds = make([]UUID, btree.getSlotLength()+1)
		rightNode.ChildrenIds = make([]UUID, btree.getSlotLength()+1)
		// Copy the left half of the children nodes.
		copyArrayElements(leftNode.ChildrenIds, btree.tempChildren, slotsHalf+1)
		// Copy the right half of the children nodes.
		copyArrayElements(rightNode.ChildrenIds, btree.tempChildren[slotsHalf+1:], slotsHalf+1)

		// Reset this Node.
		clear(node.Slots)
		clear(node.ChildrenIds)

		// Make the left sibling parent of its children.
		leftNode.updateChildrenParent(ctx, btree)

		// Make the right sibling parent of its children.
		rightNode.updateChildrenParent(ctx, btree)

		// Copy the middle slot
		node.Slots[0] = btree.tempSlots[slotsHalf]
		node.Count = 1

		// Assign the new children nodes.
		node.ChildrenIds[0] = leftNode.Id
		node.ChildrenIds[1] = rightNode.Id
		btree.saveNode(node)
		btree.saveNode(leftNode)
		btree.saveNode(rightNode)
		return nil
	}
	// Prepare this and the right node sibling and promote the temporary parent node(btree.tempParent).
	// This will be the left sibling !
	rightNode := newNode[TK, TV](btree.getSlotLength())
	rightNode.newId(node.ParentId)
	rightNode.ChildrenIds = make([]UUID, btree.getSlotLength()+1)

	// Zero out the current slot.
	clear(node.Slots)
	// Zero out this children node pointers.
	clear(node.ChildrenIds)

	// Copy the left half of the slots to left sibling(this)
	copyArrayElements(node.Slots, btree.tempSlots, slotsHalf)
	node.Count = slotsHalf

	// Copy the right half of the slots to right sibling
	copyArrayElements(rightNode.Slots, btree.tempSlots[slotsHalf+1:], slotsHalf)
	rightNode.Count = slotsHalf
	// Copy the left half of the children nodes.
	copyArrayElements(node.ChildrenIds, btree.tempChildren, slotsHalf+1)

	// Copy the right half of the children nodes.
	copyArrayElements(rightNode.ChildrenIds, btree.tempChildren[slotsHalf+1:], slotsHalf+1)

	// Make the right sibling parent of its children.
	if err := rightNode.updateChildrenParent(ctx, btree); err != nil {
		return err
	}
	btree.saveNode(rightNode)
	// Make "all" of the left sibling parent of its children.
	if err := node.updateChildrenParent(ctx, btree); err != nil {
		return err
	}
	btree.saveNode(node)

	// Copy the middle slot.
	btree.tempParent = btree.tempSlots[slotsHalf]
	// Assign the new children nodes.
	btree.tempParentChildren[0] = node.Id
	btree.tempParentChildren[1] = rightNode.Id

	// Trigger another promotion.
	var err error
	btree.promoteAction.targetNode, err = node.getParent(ctx, btree)
	if err != nil {
		return err
	}
	btree.promoteAction.slotIndex = btree.promoteAction.targetNode.getIndexOfChild(node)
	return nil
}

func (node *Node[TK, TV]) newId(parentId UUID) {
	// Set the Physical Ids, transaction commit should handle resolving physical & logical Ids.
	node.Id = NewUUID()
	node.ParentId = parentId
}

func (node *Node[TK, TV]) getChildId(index int) UUID {
	if len(node.ChildrenIds) == 0 {
		return NilUUID
	}
	return node.ChildrenIds[index]
}

func (node *Node[TK, TV]) updateChildrenParent(ctx context.Context, btree *Btree[TK, TV]) error {
	if !node.hasChildren() {
		return nil
	}
	children, err := node.getChildren(ctx, btree)
	if err != nil {
		return err
	}
	// Make node parent of its children.
	for index := 0; index < len(children); index++ {
		if children[index] != nil {
			children[index].ParentId = node.Id
			btree.saveNode(children[index])
		}
	}
	return nil
}

// copyArrayElements is a helper function for internal use only.
func copyArrayElements[T any](destination, source []T, count int) {
	if source == nil || destination == nil {
		return
	}
	for i := 0; i < count; i++ {
		destination[i] = source[i]
	}
}

func shiftSlots[T any](array []T, position int, noOfOccupiedSlots int) {
	if position < noOfOccupiedSlots {
		// Create a vacant slot by shifting node contents one slot.
		moveArrayElements(array, position+1, position, noOfOccupiedSlots-position)
	}
}

// moveArrayElements is a helper function for internal use only.
func moveArrayElements[T any](array []T, destStartIndex, srcStartIndex, count int) {
	if array == nil {
		return
	}
	addValue := -1
	srcIndex := srcStartIndex + count - 1
	destIndex := destStartIndex + count - 1
	if destStartIndex < srcStartIndex {
		srcIndex = srcStartIndex
		destIndex = destStartIndex
		addValue = 1
	}
	for i := 0; i < count; i++ {
		// Only process if w/in array range.
		if destIndex < 0 || srcIndex < 0 || destIndex >= len(array) || srcIndex >= len(array) {
			break
		}
		array[destIndex] = array[srcIndex]
		destIndex = destIndex + addValue
		srcIndex = srcIndex + addValue
	}
}

func (node *Node[TK, TV]) unlink(ctx context.Context, btree *Btree[TK, TV]) error {
	p, err := node.getParent(ctx, btree)
	if err != nil {
		return err
	}
	if !p.hasChildren() {
		return nil
	}
	// Prune empty children.
	i := p.getIndexOfChild(node)
	p.ChildrenIds[i] = NilUUID
	if p.isNilChildren() {
		p.ChildrenIds = nil
	}
	btree.saveNode(p)
	btree.removeNode(node)
	return nil
}
