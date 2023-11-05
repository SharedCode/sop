package btree

import (
	"fmt"
	"sort"
)

// Item contains key & value pair, plus the version number.
type Item[TK Comparable, TV any] struct {
	// Key is the key part in key/value pair.
	Key TK
	// Value is saved nil if data is to be persisted in the "data segment"(& ValueId set to a valid UUID),
	// otherwise it should point to the actual data and persisted in B-Tree Node segment together with the Key.
	Value *TV
	// ValueLogicalId should be a valid (logical) Id of the data if it is saved in the "data segment",
	// otherwise this should be nil(unused).
	ValueLogicalId  UUID
	Version         int
	valueNeedsFetch bool
}

// Node contains a B-Tree node's data.
type Node[TK Comparable, TV any] struct {
	Id          UUID
	ParentId    UUID
	Slots       []*Item[TK, TV]
	Count       int
	Version     int
	IsDeleted   bool
	indexOfNode int
	childrenIds []UUID
}

func newNode[TK Comparable, TV any](slotCount int) *Node[TK, TV] {
	return &Node[TK, TV]{
		Slots:       make([]*Item[TK, TV], slotCount),
		indexOfNode: -1,
	}
}

func (node *Node[TK, TV]) add(btree *Btree[TK, TV], item *Item[TK, TV]) (bool, error) {
	var currentNode = node
	var index int
	var parent *Node[TK, TV]
	for {
		var itemExists bool
		index, itemExists = currentNode.getIndexToInsertTo(btree, item)
		if itemExists {
			// set the Current item pointer to the duplicate item.
			btree.setCurrentItemId(currentNode.Id, index)
			return false, nil
		}
		if currentNode.hasChildren() {
			parent = nil
			// if not an outermost node let next lower level node do the 'Add'.
			currentNode, err := currentNode.getChild(btree, index)
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
		i := compare(currentNode.Slots[currItemIndex].Key, item.Key)
		if i == 0 {
			// set the Current item pointer to the discovered existing item.
			btree.setCurrentItemId(currentNode.Id, currItemIndex)
			return false, nil
		}
	}
	currentNode.addOnLeaf(btree, item, index, parent)
	return true, nil
}

// Outermost(a.k.a. leaf) node, the end of the recursive traversing thru all inner nodes of the Btree.
// Correct Node is reached at this point!
func (node *Node[TK, TV]) addOnLeaf(btree *Btree[TK, TV], item *Item[TK, TV], index int, parent *Node[TK, TV]) error {
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
	if !node.ParentId.IsNil() {
		isVacantSlotInLeft, err = node.isThereVacantSlotInLeft(btree, &isUnBalanced)
		if err != nil {
			return err
		}
		isVacantSlotInRight, err = node.isThereVacantSlotInRight(btree, &isUnBalanced)
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
			copyArrayElements(rightNode.Slots, btree.tempSlots[:slotsHalf+1], slotsHalf)

			rightNode.Count = slotsHalf
			clear(node.Slots)
			node.Slots[0] = btree.tempSlots[slotsHalf]

			// Save this Node, Left & Right Nodes.
			btree.saveNode(leftNode)
			btree.saveNode(rightNode)
			node.childrenIds = make([]UUID, btree.getSlotLength()+1)
			node.childrenIds[0] = leftNode.Id
			node.childrenIds[1] = rightNode.Id
			btree.saveNode(node)

			clear(btree.tempSlots)
			return nil
		}
		// All slots are occupied in this and other siblings' nodes..

		// Prepare this and the right node sibling and promote the temporary parent node(pTempSlot).
		rightNode := newNode[TK, TV](btree.getSlotLength())
		rightNode.newId(node.Id)
		// Zero out the node slots in preparation to make it the left sibling.
		clear(node.Slots)

		// Copy the left half of the slots to left sibling(node).
		copyArrayElements(node.Slots, btree.tempSlots, slotsHalf)
		node.Count = slotsHalf
		// Copy the right half of the slots to right sibling.
		copyArrayElements(rightNode.Slots, btree.tempSlots[:slotsHalf+1], slotsHalf)
		rightNode.Count = slotsHalf

		// Copy the middle slot to temp parent slot.
		btree.tempParent = btree.tempSlots[slotsHalf]

		//  Save this and Right Node.
		btree.saveNode(node)
		btree.saveNode(rightNode)

		// Assign the new children nodes.
		btree.tempParentChildren[0] = node.Id
		btree.tempParentChildren[1] = rightNode.Id

		o, err := btree.getNode(node.ParentId)
		if err != nil {
			return err
		}
		if o == nil {
			return fmt.Errorf("Can't get parent (Id='%v') of this Node.", node.ParentId)
		}

		btree.promoteAction.nodeForPromotion = o
		i, err := node.getIndexOfNode(btree)
		if err != nil {
			return err
		}
		btree.promoteAction.nodeForPromotionIndex = i
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
	copyArrayElements(rightNode.Slots, btree.tempSlots[:slotsHalf+1], slotsHalf)
	rightNode.Count = slotsHalf
	clear(node.Slots)
	node.Slots[0] = btree.tempSlots[slotsHalf]

	node.Count = 1

	// Save Left and Right Nodes.
	btree.saveNode(leftNode)
	btree.saveNode(rightNode)

	node.childrenIds = make([]UUID, btree.getSlotLength()+1)
	node.childrenIds[0] = leftNode.Id
	node.childrenIds[1] = rightNode.Id

	// Save this TreeNode.
	btree.saveNode(node)
	clear(btree.tempSlots)

	return nil
}

func (node *Node[TK, TV]) find(btree *Btree[TK, TV], key TK, firstItemWithKey bool) (bool, error) {
	n := node
	foundItemIndex := 0
	foundNodeId := NilUUID
	var err error
	index := 0
	for {
		index = 0
		if n.Count > 0 {
			index = sort.Search(n.Count-1, func(index int) bool {
				return compare(n.Slots[index].Key, key) >= 0
			})
			// If key is found in node n.
			if index < btree.getSlotLength() {
				// Make the found node & item index the "current item" of btree.
				foundNodeId = n.Id
				foundItemIndex = index
				if !firstItemWithKey {
					break
				}
				if n.hasChildren() {
					// Try to navigate to the 1st item with key, in case there are duplicate keys.
					n, err = n.getChild(btree, index)
					if err != nil {
						return false, err
					}
					continue
				}
			} else {
				index--
			}
		}
		// Check children if there are.
		if n.hasChildren() {
			n, err = n.getChild(btree, index)
			if err != nil {
				return false, err
			}
			if n == nil {
				return false, nil
			}
		} else {
			break
		}
	}
	if !foundNodeId.IsNil() {
		btree.setCurrentItemId(foundNodeId, foundItemIndex)
		return true, nil
	}
	// This must be the outermost node
	// This block will make this item the current one to give chance to the Btree
	// caller the chance to check the items having the nearest key to the one it is interested at.
	if index == btree.getSlotLength() {
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
		_, err = n.moveToNext(btree)
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

func (node *Node[TK, TV]) moveToNext(btree *Btree[TK, TV]) (bool, error) {
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
				n, err = n.getChild(btree, slotIndex)
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
		// check if SlotIndex is within the maximum slot items and if it is, will index an occupied slot.
		if slotIndex < n.Count {
			btree.setCurrentItemId(n.Id, slotIndex)
			return true, nil
		}
		// check if this is not the root node. (Root nodes don't have parent node.)
		if n.ParentId != NilUUID {
			slotIndex, err = n.getIndexOfNode(btree)
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

// Returns true if a slot is available in left side siblings of this node modified to suit possible unbalanced branch.
func (node *Node[TK, TV]) isThereVacantSlotInLeft(btree *Btree[TK, TV], isUnBalanced *bool) (bool, error) {
	*isUnBalanced = false
	// Start from this node.
	temp := node
	for temp != nil {
		if temp.childrenIds != nil {
			*isUnBalanced = true
			return false, nil
		}
		if !temp.isFull(btree.getSlotLength()) {
			return true, nil
		}
		var err error
		temp, err = temp.getLeftSibling(btree)
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

// Returns true if a slot is available in right side siblings of this node modified to suit possible unbalanced branch.
func (node *Node[TK, TV]) isThereVacantSlotInRight(btree *Btree[TK, TV], isUnBalanced *bool) (bool, error) {
	*isUnBalanced = false
	// Start from this node.
	temp := node
	for temp != nil {
		if temp.childrenIds != nil {
			*isUnBalanced = true
			return false, nil
		}
		if !temp.isFull(btree.getSlotLength()) {
			return true, nil
		}
		var err error
		temp, err = temp.getRightSibling(btree)
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

// Returns left sibling or nil if finished traversing left side nodes.
func (node *Node[TK, TV]) getLeftSibling(btree *Btree[TK, TV]) (*Node[TK, TV], error) {
	index, err := node.getIndexOfNode(btree)
	if err != nil {
		return nil, err
	}
	p, err := node.getParent(btree)
	if err != nil {
		return nil, err
	}
	if p != nil {
		// If we are not at the leftmost sibling yet..
		if index > 0 && index <= p.Count {
			return p.getChild(btree, index-1)
		}
	}
	// Leftmost was already reached..
	return nil, nil
}

// Returns right sibling or nil if finished traversing right side nodes.
func (node *Node[TK, TV]) getRightSibling(btree *Btree[TK, TV]) (*Node[TK, TV], error) {
	index, err := node.getIndexOfNode(btree)
	if err != nil {
		return nil, err
	}
	p, err := node.getParent(btree)
	if err != nil {
		return nil, err
	}
	if p != nil {
		// If we are not at the rightmost sibling yet..
		if index < p.Count {
			return p.getChild(btree, index+1)
		}
	}
	// Rightmost was already reached..
	return nil, nil
}

// Returns index of this node relative to parent.
func (node *Node[TK, TV]) getIndexOfNode(btree *Btree[TK, TV]) (int, error) {
	parent, err := node.getParent(btree)
	if err != nil {
		return -1, err
	}
	if parent != nil {
		// Make sure we don't access an invalid node item.
		if parent.childrenIds != nil &&
			(node.indexOfNode == -1 || node.Id != parent.childrenIds[node.indexOfNode]) {
			for node.indexOfNode = 0; node.indexOfNode <= btree.getSlotLength() &&
				!parent.childrenIds[node.indexOfNode].IsNil(); node.indexOfNode++ {
				if parent.childrenIds[node.indexOfNode] == node.Id {
					break
				}
			}
		}
		return node.indexOfNode, nil
	}
	// Just return 0 if called in the root node, anyway,
	// the caller code should check if it is the root node and not call this function if it is!
	return 0, nil
}

func (node *Node[TK, TV]) getParent(btree *Btree[TK, TV]) (*Node[TK, TV], error) {
	if node.ParentId.IsNil() {
		return nil, nil
	}
	return btree.getNode(node.ParentId)
}

func (node *Node[TK, TV]) isFull(slotCount int) bool {
	return node.Count >= slotCount
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
		if i >= btree.getSlotLength() {
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
func (node *Node[TK, TV]) getChild(btree *Btree[TK, TV], childSlotIndex int) (*Node[TK, TV], error) {
	id := node.getChildId(childSlotIndex)
	if id == NilUUID {
		return nil, fmt.Errorf("Can't get child Node on child index position %d", childSlotIndex)
	}
	return btree.getNode(id)
}

func (node *Node[TK, TV]) getChildren(btree *Btree[TK, TV]) ([]*Node[TK, TV], error) {
	children := make([]*Node[TK, TV], len(node.childrenIds))
	var err error
	for i := range node.childrenIds {
		children[i], err = node.getChild(btree, i)
		if err != nil {
			return nil, err
		}
	}
	return children, nil
}

// hasChildren returns true if node has children or not.
func (node *Node[TK, TV]) hasChildren() bool {
	return node.childrenIds != nil
}

func (node *Node[TK, TV]) distributeToLeft(btree *Btree[TK, TV], item *Item[TK, TV]) error {
	if node.isFull(btree.getSlotLength()) {
		// counter-clockwise rotation..
		//	----
		//	|  |
		//	-> |
		// NOTE: we don't check for null returns as this method is called only when there is vacant in left
		parent, err := node.getParent(btree)
		if err != nil {
			return err
		}

		indexOfNode, err := node.getIndexOfNode(btree)
		if err != nil {
			return err
		}

		if indexOfNode > parent.Count {
			return nil
		}

		// Let controller to make another call to distribute item to left action.
		btree.distributeAction.sourceNode, err = node.getLeftSibling(btree)
		if err != nil {
			return err
		}

		btree.distributeAction.item = parent.Slots[indexOfNode-1]
		btree.distributeAction.distributeToLeft = true

		// Update Parent (remove node and add updated one).
		parent.Slots[indexOfNode-1] = node.Slots[0]
		btree.saveNode(parent)
		moveArrayElements(node.Slots, 1, 0, btree.getSlotLength()-1)
	} else {
		node.Count++
	}
	node.Slots[node.Count-1] = item
	btree.saveNode(node)
	return nil
}

func (node *Node[TK, TV]) distributeToRight(btree *Btree[TK, TV], item *Item[TK, TV]) error {
	if node.isFull(btree.getSlotLength()) {
		// clockwise rotation..
		//	----
		//	|  |
		//	| <-
		parent, err := node.getParent(btree)
		if err != nil {
			return nil
		}
		i, err := node.getIndexOfNode(btree)
		if err != nil {
			return nil
		}

		// Let controller to make another call to distribute item to right action.
		btree.distributeAction.sourceNode, err = node.getRightSibling(btree)
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
	moveArrayElements(node.Slots, 0, 1, btree.getSlotLength()-1)
	node.Slots[0] = item
	btree.saveNode(node)
	return nil
}

func (node *Node[TK, TV]) promote(btree *Btree[TK, TV], indexPosition int) error {
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
		node.childrenIds[index] = btree.tempParentChildren[0]

		// Insert the right child.
		shiftSlots(node.childrenIds, index+1, noOfOccupiedSlots+1)
		node.Count++
		node.childrenIds[index+1] = btree.tempParentChildren[1]
		btree.saveNode(node)

		// successful
		return nil
	}

	// Insert to temp slots.. node is full, use TempSlots
	// NOTE: ensure node & its children being promoted will point to the correct
	// new ParentAddress as recursive node breakup occurs...

	copyArrayElements(btree.tempSlots, node.Slots, btree.getSlotLength())
	shiftSlots(btree.tempSlots, index, btree.getSlotLength())
	btree.tempSlots[index] = btree.tempParent
	copyArrayElements(btree.tempChildren, node.childrenIds, btree.getSlotLength()+1)

	// Insert the left child.
	btree.tempChildren[index] = btree.tempParentChildren[0]
	// Insert the right child.
	shiftSlots(btree.tempChildren, index+1, noOfOccupiedSlots+1)
	btree.tempChildren[index+1] = btree.tempParentChildren[1]

	// Try to break up the node into 2 siblings.
	slotsHalf := btree.getSlotLength() >> 1
	if node.ParentId != NilUUID {

		// Prepare this and the right node sibling and promote the temporary parent node(pTempSlot).
		// This will be the left sibling !
		rightNode := newNode[TK, TV](btree.getSlotLength())
		rightNode.newId(node.ParentId)
		rightNode.childrenIds = make([]UUID, btree.getSlotLength()+1)

		// Zero out the current slot.
		clear(node.Slots)
		// Zero out this children node pointers.
		clear(node.childrenIds)

		// Copy the left half of the slots to left sibling(this)
		copyArrayElements(node.Slots, btree.tempSlots, slotsHalf)
		node.Count = slotsHalf

		// Copy the right half of the slots to right sibling
		copyArrayElements(rightNode.Slots, btree.tempSlots[:slotsHalf+1], slotsHalf)
		rightNode.Count = slotsHalf
		// Copy the left half of the children nodes.
		copyArrayElements(node.childrenIds, btree.tempChildren, slotsHalf+1)

		// Copy the right half of the children nodes.
		copyArrayElements(rightNode.childrenIds, btree.tempChildren[:slotsHalf+1], slotsHalf+1)

		// Left sibling is already parent of its children. make the right sibling parent of its children.
		rightNode.updateChildrenParent(btree)
		btree.saveNode(rightNode)

		// Copy the middle slot.
		btree.tempParent = btree.tempSlots[slotsHalf]
		// Assign the new children nodes.
		btree.tempParentChildren[0] = node.Id
		btree.tempParentChildren[1] = rightNode.Id

		btree.saveNode(node)

		// Trigger another promotion.
		var err error
		btree.promoteAction.nodeForPromotion, err = node.getParent(btree)
		if err != nil {
			return err
		}
		btree.promoteAction.nodeForPromotionIndex, err = node.getIndexOfNode(btree)
		if err != nil {
			return err
		}
		return nil
	}
	// No parent, break up this node into two children & make this new root.
	leftNode := newNode[TK, TV](btree.getSlotLength())
	leftNode.newId(node.Id)

	rightNode := newNode[TK, TV](btree.getSlotLength())
	rightNode.newId(node.Id)

	// Copy the left half of the slots
	copyArrayElements(leftNode.Slots, btree.tempSlots, slotsHalf)
	leftNode.Count = slotsHalf
	// Copy the right half of the slots
	copyArrayElements(rightNode.Slots, btree.tempSlots[:slotsHalf+1], slotsHalf)
	rightNode.Count = slotsHalf
	leftNode.childrenIds = make([]UUID, btree.getSlotLength()+1)
	rightNode.childrenIds = make([]UUID, btree.getSlotLength()+1)
	// Copy the left half of the children nodes.
	copyArrayElements(leftNode.childrenIds, btree.tempChildren, slotsHalf+1)
	// Copy the right half of the children nodes.
	copyArrayElements(rightNode.childrenIds, btree.tempChildren[:slotsHalf+1], slotsHalf+1)

	// Reset this Node.
	clear(node.Slots)
	clear(node.childrenIds)

	btree.saveNode(leftNode)
	// Make the left sibling parent of its children.
	leftNode.updateChildrenParent(btree)

	btree.saveNode(rightNode)
	// Make the right sibling parent of its children.
	rightNode.updateChildrenParent(btree)

	// Copy the middle slot
	node.Slots[0] = btree.tempSlots[slotsHalf]
	node.Count = 1

	// Assign the new children nodes.
	node.childrenIds[0] = leftNode.Id
	node.childrenIds[1] = rightNode.Id
	btree.saveNode(node)
	// successful
	return nil
}

func (node *Node[TK, TV]) newId(parentId UUID) {
	// Set the Physical Ids, transaction commit should handle resolving physical & logical Ids.
	node.Id = NewUUID()
	node.ParentId = parentId
}

func (node *Node[TK, TV]) getChildId(index int) UUID {
	if len(node.childrenIds) == 0 {
		return NilUUID
	}
	return node.childrenIds[index]
}

func (node *Node[TK, TV]) updateChildrenParent(btree *Btree[TK, TV]) error {
	if node.childrenIds != nil {
		children, err := node.getChildren(btree)
		if err != nil {
			return err
		}
		// Make the right sibling parent of its children.
		for index := 0; index < len(children) && children[index] != nil; index++ {
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
		moveArrayElements(array, position, position+1, noOfOccupiedSlots-position)
	}
}

// moveArrayElements is a helper function for internal use only.
func moveArrayElements[T any](array []T, srcStartIndex, destStartIndex, count int) {
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
