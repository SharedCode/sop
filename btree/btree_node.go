package btree

import (
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
	Id                 UUID
	ParentId           UUID
	ChildrenLogicalIds []UUID
	Slots              []*Item[TK, TV]
	Count              int
	Version            int
	IsDeleted          bool
	indexOfNode        int
	logicalId          UUID
	parentLogicalId    UUID
	childrenIds        []UUID
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
	if btree.isUnique() && currentNode.itemCount() > 0 {
		var currItemIndex = index
		if index > 0 && index >= currentNode.itemCount() {
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

func (node *Node[TK, TV]) saveNode(btree *Btree[TK, TV]) error {
	if node.Id.IsNil() {
		node.Id = NewUUID()
		return btree.StoreInterface.NodeRepository.Add(node)
	}
	return btree.StoreInterface.NodeRepository.Update(node)
}

func (node *Node[TK, TV]) find(btree *Btree[TK, TV], key TK, firstItemWithKey bool) (bool, error) {
	n := node
	foundItemIndex := 0
	foundNodeId := NilUUID
	var err error
	index := 0
	for {
		index = 0
		if n.itemCount() > 0 {
			index = sort.Search(n.itemCount()-1, func(index int) bool {
				return compare(n.Slots[index].Key, key) >= 0
			})
			// If key is found in node n.
			if index < btree.Store.NodeSlotCount {
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
	if index == btree.Store.NodeSlotCount {
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
	slotIndex := btree.currentItemRef.NodeItemIndex
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
		if slotIndex < n.itemCount() {
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

// Outermost(a.k.a. leaf) node, the end of the recursive traversing thru all inner nodes of the Btree.
// Correct Node is reached at this point!
func (node *Node[TK, TV]) addOnLeaf(btree *Btree[TK, TV], item *Item[TK, TV], index int, parent *Node[TK, TV]) (bool, error) {
	// If node is not yet full.
	if node.itemCount() < btree.Store.NodeSlotCount {
		// Insert the Item to target position & "skud" over the items to the right.
		node.insertSlotItem(item, index)
		// Save this TreeNode
		node.saveNode(btree)
		return true, nil
	}

	// Node is full, distribute or breakup the node (use temp slots in the process).
	copy(btree.tempSlots, node.Slots)

	// Index now contains the correct array element number to insert item into.
	// Skud over then assign the item to the vacatad slot.
	copy(btree.tempSlots[index+1:], btree.tempSlots[index:])
	// Set the item to the newly vacated slot.
	btree.tempSlots[index] = item

	// var slotsHalf = btree.Store.NodeSlotCount >> 1
	// var rightNode, leftNode *Node[TK, TV]
	var isVacantSlotInLeft, isVacantSlotInRight bool
	var err error

	if !node.ParentId.IsNil() {
		var isUnBalanced bool
		isVacantSlotInLeft, err = node.isThereVacantSlotInLeft(btree, &isUnBalanced)
		if err != nil {
			return isVacantSlotInLeft, err
		}
		isVacantSlotInRight, err = node.isThereVacantSlotInRight(btree, &isUnBalanced)
		if err != nil {
			return isVacantSlotInRight, err
		}
	}

	if isVacantSlotInLeft || isVacantSlotInRight {
		// Distribute to either left or right sibling the overflowed item.
		// Copy temp buffer contents to the actual slots.
		var b int16 = 1
		if isVacantSlotInLeft {
			b = 0
		}
		copy(btree.tempSlots[b:], node.Slots)
		// Save this node.
		btree.saveNode(node)

		btree.distributeAction.Source = node
		if isVacantSlotInLeft {
			btree.distributeAction.Item = btree.tempSlots[btree.Store.NodeSlotCount]
			clear(btree.tempSlots)

			// Vacant in left, create a distribution action request to B-Tree.
			// Logic is: "skud over" the leftmost node's item to parent and the item
			// on parent to left sibling node (recursively).
			btree.distributeAction.DistributeToLeft = true
			return true, nil
		}
		btree.distributeAction.Item = btree.tempSlots[0]
		clear(btree.tempSlots)
		// Vacant in right, move the rightmost node item into the vacant slot in right.
		btree.distributeAction.DistributeToLeft = false
		return true, nil
	}

	// TODO:
	// 	if (bIsUnBalanced)
	// 	{
	// 		// if this branch is unbalanced..
	// 		// _BreakNode
	// 		// Description :
	// 		// -copy the left half of the slots
	// 		// -copy the right half of the slots
	// 		// -zero out the current slot.
	// 		// -copy the middle slot
	// 		// -allocate memory for children node *s
	// 		// -assign the new children nodes.

	// 		// Initialize should throw an exception if in error.
	// 		rightNode = CreateNode(bTree, this.GetAddress(bTree));
	// 		leftNode = CreateNode(bTree, this.GetAddress(bTree));
	// 		CopyArrayElements(bTree.tempSlots, 0, leftNode.Slots, 0, slotsHalf);
	// 		leftNode.itemCount() = slotsHalf;
	// 		CopyArrayElements(bTree.tempSlots, (short) (slotsHalf + 1), rightNode.Slots, 0, slotsHalf);
	// 		rightNode.itemCount() = slotsHalf;
	// 		ResetArray(Slots, null);
	// 		Slots[0] = bTree.tempSlots[slotsHalf];
	// 		ChildrenAddresses = new long[bTree.SlotLength + 1];
	// 		ResetArray(ChildrenAddresses, -1);

	// 		//** save this TreeNode, Left & Right Nodes
	// 		leftNode.btree.SaveNode()
	// 		rightNode.btree.SaveNode()

	// 		ChildrenAddresses[(int) ChildNodes.LeftChild] = leftNode.GetAddress(bTree);
	// 		ChildrenAddresses[(int) ChildNodes.RightChild] = rightNode.GetAddress(bTree);
	// 		btree.SaveNode()
	// 		//**

	// 		ResetArray(bTree.tempSlots, null);
	// 		return;
	// 	}
	// 	// All slots are occupied in this and other siblings' nodes..

	// 	// prepare this and the right node sibling and promote the temporary parent node(pTempSlot).
	// 	rightNode = CreateNode(bTree, ParentAddress);
	// 	// zero out the current slot.
	// 	ResetArray(Slots, null);
	// 	RemoveFromBTreeBlocksCache(bTree, this);

	// 	// copy the left half of the slots to left sibling
	// 	CopyArrayElements(bTree.tempSlots, 0, Slots, 0, slotsHalf);
	// 	Count = slotsHalf;
	// 	// copy the right half of the slots to right sibling
	// 	CopyArrayElements(bTree.tempSlots, (short) (slotsHalf + 1), rightNode.Slots, 0, slotsHalf);
	// 	rightNode.itemCount() = slotsHalf;

	// 	// copy the middle slot to temp parent slot.
	// 	bTree.TempParent = bTree.tempSlots[slotsHalf];

	// 	//*** save this and Right Node
	// 	btree.SaveNode()
	// 	rightNode.btree.SaveNode()

	// 	// assign the new children nodes.
	// 	bTree.TempParentChildren[(int) ChildNodes.LeftChild] = this.GetAddress(bTree);
	// 	bTree.TempParentChildren[(int) ChildNodes.RightChild] = rightNode.GetAddress(bTree);

	// 	BTreeNodeOnDisk o = parent ?? GetParent(bTree);
	// 	if (o == null)
	// 		throw new SopException(string.Format("Can't get parent (Id='{0}') of this Node.", ParentAddress));

	// 	bTree.PromoteParent = o;
	// 	bTree.PromoteIndexOfNode = GetIndexOfNode(bTree);
	// 	return;
	// }
	// // _BreakNode
	// // Description :
	// // -copy the left half of the temp slots
	// // -copy the right half of the temp slots
	// // -zero out the current slot.
	// // -copy the middle of temp slot to 1st elem of current slot
	// // -allocate memory for children node *s
	// // -assign the new children nodes.
	// rightNode = CreateNode(bTree, GetAddress(bTree));
	// leftNode = CreateNode(bTree, GetAddress(bTree));
	// CopyArrayElements(bTree.tempSlots, 0, leftNode.Slots, 0, slotsHalf);
	// leftNode.itemCount() = slotsHalf;
	// CopyArrayElements(bTree.tempSlots, (short)(slotsHalf + 1), rightNode.Slots, 0, slotsHalf);
	// rightNode.itemCount() = slotsHalf;
	// ResetArray(Slots, null);
	// Slots[0] = bTree.tempSlots[slotsHalf];
	// RemoveFromBTreeBlocksCache(bTree, this);

	// Count = 1;

	// // save Left and Right Nodes
	// leftNode.btree.SaveNode()
	// rightNode.btree.SaveNode()

	// ChildrenAddresses = new long[bTree.SlotLength + 1];
	// ResetArray(ChildrenAddresses, -1);
	// ChildrenAddresses[(int)ChildNodes.LeftChild] = leftNode.GetAddress(bTree);
	// ChildrenAddresses[(int)ChildNodes.RightChild] = rightNode.GetAddress(bTree);

	// //*** save this TreeNode
	// btree.SaveNode()
	// ResetArray(bTree.tempSlots, null);

	return false, nil
}

// Returns true if a slot is available in left side siblings of this node modified to suit possible unbalanced branch.
func (node *Node[TK, TV]) isThereVacantSlotInLeft(btree *Btree[TK, TV], isUnBalanced *bool) (bool, error) {
	*isUnBalanced = false
	// start from this node.
	temp := node
	for temp != nil {
		if temp.childrenIds != nil {
			*isUnBalanced = true
			return false, nil
		}
		if !temp.isFull(btree.Store.NodeSlotCount) {
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

// / <summary>
// / Returns true if a slot is available in right side siblings of this node modified to suit possible unbalanced branch.
// / </summary>
// / <param name="bTree">Parent BTree</param>
// / <param name="isUnBalanced">Will be updated to true if this branch is detected to be "unbalanced", else false</param>
// / <returns>true if there is a vacant slot, else false</returns>
func (node *Node[TK, TV]) isThereVacantSlotInRight(btree *Btree[TK, TV], isUnBalanced *bool) (bool, error) {
	*isUnBalanced = false
	// start from this node.
	temp := node
	for temp != nil {
		if temp.childrenIds != nil {
			*isUnBalanced = true
			return false, nil
		}
		if !temp.isFull(btree.Store.NodeSlotCount) {
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
		if index > 0 && index <= p.itemCount() {
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
		if index < p.itemCount() {
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
			for node.indexOfNode = 0; node.indexOfNode <= btree.Store.NodeSlotCount &&
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
	return node.itemCount() >= slotCount
}

func (node *Node[TK, TV]) insertSlotItem(item *Item[TK, TV], position int) {
	copy(node.Slots[position+1:], node.Slots[position:])
	node.Slots[position] = item
	node.Count++
}

func (node *Node[TK, TV]) getIndexToInsertTo(btree *Btree[TK, TV], item *Item[TK, TV]) (int, bool) {
	if node.itemCount() == 0 {
		// empty node.
		return 0, false
	}
	index := sort.Search(node.itemCount(), func(index int) bool {
		return compare(node.Slots[index].Key, item.Key) >= 0
	})
	if btree.isUnique() {
		i := index
		if i >= btree.Store.NodeSlotCount {
			i--
		}
		// Returns index in slot that is available for insert to.
		// Also returns true if an existing item with such key is found.
		return index, compare(node.Slots[i].Key, item.Key) == 0
	}
	// Returns index in slot that is available for insert to.
	return index, false
}

// TODO: Resolve story of fetching Nodes via logical Id vs. physical Id. Example, in a transaction,
// like when adding an item, newly created nodes need to be using UUID that then becomes logical Id
// during commit. When working with Children logical Ids(saved in backend!), we need to convert logical to physical Id.
func (node *Node[TK, TV]) getChild(btree *Btree[TK, TV], childSlotIndex int) (*Node[TK, TV], error) {
	h, err := btree.StoreInterface.VirtualIdRepository.Get(node.ChildrenLogicalIds[childSlotIndex])
	if err != nil {
		return nil, err
	}
	return btree.getNode(h.GetActiveId())
}

// hasChildren returns true if node has children or not.
func (node *Node[TK, TV]) hasChildren() bool {
	return node.childrenIds != nil || node.ChildrenLogicalIds != nil
}

func (node *Node[TK, TV]) itemCount() int {
	return node.Count
}

func (node *Node[TK, TV]) distributeToLeft(btree *Btree[TK, TV], item *Item[TK, TV]) error {
	return nil
}

func (node *Node[TK, TV]) distributeToRight(btree *Btree[TK, TV], item *Item[TK, TV]) error {
	return nil
}
