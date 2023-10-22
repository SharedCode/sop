package btree

import (
	"sort"
)

// Item contains key & value pair, plus the version number.
type Item[TKey Comparable, TValue any] struct {
	Key             TKey
	Value           TValue
	ValueId         UUID
	Version         int
	valueNeedsFetch bool
}

// Node contains a B-Tree node's data.
type Node[TKey Comparable, TValue any] struct {
	Id                 UUID
	ParentId           UUID
	ChildrenLogicalIds []UUID
	Slots              []*Item[TKey, TValue]
	// Count of Items stored in Slots array.
	Count       int
	Version     int
	IsDeleted   bool
	indexOfNode int
	lid         UUID
}

func NewNode[TKey Comparable, TValue any](slotCount int) *Node[TKey, TValue] {
	return &Node[TKey, TValue]{
		Slots:       make([]*Item[TKey, TValue], slotCount),
		indexOfNode: -1,
	}
}

func (node *Node[TKey, TValue]) add(btree *Btree[TKey, TValue], item *Item[TKey, TValue]) (bool, error) {
	var currentNode = node
	var index int
	var parent *Node[TKey, TValue]
	for {
		var itemExists bool
		index, itemExists = currentNode.getIndexToInsertTo(btree, item)
		if itemExists {
			// set the Current item pointer to the duplicate item.
			btree.setCurrentItemAddress(currentNode.Id, index)
			return false, nil
		}
		if currentNode.ChildrenLogicalIds != nil {
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
			btree.setCurrentItemAddress(currentNode.getAddress(btree), currItemIndex)
			return false, nil
		}
	}
	currentNode.addOnLeaf(btree, item, index, parent)
	return true, nil
}

func (node *Node[TKey, TValue]) saveNode(btree *Btree[TKey, TValue]) error {
	if node.Id.IsNil() {
		node.Id = NewUUID()
		return btree.StoreInterface.NodeRepository.Add(node)
	}
	return btree.StoreInterface.NodeRepository.Update(node)
}

func (node *Node[TKey, TValue]) addOnLeaf(btree *Btree[TKey, TValue], item *Item[TKey, TValue], index int, parent *Node[TKey, TValue]) (bool, error) {
	// outermost(a.k.a. leaf) node, the end of the recursive traversing
	// thru all inner nodes of the Btree..
	// Correct Node is reached at this point!
	// if node is not yet full..
	if node.Count < btree.Store.NodeSlotCount {
		// insert the Item to target position & "skud" over the items to the right
		node.insertSlotItem(item, index)
		// save this TreeNode
		node.saveNode(btree)
		return true, nil
	}

	// node is full, distribute or breakup the node (use temp slots in the process)...
	copy(btree.TempSlots, node.Slots)

	// Index now contains the correct array element number to insert item into.
	copy(btree.TempSlots[index+1:], btree.TempSlots[index:])
	btree.TempSlots[index] = item

	// work in progress marker...

	// var slotsHalf = btree.Store.NodeSlotCount >> 1
	// var rightNode, leftNode Node

	if !node.ParentId.IsNil() {
		var isUnBalanced bool
		isVacantSlotInLeft, err := node.isThereVacantSlotInLeft(btree, &isUnBalanced)
		if err != nil {
			return isVacantSlotInLeft, err
		}
		//isVacantSlotInRight = node.isThereVacantSlotInRight(btree, &isUnBalanced)

	}
	/*
			if (iIsThereVacantSlot > 0)
			{
				//** distribute to either left or right sibling the overflowed item...
				// copy temp buffer contents to the actual slots.
				short b = (short) (iIsThereVacantSlot == 1 ? 0 : 1);
				CopyArrayElements(bTree.TempSlots, b, Slots, 0, bTree.SlotLength);

				//*** save this TreeNode
				SaveNodeToDisk(bTree);

				BTreeItemOnDisk biod;
				if (iIsThereVacantSlot == 1)
				{
					biod = bTree.TempSlots[bTree.SlotLength];
					ResetArray(bTree.TempSlots, null, bTree.TempSlots.Length);

					// Vacant in left, "skud over" the leftmost node's item to parent and the item
					// on parent to left sibling node (recursively).
					bTree.DistributeSibling = this;
					bTree.DistributeItem = biod;
					bTree.DistributeLeftDirection = true;
					//DistributeToLeft(bTree, biod);

				}
				else if (iIsThereVacantSlot == 2)
				{
					biod = bTree.TempSlots[0];
					ResetArray(bTree.TempSlots, null);
					// Vacant in right, move the rightmost node item into the vacant slot in right.

					bTree.DistributeSibling = this;
					bTree.DistributeItem = biod;
					bTree.DistributeLeftDirection = false;
					//DistributeToRight(bTree, biod);
				}
				return;
			}
			if (bIsUnBalanced)
			{
				// if this branch is unbalanced..
				// _BreakNode
				// Description :
				// -copy the left half of the slots
				// -copy the right half of the slots
				// -zero out the current slot.
				// -copy the middle slot
				// -allocate memory for children node *s
				// -assign the new children nodes.

				// Initialize should throw an exception if in error.
				rightNode = CreateNode(bTree, this.GetAddress(bTree));
				leftNode = CreateNode(bTree, this.GetAddress(bTree));
				CopyArrayElements(bTree.TempSlots, 0, leftNode.Slots, 0, slotsHalf);
				leftNode.Count = slotsHalf;
				CopyArrayElements(bTree.TempSlots, (short) (slotsHalf + 1), rightNode.Slots, 0, slotsHalf);
				rightNode.Count = slotsHalf;
				ResetArray(Slots, null);
				Slots[0] = bTree.TempSlots[slotsHalf];
				ChildrenAddresses = new long[bTree.SlotLength + 1];
				ResetArray(ChildrenAddresses, -1);

				//** save this TreeNode, Left & Right Nodes
				leftNode.SaveNodeToDisk(bTree);
				rightNode.SaveNodeToDisk(bTree);

				ChildrenAddresses[(int) ChildNodes.LeftChild] = leftNode.GetAddress(bTree);
				ChildrenAddresses[(int) ChildNodes.RightChild] = rightNode.GetAddress(bTree);
				SaveNodeToDisk(bTree);
				//**

				ResetArray(bTree.TempSlots, null);
				return;
			}
			// All slots are occupied in this and other siblings' nodes..

			// prepare this and the right node sibling and promote the temporary parent node(pTempSlot).
			rightNode = CreateNode(bTree, ParentAddress);
			// zero out the current slot.
			ResetArray(Slots, null);
			RemoveFromBTreeBlocksCache(bTree, this);

			// copy the left half of the slots to left sibling
			CopyArrayElements(bTree.TempSlots, 0, Slots, 0, slotsHalf);
			Count = slotsHalf;
			// copy the right half of the slots to right sibling
			CopyArrayElements(bTree.TempSlots, (short) (slotsHalf + 1), rightNode.Slots, 0, slotsHalf);
			rightNode.Count = slotsHalf;

			// copy the middle slot to temp parent slot.
			bTree.TempParent = bTree.TempSlots[slotsHalf];

			//*** save this and Right Node
			SaveNodeToDisk(bTree);
			rightNode.SaveNodeToDisk(bTree);

			// assign the new children nodes.
			bTree.TempParentChildren[(int) ChildNodes.LeftChild] = this.GetAddress(bTree);
			bTree.TempParentChildren[(int) ChildNodes.RightChild] = rightNode.GetAddress(bTree);

			BTreeNodeOnDisk o = parent ?? GetParent(bTree);
			if (o == null)
				throw new SopException(string.Format("Can't get parent (Id='{0}') of this Node.", ParentAddress));

			bTree.PromoteParent = o;
			bTree.PromoteIndexOfNode = GetIndexOfNode(bTree);
			return;
		}
		// _BreakNode
		// Description :
		// -copy the left half of the temp slots
		// -copy the right half of the temp slots
		// -zero out the current slot.
		// -copy the middle of temp slot to 1st elem of current slot
		// -allocate memory for children node *s
		// -assign the new children nodes.
		rightNode = CreateNode(bTree, GetAddress(bTree));
		leftNode = CreateNode(bTree, GetAddress(bTree));
		CopyArrayElements(bTree.TempSlots, 0, leftNode.Slots, 0, slotsHalf);
		leftNode.Count = slotsHalf;
		CopyArrayElements(bTree.TempSlots, (short)(slotsHalf + 1), rightNode.Slots, 0, slotsHalf);
		rightNode.Count = slotsHalf;
		ResetArray(Slots, null);
		Slots[0] = bTree.TempSlots[slotsHalf];
		RemoveFromBTreeBlocksCache(bTree, this);

		Count = 1;

		// save Left and Right Nodes
		leftNode.SaveNodeToDisk(bTree);
		rightNode.SaveNodeToDisk(bTree);

		ChildrenAddresses = new long[bTree.SlotLength + 1];
		ResetArray(ChildrenAddresses, -1);
		ChildrenAddresses[(int)ChildNodes.LeftChild] = leftNode.GetAddress(bTree);
		ChildrenAddresses[(int)ChildNodes.RightChild] = rightNode.GetAddress(bTree);

		//*** save this TreeNode
		SaveNodeToDisk(bTree);
		ResetArray(bTree.TempSlots, null);
	*/

	return false, nil
}

// Returns true if a slot is available in left side siblings of this node modified to suit possible unbalanced branch.
func (node *Node[TKey, TValue]) isThereVacantSlotInLeft(btree *Btree[TKey, TValue], isUnBalanced *bool) (bool, error) {
	*isUnBalanced = false
	// start from this node.
	temp := node
	for temp != nil {
		if temp.ChildrenLogicalIds != nil {
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

/*
   /// <summary>
   /// Returns true if a slot is available in right side siblings of this node modified to suit possible unbalanced branch.
   /// </summary>
   /// <param name="bTree">Parent BTree</param>
   /// <param name="isUnBalanced">Will be updated to true if this branch is detected to be "unbalanced", else false</param>
   /// <returns>true if there is a vacant slot, else false</returns>
   private bool IsThereVacantSlotInRight(BTree.BTreeAlgorithm bTree, ref bool isUnBalanced)
   {
       isUnBalanced = false;
       // start from this node.
       BTreeNodeOnDisk temp = this;
       while ((temp = temp.GetRightSibling(bTree)) != null)
       {
           if (temp.ChildrenAddresses != null)
           {
               isUnBalanced = true;
               return false;
           }
           if (!temp.IsFull(bTree.SlotLength))
               return true;
       }
       return false;
   }
*/

// todo:
// Returns left sibling or nil if finished traversing left nodes.
func (node *Node[TKey, TValue]) getLeftSibling(btree *Btree[TKey, TValue]) (*Node[TKey, TValue], error) {
	index, err := node.getIndexOfNode(btree)
	if err != nil {
		return nil, err
	}
	p, err := node.getParent(btree)
	if err != nil {
		return nil, err
	}
	// if we are not at the leftmost sibling yet..
	if index > 0 && p != nil && index <= p.Count {
		return p.getChild(btree, index-1)
	}
	// leftmost was already reached..
	return nil, nil
}

// Returns index of this node relative to parent.
func (node *Node[TKey, TValue]) getIndexOfNode(btree *Btree[TKey, TValue]) (int, error) {
	parent, err := node.getParent(btree)
	if err != nil {
		return -1, err
	}
	if parent != nil {
		thisId := node.getId(btree)
		// Make sure we don't access an invalid memory address
		if parent.ChildrenLogicalIds != nil &&
			(node.indexOfNode == -1 || thisId.LogicalId != parent.ChildrenAddresses[node.indexOfNode]) {
			for node.indexOfNode = 0; node.indexOfNode <= btree.Store.NodeSlotCount && !parent.ChildrenAddresses[node.indexOfNode].IsNil(); node.indexOfNode++ {
				if parent.ChildrenLogicalIds[node.indexOfNode] == thisId.LogicalId {
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

func (node *Node[TKey, TValue]) getParent(btree *Btree[TKey, TValue]) (*Node[TKey, TValue], error) {
	if node.ParentId.IsEmpty() {
		return nil, nil
	}
	return btree.getNode(node.ParentId)
}

func (node *Node[TKey, TValue]) isFull(slotCount int) bool {
	return node.Count >= slotCount
}

func (node *Node[TKey, TValue]) insertSlotItem(item *Item[TKey, TValue], position int) {
	copy(node.Slots[position+1:], node.Slots[position:])
	node.Slots[position] = item
	node.Count++
}

func (node *Node[TKey, TValue]) getIndexToInsertTo(btree *Btree[TKey, TValue], item *Item[TKey, TValue]) (int, bool) {
	if node.Count == 0 {
		// empty node.
		return 0, false
	}
	index := sort.Search(node.Count, func(index int) bool {
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
func (node *Node[TKey, TValue]) getChild(btree *Btree[TKey, TValue], childSlotIndex int) (*Node[TKey, TValue], error) {
	h, err := btree.StoreInterface.VirtualIdRepository.Get(node.ChildrenLogicalIds[childSlotIndex])
	if err != nil {
		return nil, err
	}
	return btree.getNode(h.GetActiveId())
}
