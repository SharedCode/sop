package btree

import "sort"

func (node *Node) add(btree *Btree, item Item) (bool, error) {
	var currentNode = node;
	var index int
	var parent *Node
	for {
		var itemExists bool
		var err error
		index, itemExists, err = currentNode.getIndex(btree, item)
		if err != nil {
			return false, err
		}
		if itemExists {
			// set the Current item pointer to the discovered item then return fail.
			btree.setCurrentItemAddress(currentNode.getAddress(btree), index);
			return false, nil;
		}
		if (currentNode.ChildrenAddresses != nil){
			parent = nil
			// if not an outermost node let next lower level node do the 'Add'.
			currentNode, err = currentNode.getChild(btree, index);
			if (err != nil || currentNode == nil){
				return false, err;
			}
		} else {
			break
		}
	}
	if (btree.isUnique() && currentNode.Count > 0) {
		var currItemIndex = index;
		if index > 0 && index >= currentNode.Count{
			currItemIndex--
		}
		i,e := compare(btree, currentNode.Slots[currItemIndex], item)
		if e != nil {return false, e}
		if (i == 0) {
			// set the Current item pointer to the discovered existing item.
			btree.setCurrentItemAddress(currentNode.getAddress(btree), currItemIndex);
			return false, nil;
		}
	}
	currentNode.addOnLeaf(btree, item, index, parent);
	return true, nil;
}

func (node *Node) saveNode(btree *Btree) error{
	if node.ID.IsEmpty() {
		node.ID = btree.StoreInterface.VirtualIDRepository.NewUUID().ToHandle()
		return btree.StoreInterface.NodeRepository.Add(node)
	}
	return btree.StoreInterface.NodeRepository.Update(node)
}

func (node *Node) addOnLeaf(btree *Btree, item Item, index int, parent *Node) (bool, error) {
	// outermost(a.k.a. leaf) node, the end of the recursive traversing 
	// thru all inner nodes of the Btree.. 
	// Correct Node is reached at this point!
	// if node is not yet full..
	if (node.Count < btree.Store.NodeSlotCount){
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

	if (!node.parentAddress.IsEmpty()){
		var isUnBalanced bool
		isVacantSlotInLeft,err := node.isThereVacantSlotInLeft(btree, &isUnBalanced)
		if err != nil {return isVacantSlotInLeft, err}
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
			throw new SopException(string.Format("Can't get parent (ID='{0}') of this Node.", ParentAddress));

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
func (node *Node) isThereVacantSlotInLeft(btree *Btree, isUnBalanced *bool) (bool, error) {
	*isUnBalanced = false
	// start from this node.
	temp := node
	for (temp != nil){		
		if (temp.ChildrenAddresses != nil){
			*isUnBalanced = true;
			return false, nil
		}
		if (!temp.isFull(btree.Store.NodeSlotCount)){
			return true, nil
		}
		var err error
		temp,err = temp.getLeftSibling(btree)
		if err != nil {return false, err}
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
func (node *Node) getLeftSibling(btree *Btree) (*Node, error){
	index, err := node.getIndexOfNode(btree)
	if err != nil {return nil, err}
	p, err := node.getParent(btree)
	if err != nil {return nil, err}
	// if we are not at the leftmost sibling yet..
	if (index > 0 && p != nil && index <= p.Count){
		return p.getChild(btree, index - 1)
	}
	// leftmost was already reached..
	return nil, nil;
}


// Returns index of this node relative to parent. 
func (node *Node) getIndexOfNode(btree *Btree) (int,error) {
	parent, err := node.getParent(btree);
	if err != nil {return -1,err}
	if (parent != nil){
		thisID := node.getAddress(btree)
		// Make sure we don't access an invalid memory address
		if (parent.ChildrenAddresses != nil &&
			(node.indexOfNode == -1 || thisID.LogicalID != parent.ChildrenAddresses[node.indexOfNode])){
			for node.indexOfNode = 0;
				node.indexOfNode <= btree.Store.NodeSlotCount && !parent.ChildrenAddresses[node.indexOfNode].IsNil();
				node.indexOfNode++ {
				if (parent.ChildrenAddresses[node.indexOfNode] == thisID.LogicalID){
					break;
				}
			}
		}
		return node.indexOfNode, nil;
	}
	// Just return 0 if called in the root node, anyway, 
	// the caller code should check if it is the root node and not call this function if it is!
	return 0, nil;
}

func (node *Node) getParent(btree *Btree) (*Node, error){
	if node.parentAddress.IsEmpty() {
		return nil, nil
	}
	return btree.getNode(node.parentAddress);
}

func (node *Node) isFull(slotCount int) bool{
	return node.Count >= slotCount
}

func (node *Node) insertSlotItem(item Item, position int){
	copy(node.Slots[position+1:], node.Slots[position:])
	node.Slots[position] = item
	node.Count++
}

func compare(btree *Btree, a Item, b Item) (int, error) {
	if a.IsEmpty() && b.IsEmpty() {return 0, nil}
	if a.IsEmpty() {return -1, nil}
	if b.IsEmpty() {return 1, nil}
	return btree.Store.ItemSerializer.CompareKey(a.Key, b.Key)
}

func (node *Node) getIndex(btree *Btree, item Item) (int, bool, error) {
	if node.Count == 0 {
		// empty node.
		return 0, false, nil
	}
	var itemFound = false
	var index int
	if node.Count > 1 {
		var err error
		index = sort.Search(node.Count, func(index int) bool{
			var r int
			r,err = btree.Store.ItemSerializer.CompareKey(node.Slots[index].Key, item.Key)
			if err != nil{
				return true
			}
			if r == 0{itemFound = true}
			return r >= 0
		})
		if err != nil{
			return 0, false, err
		}
		return index, itemFound, nil
	}
	// node count == 1
	result,err := btree.Store.ItemSerializer.CompareKey(node.Slots[0].Key, item.Key)
	if err != nil {
		return 0, false, err
	}
	if (result < 0){
		index = 1;
	} else if (btree.isUnique() && result == 0) {
		return 0, true, nil
	}
	return index, false, nil
}

func (node *Node) getChild(btree *Btree, childSlotIndex int) (*Node, error) {
	return btree.getNode(node.ChildrenAddresses[childSlotIndex].ToHandle())
}

func (node *Node) getAddress(btree *Btree) Handle {
	return node.ID
}
