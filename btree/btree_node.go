package btree

//import "sort"

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
		if (currentNode.Children != nil){
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
		if (compare(btree, currentNode.Slots[currItemIndex], item) == 0) {
			// set the Current item pointer to the discovered existing item.
			btree.setCurrentItemAddress(currentNode.getAddress(btree), currItemIndex);
			return false, nil;
		}
	}
	currentNode.addOnLeaf(btree, item, index, parent);
	return true, nil;
}

// todo:

func (node *Node) saveNodeToDisk(btree *Btree){
	//btree.StoreInterface.NodeRepository.Add(node)
}

func (node *Node) addOnLeaf(btree *Btree, item Item, index int, parent *Node) (bool, error) {
	// outermost(a.k.a. leaf) node, the end of the recursive traversing 
	// thru all inner nodes of the Btree.. 
	// Correct Node is reached at this point!
	// if node is not yet full..
	if (node.Count < btree.Store.NodeSlotCount){
		// insert the Item to target position & "skud" over the items to the right
		node.insertSlotItem(item, index)
		node.Count++;
		// save this TreeNode and HeaderData
		node.saveNodeToDisk(btree);
		return true, nil;
	}

	// node is full, distribute or breakup the node (use temp slots in the process)...
	copy(btree.TempSlots, node.Slots);

	// Index now contains the correct array element number to insert item into.
	copy(btree.TempSlots[index+1:], btree.TempSlots[index:])
	btree.TempSlots[index] = item;


/*	
	var slotsHalf = (short) (bTree.SlotLength >> 1);
	BTreeNodeOnDisk rightNode;
	BTreeNodeOnDisk leftNode;
	if (ParentAddress != -1)
	{
		bool bIsUnBalanced = false;
		int iIsThereVacantSlot = 0;
		if (IsThereVacantSlotInLeft(bTree, ref bIsUnBalanced))
			iIsThereVacantSlot = 1;
		else if (IsThereVacantSlotInRight(bTree, ref bIsUnBalanced))
			iIsThereVacantSlot = 2;
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

func compare(btree *Btree, a Item, b Item) int {
	return 0
	// if (a == null && b == null) return 0;
	// if (a == null) return -1;
	// if (b == null) return 1;

	// if (btree.Comparer != null)
	// {
	// 	return btree.ComparerWrapper.Compare(a, b);
	// }
	// else
	// {
	// 	btree.Comparer = new SystemDefaultComparer();
	// 	try
	// 	{
	// 		return btree.ComparerWrapper.Compare(a, b);
	// 	}
	// 	catch (Exception)
	// 	{
	// 		btree.Comparer = new BTree.BTreeDefaultComparer();
	// 		return btree.ComparerWrapper.Compare(a, b);
	// 	}
	// }
}

func (node *Node) getIndex(btree *Btree, item Item) (int, bool, error) {
	return -1, false, nil
	// var dupeDetected = false
	// var index int
	// if (node.count > 1) {
	// 	if (bTree.Comparer != null){
	// 		index = sort.Search(node.count, )
	// 	} else {
	// 		try
	// 		{
	// 			index = (short) Array.BinarySearch(Slots, 0, Count, item);
	// 		}
	// 		catch
	// 		{
	// 			try
	// 			{
	// 				index = (short) Array.BinarySearch(Slots, item);
	// 			}
	// 			catch //(Exception innerE)
	// 			{
	// 				throw new InvalidOperationException("No Comparer Error.");
	// 			}
	// 		}
	// 	}
	// 	if (index < 0)
	// 		index = (short)~index;
	// 	if (bTree.IsUnique && index >= 0)
	// 	{
	// 		short i = index;
	// 		if (i >= Slots.Length)
	// 			i--;
	// 		var result = Compare(bTree, Slots[i], item);
	// 		if (result == 0)
	// 		{
	// 			dupeDetected = true;
	// 			return i;
	// 		}
	// 	}
	// } else if (Count == 1){
	// 	var result = Compare(bTree, Slots[0], item);
	// 	if (result < 0){
	// 		index = 1;
	// 	} else if (bTree.IsUnique && result == 0) {
	// 		dupeDetected = true;
	// 		return 0;
	// 	}
	// }
	// return index;
}

func (node *Node) getChild(btree *Btree, index int) (*Node, error) {
	var n *Node
	return n, nil
}

func (node *Node) getAddress(btree *Btree) UUID {
	var r UUID
	return r
}
