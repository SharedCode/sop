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
	if (btree.isUnique() && currentNode.count > 0) {
		var currItemIndex = index;
		if index > 0 && index >= currentNode.count{
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

func (node *Node) addOnLeaf(btree *Btree, item Item, index int, parent *Node) (bool, error) {
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
