using System;
using System.Collections;
using System.ComponentModel;

namespace Sop.Collections.Generic
{
    namespace BTree
    {
        internal partial class BTreeAlgorithm<TKey, TValue>
        {
            internal bool FixVacatedSlot;
            private TreeNode _promoteParent;
            private short _promoteIndexOfNode;

            internal void ProcessDistribution()
            {
                while (DistributeSibling != null)
                {
                    TreeNode n = DistributeSibling;
                    BTreeItem<TKey, TValue> item = DistributeItem;
                    DistributeSibling = null;
                    DistributeItem = null;
                    if (DistributeLeftDirection)
                        n.DistributeToLeft(this, item);
                    else
                        n.DistributeToRight(this, item);
                }
            }
            internal bool DistributeLeftDirection;
            internal TreeNode DistributeSibling;
            internal BTreeItem<TKey, TValue> DistributeItem;

            internal bool PullLeftDirection;
            internal TreeNode PullSibling;


            /// <summary>
            /// TreeNode is where the actual B-Tree operation happens. Each object of TreeNode serves
            /// as the node of B-Tree tree
            /// </summary>
            internal class TreeNode
            {
                /// <summary>
                /// A BTreeAlgorithm's item address is composed of the node's address + the item's index in the Slots.
                /// </summary>
                internal struct ItemAddress
                {
                    /// <summary>
                    /// Node Reference (low-level is equivalent to Node Address)
                    /// </summary>
                    public TreeNode Node;

                    /// <summary>
                    /// Index of the item in the Node's Slots
                    /// </summary>
                    public byte NodeItemIndex;
                }

                /// <summary>
                /// Protected Constructor. This is used to construct the root node of the tree.
                /// (has null parent)
                /// </summary>
                /// <param name="oBTree"></param>
                protected TreeNode(BTreeAlgorithm<TKey, TValue> oBTree)
                {
                    this.Initialize(oBTree, null);
                }

                /// <summary>
                /// Constructor expecting ParentTree and ParentNode params.
                ///	This form is invoked from another instance of this class when node 
                ///	splitting occurs. Normally, node split occurs to accomodate new items that
                ///	could not be loaded to the node since the node is already full. 
                ///	Calls <see cref="Initialize"/> to prepare class variables/objects
                /// </summary>
                /// <param name="parentTree">Parent B-Tree instance</param>
                /// <param name="parentNode">Parent Node instance</param>
                protected internal TreeNode(BTreeAlgorithm<TKey, TValue> parentTree, TreeNode parentNode)
                {
                    Initialize(parentTree, parentNode);
                }

                /// <summary>
                /// Reset all elements of the array to Value
                /// </summary>
                /// <param name="array">Array to reset all elements of</param>
                /// <param name="value">Value to assign to each element of the array</param>
                protected internal static void ResetArray<T>(T[] array, T value)
                {
                    for (ushort i = 0; i < array.Length; i++)
                        array[i] = value;
                }

                /// <summary>
                /// Do class variable/object initialization. Usually invoked from this class' constructor.
                /// </summary>
                /// <param name="btree">Parent BTree</param>
                /// <param name="parentObj">Parent Node</param>
                protected internal void Initialize(BTreeAlgorithm<TKey, TValue> btree, TreeNode parentObj)
                {
                    if (Slots == null)
                        Slots = new BTreeItem<TKey, TValue>[btree.SlotLength];
                    Children = null;
                    Parent = parentObj;
                }

                // Utility methods...
                /// <summary>
                /// "Shallow" move elements of an array. 
                /// "MoveArrayElements" moves a group (Count) of elements of an array from
                /// source index to destination index.
                /// </summary>
                /// <param name="array">Array whose elements will be moved</param>
                /// <param name="srcIndex">Source index of the 1st element to move</param>
                /// <param name="destIndex">Target index of the 1st element to move to</param>
                /// <param name="count">Number of elements to move</param>
                private static void MoveArrayElements<T>(T[] array, ushort srcIndex, ushort destIndex, ushort count)
                {
                    try
                    {
                        sbyte addValue = -1;
                        uint srcStartIndex = (uint) srcIndex + count - 1, destStartIndex = (uint) destIndex + count - 1;
                        if (destIndex < srcIndex)
                        {
                            srcStartIndex = srcIndex;
                            destStartIndex = destIndex;
                            addValue = 1;
                        }
                        for (int i = 0; i < count; i++)
                        {
                            if (destStartIndex >= array.Length) continue;
                            array[destStartIndex] = array[srcStartIndex];
                            destStartIndex = (uint) (destStartIndex + addValue);
                            srcStartIndex = (uint) (srcStartIndex + addValue);
                        }
                    }
                    catch (Exception)
                    {
                        // don't do anything during exception
                    }
                }

                /// <summary>
                /// "CopyArrayElements" copies elements of an array (Source) to destination array (Destination).
                /// </summary>
                /// <param name="source">Array to copy elements from</param>
                /// <param name="srcIndex">Index of the 1st element to copy</param>
                /// <param name="destination">Array to copy elements to</param>
                /// <param name="destIndex">Index of the 1st element to copy to</param>
                /// <param name="count">Number of elements to copy</param>
                private static void CopyArrayElements<T>(T[] source, ushort srcIndex, T[] destination, ushort destIndex,
                                                         ushort count)
                {
                    try
                    {
                        for (ushort i = 0; i < count; i++)
                            destination[destIndex + i] = source[srcIndex + i];
                    }
                    catch (Exception)
                    {
                        // don't do anything during exception
                    }
                }

                /// <summary>
                /// Skud over one slot all items to the right.
                /// The 1st element moved will then be vacated ready for an occupant.
                /// </summary>
                /// <param name="slots">"Slots" to skud over its contents</param>
                /// <param name="position">1st element index to skud over</param>
                /// <param name="noOfOccupiedSlots">Number of occupied slots</param>
                private static void ShiftSlots<T>(T[] slots, byte position, byte noOfOccupiedSlots)
                {
                    if (position < noOfOccupiedSlots)
                        // create a vacant slot by shifting node contents one slot
                        MoveArrayElements(slots, position, (ushort) (position + 1),
                                          (ushort) (noOfOccupiedSlots - position));
                }

                /// <summary>
                /// Recursive Add function. Actual addition of node item happens at the outermost level !
                /// </summary>
                /// <param name="parentBTree">Parent BTree</param>
                /// <param name="item">Item to add to the tree</param>
                /// <throws>Exception if No Comparer or Mem Alloc err is encountered.</throws>
                protected internal void Add(BTreeAlgorithm<TKey, TValue> parentBTree,
                                            BTreeItem<TKey, TValue> item)
                {
                    TreeNode currentNode = this;
                    int index = 0;
                    while (true)
                    {
                        index = 0;
                        byte noOfOccupiedSlots = currentNode._count;
                        if (noOfOccupiedSlots > 1)
                        {
                            if (parentBTree.SlotsComparer != null)
                                index = Array.BinarySearch(currentNode.Slots, 0, noOfOccupiedSlots, item,
                                                           parentBTree.SlotsComparer);
                            else
                                index = Array.BinarySearch(currentNode.Slots, 0, noOfOccupiedSlots, item);
                            if (index < 0)
                                index = ~index;
                        }
                        else if (noOfOccupiedSlots == 1)
                        {
                            if (parentBTree.Comparer != null)
                            {
                                if (parentBTree.Comparer.Compare(currentNode.Slots[0].Key, item.Key) < 0)
                                    index = 1;
                            }
                            else
                            {
                                try
                                {
                                    if (
                                        System.Collections.Generic.Comparer<TKey>.Default.Compare(
                                            currentNode.Slots[0].Key, item.Key) < 0)
                                        index = 1;
                                }
                                catch (Exception)
                                {
                                    if (System.String.CompareOrdinal(currentNode.Slots[0].Key.ToString(), item.Key.ToString()) < 0)
                                        index = 1;
                                }
                            }
                        }
                        if (currentNode.Children == null)
                            break;
                        currentNode = currentNode.Children[index];
                    }
                    currentNode.Add(parentBTree, item, index);
                }

                private void Add(BTreeAlgorithm<TKey, TValue> parentBTree,
                                 BTreeItem<TKey, TValue> item, int index)
                {
                    byte noOfOccupiedSlots = _count;
                    // Add. check if node is not yet full..
                    if (noOfOccupiedSlots < parentBTree.SlotLength)
                    {
                        // ************** BPLUS 
                        // Insert the item(Add). if we want to implement BPLUS, we must do the modification here..
                        ShiftSlots(Slots, (byte) index, (byte) noOfOccupiedSlots);
                        Slots[index] = item;
                        _count++;
                        // *************BPLUS
                        return;
                    }
                    // node is full, use pTempSlots
                    Slots.CopyTo(parentBTree._tempSlots, 0);

                    // *************BPLUS
                    // Index now contains the correct array element number to insert item into.
                    // if we want to implement BPLUS, we must do the modification here..
                    ShiftSlots(parentBTree._tempSlots, (byte) index, (byte) (parentBTree.SlotLength));
                    parentBTree._tempSlots[index] = item;
                    // *************BPLUS

                    TreeNode rightNode;
                    TreeNode leftNode;
                    byte slotsHalf = (byte) (parentBTree.SlotLength >> 1);
                    if (Parent != null)
                    {
                        bool bIsUnBalanced = false;
                        int iIsThereVacantSlot = 0;
                        if (IsThereVacantSlotInLeft(parentBTree, ref bIsUnBalanced))
                            iIsThereVacantSlot = 1;
                        else if (IsThereVacantSlotInRight(parentBTree, ref bIsUnBalanced))
                            iIsThereVacantSlot = 2;
                        if (iIsThereVacantSlot > 0)
                        {
                            // copy temp buffer contents to the actual slots.
                            byte b = (byte) (iIsThereVacantSlot == 1 ? 0 : 1);
                            CopyArrayElements(parentBTree._tempSlots, b, Slots, 0, parentBTree.SlotLength);
                            if (iIsThereVacantSlot == 1)
                                // Vacant in left, "skud over" the leftmost node's item to parent and left.
                                DistributeToLeft(parentBTree, parentBTree._tempSlots[parentBTree.SlotLength]);
                            else if (iIsThereVacantSlot == 2)
                                // Vacant in right, move the rightmost node item into the vacant slot in right.
                                DistributeToRight(parentBTree, parentBTree._tempSlots[0]);
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
                            try
                            {
                                // Initialize should throw an exception if in error.
                                rightNode = parentBTree.GetRecycleNode(this);
                                leftNode = parentBTree.GetRecycleNode(this);
                                CopyArrayElements(parentBTree._tempSlots, 0, leftNode.Slots, 0, slotsHalf);
                                leftNode._count = slotsHalf;
                                CopyArrayElements(parentBTree._tempSlots, (ushort) (slotsHalf + 1), rightNode.Slots,
                                                  0, slotsHalf);
                                rightNode._count = slotsHalf;
                                ResetArray(Slots, null);
                                _count = 1;
                                Slots[0] = parentBTree._tempSlots[slotsHalf];
                                Children = new TreeNode[parentBTree.SlotLength + 1];

                                ResetArray(Children, null);
                                Children[(int) Sop.Collections.BTree.ChildNodes.LeftChild] = leftNode;
                                Children[(int) Sop.Collections.BTree.ChildNodes.RightChild] = rightNode;
                                //SUCCESSFUL!
                                return;
                            }
                            catch (Exception)
                            {
                                Children = null;
                                throw;
                            }
                        }
                        // All slots are occupied in this and other siblings' nodes..
                        // prepare this and the right node sibling and promote the temporary parent node(pTempSlot).
                        rightNode = parentBTree.GetRecycleNode(Parent);
                        // zero out the current slot.
                        ResetArray(Slots, null);
                        // copy the left half of the slots to left sibling
                        CopyArrayElements(parentBTree._tempSlots, 0, Slots, 0, slotsHalf);
                        _count = slotsHalf;
                        // copy the right half of the slots to right sibling
                        CopyArrayElements(parentBTree._tempSlots, (ushort) (slotsHalf + 1), rightNode.Slots,
                                          0, slotsHalf);
                        rightNode._count = slotsHalf;

                        // copy the middle slot to temp parent slot.
                        parentBTree._tempParent = parentBTree._tempSlots[slotsHalf];

                        // assign the new children nodes.
                        parentBTree._tempParentChildren[(int) Sop.Collections.BTree.ChildNodes.LeftChild] =
                            this;
                        parentBTree._tempParentChildren[(int) Sop.Collections.BTree.ChildNodes.RightChild] =
                            rightNode;

                        parentBTree._promoteParent = (TreeNode) Parent;
                        parentBTree._promoteIndexOfNode = GetIndexOfNode(parentBTree);
                        //TreeNode o = (TreeNode)Parent;
                        //o.Promote(ParentBTree, GetIndexOfNode(ParentBTree));
                        //SUCCESSFUL!
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
                    leftNode = null;
                    rightNode = null;
                    rightNode = parentBTree.GetRecycleNode(this);
                    leftNode = parentBTree.GetRecycleNode(this);
                    CopyArrayElements(parentBTree._tempSlots, 0, leftNode.Slots, 0, slotsHalf);
                    leftNode._count = slotsHalf;
                    CopyArrayElements(parentBTree._tempSlots, (ushort) (slotsHalf + 1), rightNode.Slots, 0,
                                      slotsHalf);
                    rightNode._count = slotsHalf;
                    ResetArray(Slots, null);
                    Slots[0] = parentBTree._tempSlots[slotsHalf];
                    _count = 1;
                    Children = new TreeNode[parentBTree.SlotLength + 1];
                    Children[(int) Sop.Collections.BTree.ChildNodes.LeftChild] = leftNode;
                    Children[(int) Sop.Collections.BTree.ChildNodes.RightChild] = rightNode;
                }

                //****** private modified binary search that facilitates Search of a key and if duplicates were found, positions the current record pointer to the 1st key instance.
                private static int BinarySearch<T>(T[] itemArray, int index,
                                                   int length, T value, System.Collections.Generic.IComparer<T> comparer)
                {
                    int r;
                    if (comparer != null && index != -1 && length != -1)
                        r = Array.BinarySearch<T>(itemArray, index, length, value, comparer);
#if !DEVICE
                    else if (index != -1 && length != -1)
                        r = Array.BinarySearch<T>(itemArray, index, length, value);
#endif
                    else
                        r = Array.BinarySearch<T>(itemArray, value);
                    if (r >= 0)
                    {
                        if (r >= 1)
                        {
                            int rr = BinarySearch(itemArray, 0, r, value, comparer);
                            if (rr >= 0)
                                return rr;
                        }
                    }
                    return r;
                }

                //****** end of modifed binary search functions

                /// <summary>
                /// Search BTreeAlgorithm for the item pointed to by Item. 
                /// NOTE: this should be invoked from root node.
                /// </summary>
                /// <param name="parentBTree">Parent BTree</param>
                /// <param name="item">Item to search in tree</param>
                /// <param name="goToFirstInstance">true tells BTree to go to First Instance of Key, else any key instance matching will match</param>
                /// <returns>true if item found, else false</returns>
                protected internal bool Search(BTreeAlgorithm<TKey, TValue> parentBTree,
                                               BTreeItem<TKey, TValue> item, bool goToFirstInstance)
                {
                    byte i = 0;
                    TreeNode currentNode = this;
                    TreeNode foundNode = null;
                    byte foundIndex = 0;
                    while (true)
                    {
                        i = 0;
                        byte noOfOccupiedSlots = currentNode._count;
                        if (noOfOccupiedSlots > 0)
                        {
                            int result;
                            if (parentBTree.Comparer != null)
                            {
                                result = goToFirstInstance
                                             ? BinarySearch(currentNode.Slots, 0, noOfOccupiedSlots, item,
                                                            parentBTree.SlotsComparer)
                                             : Array.BinarySearch<BTreeItem<TKey, TValue>>(currentNode.Slots, 0,
                                                                                           noOfOccupiedSlots, item,
                                                                                           parentBTree.SlotsComparer);
                            }
                            else
                            {
#if !DEVICE
                                try
                                {
                                    result = !goToFirstInstance ? Array.BinarySearch(currentNode.Slots, 0, noOfOccupiedSlots, item) : 
                                        BinarySearch(currentNode.Slots, 0, noOfOccupiedSlots, item, null);
                                }
                                catch (Exception)
                                {
#endif
                                    result = goToFirstInstance ? BinarySearch(currentNode.Slots, -1, -1, item, null) : 
                                        Array.BinarySearch(currentNode.Slots, item);
#if !DEVICE
                                }
#endif
                            }
                            if (result >= 0) // if found...
                            {
                                i = (byte) result;
                                foundNode = currentNode;
                                foundIndex = i;
                                if (!goToFirstInstance)
                                    break;
                            }
                            else
                                i = (byte) (~result);
                        }
                        if (currentNode.Children != null)
                            currentNode = currentNode.Children[i];
                        else
                            break;
                    }
                    if (foundNode != null)
                    {
                        parentBTree.SetCurrentItemAddress(foundNode, foundIndex);
                        return true;
                    }
                    // this must be the outermost node
                    // This block will make this item the current one to give chance to the Btree 
                    // caller the chance to check the items having the nearest key to the one it is interested at.
                    if (i == parentBTree.SlotLength) i--; // make sure i points to valid item
                    if (currentNode.Slots[i] != null)
                        parentBTree.SetCurrentItemAddress(currentNode, i);
                    else
                    {
                        i--;
                        // Update Current Item of this Node and nearest to the Key in sought Slot index
                        parentBTree.SetCurrentItemAddress(currentNode, i);
                        // Make the next item the current item. This has the effect of positioning making the next greater item the current item.
                        currentNode.MoveNext(parentBTree);
                        /*
							ItemAddress c = ParentBTree.CurrentItem;
							c.Node = this;
							c.NodeItemIndex = i;
							*/
                    }
                    return false;
                }
                /// <summary>
                /// Remove the current item from the tree
                /// </summary>
                /// <param name="parentBTree">Parent BTree</param>
                /// <returns>Always returns true</returns>
                protected internal bool Remove(BTreeAlgorithm<TKey, TValue> parentBTree)
                {
                    // check if there are children nodes.
                    if (Children != null)
                    {
                        byte byIndex = parentBTree.CurrentItem.NodeItemIndex;
                        // The below code allows the btree mngr to do virtually, all deletion to happen in the outermost nodes' slots.
                        MoveNext(parentBTree);
                            // sure to succeed since Children nodes are always in pairs(left & right). Make the new current item the occupant of the slot occupied by the deleted item.
                        Slots[byIndex] = parentBTree.CurrentItem.Node.Slots[parentBTree.CurrentItem.NodeItemIndex];
                        // Thus, the above code has the effect that the current item's slot is the deleted slot, so, the succeeding code that will remove the current slot will be fine..
                    }
                    // Always true since we expect the caller code to check if there is current item to delete and therefore, every Delete call will succeed.
                    return true;
                }

                protected internal void Clear()
                {
                    Clear(false);
                }

                /// <summary>
                /// Clear the whole tree.
                /// </summary>
                protected internal void Clear(bool recycle)
                {
                    Parent = null;
                    byte i;
                    for (i = 0; i < _count; i++)
                        Slots[i] = null;
                    if (!recycle)
                        Slots = null;
                    if (this.Children != null)
                    {
                        for (i = 0; i <= _count; i++)
                        {
                            if (!recycle)
                                Children[i].Clear();
                            Children[i] = null;
                        }
                        Children = null;
                    }
                    _count = 0;
                }

                /// <summary>
                /// Make the first item the current item. This member should be called from Root.
                /// </summary>
                /// <param name="parentBTree">BTree instance this Node is a part of</param>
                protected internal bool MoveFirst(BTreeAlgorithm<TKey, TValue> parentBTree)
                {
                    TreeNode node = this;
                    if (_count > 0 && Slots[0] != null)
                    {
                        while (node.Children != null)
                            node = node.Children[0];
                        parentBTree.SetCurrentItemAddress(node, 0);
                        return true; // At this level, always return SUCCESS
                    }
                    return false; // Collection is empty.
                }

                // If BPlus is gonna be implemented, just override this and use the CBPlusNode's Next and previous pointers.
                /// <summary>
                /// Make the next item in the tree the current item.
                /// </summary>
                /// <param name="parentBTree">Parent BTree</param>
                /// <returns>true if successful, else false</returns>
                protected internal bool MoveNext(BTreeAlgorithm<TKey, TValue> parentBTree)
                {
                    TreeNode currentNode = this;
                    byte slotIndex = (byte) (parentBTree.CurrentItem.NodeItemIndex + 1);
                    bool goRightDown = Children != null;
                    if (goRightDown)
                    {
                        while (true)
                        {
                            if (currentNode.Children != null)
                            {
                                currentNode = currentNode.Children[slotIndex];
                                slotIndex = 0;
                            }
                            else
                            {
                                parentBTree.SetCurrentItemAddress(currentNode, 0);
                                return true;
                            }
                        }
                    }
                    while (true)
                    {
                        // check if SlotIndex is within the maximum slot items and if it is, will index an occupied slot.
                        if (slotIndex < parentBTree.SlotLength && currentNode.Slots[slotIndex] != null)
                        {
                            parentBTree.SetCurrentItemAddress(currentNode, slotIndex);
                            return true;
                        }
                        if (currentNode.Parent != null)
                            // check if this is not the root node. (Root nodes don't have parent node.)
                        {
                            slotIndex = currentNode.GetIndexOfNode(parentBTree);
                            currentNode = currentNode.Parent;
                        }
                        else
                        {
                            // this is root node. set to null the current item(End of Btree is reached)
                            parentBTree.SetCurrentItemAddress(null, 0);
                            return false;
                        }
                    }
                }

                /// <summary>
                /// Make previous item in the tree current item.
                /// </summary>
                /// <param name="parentBTree">Parent BTree</param>
                /// <returns>true if successful, else false</returns>
                protected internal bool MovePrevious(BTreeAlgorithm<TKey, TValue> parentBTree)
                {
                    byte slotIndex = parentBTree.CurrentItem.NodeItemIndex;
                    bool goLeftDown = Children != null;
                    TreeNode currentNode = this;
                    if (goLeftDown)
                    {
                        while (true)
                        {
                            if (currentNode.Children != null)
                            {
                                byte ii = currentNode.Children[slotIndex]._count;
                                currentNode = currentNode.Children[slotIndex];
                                slotIndex = ii;
                            }
                            else
                            {
                                // 'SlotIndex -1' since we are now using SlotIndex as index to pSlots.
                                parentBTree.SetCurrentItemAddress(currentNode, (byte) (slotIndex - 1));
                                return true;
                            }
                        }
                    }
                    short si = (short) (slotIndex - 1);
                    while (true)
                    {
                        // check if SlotIndex is within the maximum slot items and if it is, will index an occupied slot.
                        if (si >= 0)
                        {
                            parentBTree.SetCurrentItemAddress(currentNode, (byte) si);
                            return true;
                        }
                        if (currentNode.Parent != null)
                            // check if this is not the root node. (Root nodes don't have parent node.)
                        {
                            byte i = currentNode.GetIndexOfNode(parentBTree);
                            currentNode = currentNode.Parent;
                            si = (short) (i - 1);
                        }
                        else
                        {
                            // this is root node. set to null the current item(End of Btree is reached)
                            parentBTree.SetCurrentItemAddress(null, 0);
                            return false;
                        }
                    }
                }

                // CBPlusNode has pointer to last, use it when implementing BPlus.. This should be called from the root node.
                /// <summary>
                /// Make the last item in the tree the current item.
                /// </summary>
                /// <param name="parentBTree">Parent BTree</param>
                /// <returns>true if successful, else false</returns>
                protected internal bool MoveLast(BTreeAlgorithm<TKey, TValue> parentBTree)
                {
                    TreeNode node = this;
                    while (node.Children != null)
                        node = node.Children[node._count];
                    parentBTree.SetCurrentItemAddress(node, (byte) (node._count - 1));
                    if (parentBTree.CurrentItem.Node != null)
                        return true;
                    // return BOC error to tell the caller code that this tree is empty.
                    return false; // empty tree.
                }

                // Assigns a new parent for this node.
                /// <summary>
                /// Make "NewParent" the parent of this Node.
                /// </summary>
                /// <param name="newParent">New Parent TreeNode</param>
                private void SetParent(TreeNode newParent)
                {
                    Parent = newParent;
                }

                /// <summary>
                /// Returns true if slots are all occupied, else false
                /// </summary>
                /// <param name="slotLength">Number of slots per node</param>
                /// <returns>true if full, else false</returns>
                private bool IsFull(byte slotLength)
                {
                    return _count == slotLength;
                }

                /// <summary>
                /// Returns index of this node relative to parent. 
                /// Note: you must call this after you check that there is a parent node.
                /// </summary>
                /// <param name="parentBTree"> </param>
                /// <returns>Index of this node per its parent</returns>
                private byte GetIndexOfNode(BTreeAlgorithm<TKey, TValue> parentBTree)
                {
                    if (Parent != null)
                    {
                        byte slotLength = parentBTree.SlotLength;
                        if (_indexOfNode == -1 || Parent.Children[_indexOfNode] != this)
                        {
                            for (_indexOfNode = 0;
                                 _indexOfNode <= slotLength &&
                                 Parent.Children[_indexOfNode] != null;
                                 _indexOfNode++)
                                if (Parent.Children[_indexOfNode] == this) break;
                        }
                        if (_indexOfNode >= 0 || _indexOfNode > Parent._count + 1)
                            return (byte) _indexOfNode;
                    }
                    // Just return 0 if called in the root node, anyway, the caller code should check if it is the root node and not call this function if it is!
                    return 0;
                }

                private short _indexOfNode = -1;

                /// <summary>
                /// Returns left sibling or null if finished traversing left nodes.
                /// </summary>
                /// <param name="parentBTree"> </param>
                /// <returns>Left sibling TreeNode reference</returns>
                private TreeNode GetLeftSibling(BTreeAlgorithm<TKey, TValue> parentBTree)
                {
                    int index = GetIndexOfNode(parentBTree);
                    // if we are not at the leftmost sibling yet, return left sibling, otherwise null..
                    return index > 0 ? Parent.Children[index - 1] : null;
                }

                /// <summary>
                /// Returns right sibling or null if finished traversing right nodes.
                /// </summary>
                /// <param name="parentBTree"> </param>
                /// <returns>Right sibling TreeNode reference</returns>
                private TreeNode GetRightSibling(BTreeAlgorithm<TKey, TValue> parentBTree)
                {
                    int index = GetIndexOfNode(parentBTree);
                    // if we are not at the Rightmost sibling yet, returh right sibling, otherwise null..
                    return index < parentBTree.SlotLength ? Parent.Children[index + 1] : null;
                }

                /// <summary>
                /// Returns true if a slot is available in left side siblings of this node modified to suit possible unbalanced branch.
                /// </summary>
                /// <param name="parentBTree">Parent BTree</param>
                /// <param name="isUnBalanced">Will be updated to true if this branch is detected to be "unbalanced", else false</param>
                /// <returns>true if there is a vacant slot, else false</returns>
                private bool IsThereVacantSlotInLeft(BTreeAlgorithm<TKey, TValue> parentBTree, ref bool isUnBalanced)
                {
                    isUnBalanced = false;
                    // start from this node.
                    TreeNode temp = this;
                    while ((temp = temp.GetLeftSibling(parentBTree)) != null)
                    {
                        if (temp.Children != null)
                        {
                            isUnBalanced = true;
                            return false;
                        }
                        if (!temp.IsFull(parentBTree.SlotLength))
                            return true;
                    }
                    return false;
                }

                /// <summary>
                /// Returns true if a slot is available in right side siblings of this node modified to suit possible unbalanced branch.
                /// </summary>
                /// <param name="parentBTree">Parent BTree</param>
                /// <param name="isUnBalanced">Will be updated to true if this branch is detected to be "unbalanced", else false</param>
                /// <returns>true if there is a vacant slot, else false</returns>
                private bool IsThereVacantSlotInRight(BTreeAlgorithm<TKey, TValue> parentBTree, ref bool isUnBalanced)
                {
                    isUnBalanced = false;
                    // start from this node.
                    TreeNode temp = this;
                    while ((temp = temp.GetRightSibling(parentBTree)) != null)
                    {
                        if (temp.Children != null)
                        {
                            isUnBalanced = true;
                            return false;
                        }
                        if (!temp.IsFull((byte) parentBTree.SlotLength)) return true;
                    }
                    return false;
                }

                /// <summary>
                /// This gets called when the node's slots are overflowed and break up
                ///	is needed. This does the necessary recursive promotion of the 
                ///	newly born nodes as affected by the break up.<br/>
                ///	Uses caller Btree object's Temporary Slots and Children nodes
                ///	which are accessible via GetTempSlot() and _GetTempParentChildren()
                ///	as storage of Parent and newly born siblings.<br/><br/>
                ///	NOTE: Uses Temporary Slots and Children nodes which are accessible via GetTempSlot() and _GetTempParentChildren() as storage of Parent and newly born siblings.
                /// </summary>
                /// <param name="parentBTree">parent BTree</param>
                /// <param name="position">Position of the broken apart node in its parent node's slots</param>
                internal void Promote(BTreeAlgorithm<TKey, TValue> parentBTree, byte position)
                {
                    byte noOfOccupiedSlots = this._count,
                         index = position;
                    if (noOfOccupiedSlots < (byte) parentBTree.SlotLength)
                    {
                        // node is not yet full.. insert the parent.
                        ShiftSlots(Slots, index, noOfOccupiedSlots);

                        if (index > noOfOccupiedSlots)
                            index = noOfOccupiedSlots;

                        Slots[index] = parentBTree._tempParent;
                        // insert the left child
                        Children[index] =
                            parentBTree._tempParentChildren[(int) Sop.Collections.BTree.ChildNodes.LeftChild];
                        // insert the right child
                        ShiftSlots(Children, (byte) (index + 1), (byte) (noOfOccupiedSlots + 1));
                        Children[index + 1] =
                            parentBTree._tempParentChildren[(int) Sop.Collections.BTree.ChildNodes.RightChild];
                        _count++;
                        return; // successful
                    }
                    // *** Insert to temp slots.. node is full, use pTempSlots
                    CopyArrayElements(Slots, 0, parentBTree._tempSlots, 0, (ushort) parentBTree.SlotLength);
                    ShiftSlots(parentBTree._tempSlots, index, (byte) parentBTree.SlotLength);
                    parentBTree._tempSlots[index] = parentBTree._tempParent;
                    CopyArrayElements(Children, 0, parentBTree._tempChildren, 0,
                                      (ushort) (parentBTree.SlotLength + 1));
                    // insert the left child
                    parentBTree._tempChildren[index] =
                        parentBTree._tempParentChildren[(int) Sop.Collections.BTree.ChildNodes.LeftChild];
                    // insert the right child
                    ShiftSlots(parentBTree._tempChildren, (byte) (index + 1), (byte) (noOfOccupiedSlots + 1));
                    parentBTree._tempChildren[index + 1] =
                        parentBTree._tempParentChildren[(int) Sop.Collections.BTree.ChildNodes.RightChild];
                    // *** Try to break up the node into 2 siblings.
                    TreeNode leftNode = null;
                    TreeNode rightNode = null;
                    byte slotsHalf = (byte) ((byte) parentBTree.SlotLength >> (byte) 1);
                    if (Parent != null)
                    {
                        // prepare this and the right node sibling and promote the temporary parent node(pTempSlot).
                        // this is the left sibling !
                        try
                        {

                            rightNode = parentBTree.GetRecycleNode(Parent);
                            rightNode.Children = new TreeNode[parentBTree.SlotLength + 1];
                            // zero out the current slot.
                            ResetArray(Slots, null);
                            // zero out this children node pointers.
                            ResetArray(Children, null);
                            // copy the left half of the slots to left sibling(this)
                            CopyArrayElements(parentBTree._tempSlots, 0, Slots, 0, slotsHalf);
                            // copy the right half of the slots to right sibling
                            CopyArrayElements(parentBTree._tempSlots, (ushort) (slotsHalf + 1), rightNode.Slots,
                                              (ushort) 0, (ushort) slotsHalf);
                            _count = slotsHalf;
                            rightNode._count = slotsHalf;
                            // copy the left half of the children nodes.
                            CopyArrayElements(parentBTree._tempChildren, 0, Children, 0, (ushort) (slotsHalf + 1));
                            // copy the right half of the children nodes.
                            CopyArrayElements(parentBTree._tempChildren, (ushort) (slotsHalf + 1), rightNode.Children,
                                              0, (ushort) (slotsHalf + 1));
                            if (rightNode.Children != null)
                                // left sibling is already parent of its children. make the right sibling parent of its children.
                                for (index = 0; index <= slotsHalf; index++)
                                    rightNode.Children[index].SetParent(rightNode);
                            // copy the middle slot
                            parentBTree._tempParent = parentBTree._tempSlots[slotsHalf];
                            // assign the new children nodes.
                            parentBTree._tempParentChildren[(int) Sop.Collections.BTree.ChildNodes.LeftChild] = this;
                            parentBTree._tempParentChildren[(int) Sop.Collections.BTree.ChildNodes.RightChild] =
                                rightNode;
                            parentBTree._promoteParent = Parent;
                            parentBTree._promoteIndexOfNode = GetIndexOfNode(parentBTree);
                            //Parent.Promote(ParentBTree, GetIndexOfNode(ParentBTree));
                            return;
                        }
                        catch (Exception e)
                        {
                            string s = "Error in attempt to promote parent of a splitted node.";
                            Log.Logger.Instance.Log(Log.LogLevels.Fatal, e);
                            throw new Exception(s, e);
                        }
                    }
                    // no parent
                    leftNode = parentBTree.GetRecycleNode(this);
                    rightNode = parentBTree.GetRecycleNode(this);
                    // copy the left half of the slots
                    CopyArrayElements(parentBTree._tempSlots, 0, leftNode.Slots, 0, slotsHalf);
                    leftNode._count = slotsHalf;
                    // copy the right half of the slots
                    CopyArrayElements(parentBTree._tempSlots, (ushort) (slotsHalf + 1), rightNode.Slots, 0,
                                      slotsHalf);
                    rightNode._count = slotsHalf;
                    leftNode.Children = new TreeNode[parentBTree.SlotLength + 1];
                    rightNode.Children = new TreeNode[parentBTree.SlotLength + 1];
                    // copy the left half of the children nodes.
                    CopyArrayElements(parentBTree._tempChildren, 0, leftNode.Children, 0,
                                      (ushort) (slotsHalf + 1));
                    // copy the right half of the children nodes.
                    CopyArrayElements(parentBTree._tempChildren, (ushort) (slotsHalf + 1), rightNode.Children,
                                      0, (ushort) (slotsHalf + 1));
                    // make the left sibling parent of its children.
                    for (index = 0; index <= slotsHalf; index++)
                        leftNode.Children[index].SetParent(leftNode);
                    // make the right sibling parent of its children.
                    for (index = 0; index <= slotsHalf; index++)
                        rightNode.Children[index].SetParent(rightNode);
                    // zero out the current slot.
                    ResetArray(Slots, null);
                    ResetArray(Children, null);
                    // copy the middle slot
                    Slots[0] = parentBTree._tempSlots[slotsHalf];
                    _count = 1;
                    // assign the new children nodes.
                    Children[(int) Sop.Collections.BTree.ChildNodes.LeftChild] = leftNode;
                    Children[(int) Sop.Collections.BTree.ChildNodes.RightChild] = rightNode;
                }

                /// <summary>
                /// Distribute to left siblings the item if the current slots are  all filled up.
                /// Used when balancing the nodes' load of the current sub-tree.
                /// </summary>
                /// <param name="bTree"> </param>
                /// <param name="item">Item to distribute to left sibling node</param>
                internal void DistributeToLeft(BTreeAlgorithm<TKey, TValue> bTree, BTreeItem<TKey, TValue> item)
                {
                    byte slotLength = bTree.SlotLength;
                    if (IsFull(slotLength))
                    {
                        // counter-clockwise rotation..					
                        //	----
                        //	|  |
                        //	-> |
                        // NOTE: we don't check for null returns as this method is called only when there is vacant in left

                        bTree.DistributeSibling = GetLeftSibling(bTree);
                        bTree.DistributeItem = Parent.Slots[GetIndexOfNode(bTree) - 1];
                        bTree.DistributeLeftDirection = true;

                        //GetLeftSibling(parentBTree).DistributeToLeft(parentBTree,
                        //                                             Parent.Slots[GetIndexOfNode(parentBTree) - 1]);

                        Parent.Slots[GetIndexOfNode(bTree) - 1] = Slots[0];
                        MoveArrayElements(Slots, 1, 0, (ushort) (slotLength - 1));
                        Slots[_count - 1] = null;
                    }
                    else
                        _count++;
                    Slots[_count - 1] = item;
                }

                /// <summary>
                /// Distribute to right siblings the item if the current slots are all filled up.
                /// Used when balancing the nodes' load of the current sub-tree.
                /// </summary>
                /// <param name="bTree"> </param>
                /// <param name="item">Item to distribute to right sibling</param>
                internal void DistributeToRight(BTreeAlgorithm<TKey, TValue> bTree, BTreeItem<TKey, TValue> item)
                {
                    byte slotLength = bTree.SlotLength;
                    if (IsFull(slotLength))
                    {
                        // clockwise rotation..
                        //	----
                        //	|  |
                        //	| <-

                        bTree.DistributeSibling = GetRightSibling(bTree);
                        bTree.DistributeItem = Parent.Slots[GetIndexOfNode(bTree)];
                        bTree.DistributeLeftDirection = false;
                        //GetRightSibling(parentBTree).DistributeToRight(parentBTree,
                        //                                               Parent.Slots[GetIndexOfNode(parentBTree)]);

                        Parent.Slots[GetIndexOfNode(bTree)] = Slots[_count - 1];
                    }
                    else
                        _count++;
                    ShiftSlots(Slots, 0, (byte) (slotLength - 1));
                    Slots[0] = item;
                }

                /// <summary>
                /// Overwrite the current item with the item from the next or previous slot.
                /// Attempts to free the TreeNode object by setting Parent, Children and Slots to null.
                /// </summary>
                /// <param name="bTree">Parent BTree</param>
                internal void FixTheVacatedSlot(BTreeAlgorithm<TKey, TValue> bTree)
                {
                    sbyte c;
                    c = (sbyte) _count;
                    if (c > 1) // if there are more than 1 items in slot then..
                    {
                        //***** We don't fix the children since there are no children at this scenario.
                        if (bTree.CurrentItem.NodeItemIndex < c - 1)
                            MoveArrayElements(Slots,
                                              (ushort) (bTree.CurrentItem.NodeItemIndex + 1),
                                              bTree.CurrentItem.NodeItemIndex,
                                              (ushort) (c - 1 - bTree.CurrentItem.NodeItemIndex));
                        _count--;
                        Slots[_count] = null; // nullify the last slot.
                        return;
                    }
                    // only 1 item in slot
                    if (Parent != null)
                    {
                        byte ucIndex;
                        // if there is a pullable item from sibling nodes.
                        if (SearchForPullableItem(bTree, out ucIndex))
                        {
                            if (ucIndex < GetIndexOfNode(bTree))
                                PullFromLeft(bTree); // pull an item from left
                            else
                                PullFromRight(bTree); // pull an item from right
                            return;
                        }
                        // Parent has only 2 children nodes..
                        if (Parent.Children[0] == this)
                        {
                            // this is left node
                            TreeNode rightSibling = GetRightSibling(bTree);
                            Parent.Slots[1] = rightSibling.Slots[0];
                            Parent._count = 2;
                            bTree.AddRecycleNode(rightSibling);
                        }
                        else
                        {
                            // this is right node
                            Parent.Slots[1] = Parent.Slots[0];
                            TreeNode leftSibling = GetLeftSibling(bTree);
                            Parent.Slots[0] = leftSibling.Slots[0];
                            Parent._count = 2;
                            bTree.AddRecycleNode(leftSibling);
                        }
                        // recycle this node...
                        Parent.Children[0] = null;
                        Parent.Children[1] = null;
                        Parent.Children = null;
                        //bTree.AddRecycleNode(this);
                        return;
                    }
                    // only 1 item in root node !
                    Slots[0] = null; // just nullIFY the slot.
                    _count = 0;
                    bTree.SetCurrentItemAddress(null, 0); // Point the current item pointer to end of tree
                }

                /// <summary>
                /// Recursively pull item from left side. Modified to process unbalanced branch - 10/31/97.
                /// Pull an item from the left siblings. Used when this node run out of loaded items and instead of destroying itself, will pull an item from the left siblings to maintain the balanceness of this sub-tree.
                /// </summary>
                /// <param name="bTree">Parent BTree</param>
                internal void PullFromLeft(BTreeAlgorithm<TKey, TValue> bTree)
                {
                    byte i = _count;
                    if (i > 1) // more than 1 item.
                    {
                        _count--;
                        // we only need to nullify the last item since the caller code should have moved it to the slot, which item just got deleted or pulled.
                        Slots[i - 1] = null;
                        return;
                    }
                    // *********** Start of Unbalanced right sibling branch processing check if there is a right sibling and if it has children node.
                    TreeNode leftSibling = GetLeftSibling(bTree);
                    i = GetIndexOfNode(bTree);
                    if (leftSibling != null && leftSibling.Children != null)
                    {
                        #region process unbalanced branch
                        // the following code should process unbalanced sibling branch.
                        //BTreeItem<TKey, TValue> MoveThisToLeft = Parent.Slots[i - 1];
                        //int CopyItems = Parent.count - i;
                        //if (CopyItems == 0)
                        //    CopyItems++;
                        //MoveArrayElements(Parent.Slots, (ushort)i, (ushort)(i - 1), (ushort)(CopyItems - 1));
                        //Parent.Slots[Parent.count - 1] = null;
                        //if (Parent.Children.Length > i + 1)
                        //    MoveArrayElements(Parent.Children, (ushort)(i + 1), (ushort)i, (ushort)CopyItems);
                        //Parent.Children[Parent.count] = null;
                        //Parent.count--;
                        //LeftSibling.Add(ParentBTree, MoveThisToLeft);
                        //if (Parent.count == 0)
                        //{
                        //    Array.Copy(LeftSibling.Slots, Parent.Slots, LeftSibling.count);
                        //    if (LeftSibling.Children != null)
                        //    {
                        //        Array.Copy(LeftSibling.Children, Parent.Children, LeftSibling.count + 1);
                        //        for (int Index = 0; Index <= LeftSibling.count; Index++)
                        //            Parent.Children[Index].SetParent(Parent);
                        //    }
                        //    else
                        //    {
                        //        Parent.Children[0] = null;
                        //        Parent.Children[1] = null;
                        //        Parent.Children = null;
                        //    }
                        //    Parent.count = LeftSibling.count;
                        //    ParentBTree.AddRecycleNode(LeftSibling);
                        //}
                        //MoveThisToLeft = null;
                        //ParentBTree.AddRecycleNode(this);
                        #endregion
                        i = (byte) (GetIndexOfNode(bTree) - 1);
                        Slots[0] = Parent.Slots[i];
                        bTree.SetCurrentItemAddress(Parent, i);
                        bTree.CurrentItem.Node.MovePrevious(bTree);
                        Parent.Slots[i] = bTree.CurrentItem.Node.Slots[bTree.CurrentItem.NodeItemIndex];
                        bTree.FixVacatedSlot = true;
                        //ParentBTree.CurrentItem.Node.FixTheVacatedSlot(ParentBTree);
                        return;
                    } // *********** End of Unbalanced right sibling branch processing
                    // There is only 1 item in the slot and there is no unbalanced left sibling.
                    if (i == 1 && leftSibling._count == 1)
                    {
                        // we need to combine the leftmost sibling's item with the
                        // parent's 1st item and make them the leftmost node's items.
                        // This scenario caters for this:
                        //		[ 5 | 7 ]
                        //     /    |    \
                        //   [3]  [*7*] [10]
                        // NOTE: slot containing 7 is this node's slot and which was already moved 
                        // to the parent. See the slot in the parent containing 7.

                        // After this block of code will be:
                        //		[ 7 ]
                        //     /     \
                        //  [3|5]    [10]
                        // NOTE: 5 was joined with 3 on the same slots container, 
                        // parent's 7 moved to the slot previously occupied by 5, [*7] got deleted
                        // and 10 became the parent's 1st item's right child.

                        leftSibling.Slots[1] = Parent.Slots[0];
                        leftSibling._count = 2;
                        i = Parent._count;
                        MoveArrayElements(Parent.Slots, 1, 0, (ushort) (i - 1));
                        Parent.Slots[i - 1] = null;
                        MoveArrayElements(Parent.Children, 2, 1, (ushort) (i - 1));
                        Parent.Children[i] = null;
                        Parent._count--;
                        //bTree.AddRecycleNode(this);
                        return;
                    }
                    Slots[0] = Parent.Slots[i - 1];
                    Parent.Slots[i - 1] = leftSibling.Slots[leftSibling._count - 1];
                    bTree.PullSibling = leftSibling;
                    bTree.PullLeftDirection = true;
                    //leftSibling.PullFromLeft(parentBTree);
                }

                /// <summary>
                /// Recursively pull item from right side. Modified to process unbalanced branch - 10/31/97
                /// Same as above except that the pull is from the right siblings.
                /// </summary>
                /// <param name="bTree">Paren BTree</param>
                internal void PullFromRight(BTreeAlgorithm<TKey, TValue> bTree)
                {
                    byte i = _count;
                    if (i > 1)
                    {
                        _count--;
                        MoveArrayElements(Slots, 1, 0, _count);
                        Slots[i - 1] = null;
                        return;
                    }
                        // *********** Start of Unbalanced right sibling branch processing check if there is a right sibling and if it has children node.
                    TreeNode rightSibling = GetRightSibling(bTree);
                    i = GetIndexOfNode(bTree);
                    if (rightSibling != null && rightSibling.Children != null)
                    {
                        // the following code should process unbalanced sibling branch.

                        // This scenario caters for this:
                        //		[ 5 | 7 ]
                        //     /    |    \
                        //   [3]  [*7*] [10]
                        // NOTE: slot containing 7 is this node's slot and which was already moved 
                        // to the parent. See the slot in the parent containing 7.

                        // After this block of code will be:
                        //		[ 7 ]
                        //     /     \
                        //  [3|5]    [10]
                        // NOTE: 5 was joined with 3 on the same slots container, 
                        // parent's 7 moved to the slot previously occupied by 5, [*7] got deleted
                        // and 10 became the parent's 1st item's right child.

                        //BTreeItem<TKey, TValue> MoveThisToLeft = Parent.Slots[i];
                        //MoveArrayElements(Parent.Slots, (ushort)(i + 1), (ushort)i, (ushort)(Parent.count - i - 1));
                        //Parent.Slots[Parent.count - 1] = null;
                        //MoveArrayElements(Parent.Children, (ushort)(i + 1), (ushort)i, (ushort)(Parent.count - i));
                        //Parent.Children[Parent.count] = null;
                        //Parent.count--;
                        //RightSibling.Add(ParentBTree, MoveThisToLeft);
                        //if (Parent.count == 0)
                        //{
                        //    Array.Copy(RightSibling.Slots, Parent.Slots, RightSibling.count);
                        //    if (RightSibling.Children != null)
                        //    {
                        //        Array.Copy(RightSibling.Children, Parent.Children, RightSibling.count + 1);
                        //        for (int Index = 0; Index <= RightSibling.count; Index++)
                        //            Parent.Children[Index].SetParent(Parent);
                        //    }
                        //    else
                        //    {
                        //        Parent.Children[0] = null;
                        //        Parent.Children[1] = null;
                        //        Parent.Children = null;
                        //    }
                        //    Parent.count = RightSibling.count;
                        //    ParentBTree.AddRecycleNode(RightSibling);
                        //}
                        //MoveThisToLeft = null;
                        //ParentBTree.AddRecycleNode(this);

                        Slots[0] = Parent.Slots[i];
                        bTree.SetCurrentItemAddress(Parent, i);
                        bTree.CurrentItem.Node.MoveNext(bTree);
                        Parent.Slots[i] = bTree.CurrentItem.Node.Slots[bTree.CurrentItem.NodeItemIndex];
                        bTree.FixVacatedSlot = true;
                        //ParentBTree.CurrentItem.Node.FixTheVacatedSlot(ParentBTree);
                        return;
                    } // *********** End of Unbalanced right sibling branch processing
                    if (i == Parent._count - 1 &&
                        rightSibling._count == 1)
                    {
                        // we need to combine the Rightmost sibling's item with the parent's last item and make them the rightmost node's items.
                        rightSibling.Slots[1] = rightSibling.Slots[0];
                        rightSibling.Slots[0] = Parent.Slots[Parent._count - 1];
                        Parent.Children[i] = rightSibling;
                        Parent.Children[i + 1] = null;
                        Parent.Slots[i] = null;
                        Parent._count--;
                        rightSibling._count = 2;
                        //bTree.AddRecycleNode(this);
                        return;
                    }
                    Slots[0] = Parent.Slots[i];
                    Parent.Slots[i] = rightSibling.Slots[0];

                    bTree.PullSibling = rightSibling;
                    bTree.PullLeftDirection = false;
                    //rightSibling.PullFromRight(bTree);
                }

                /// <summary>
                /// Search for a pullable item from sibling nodes of this node. Modified for unbalanced branch's correct detection of pullable item. -10/31/97
                /// Find a pullable item. Will return true if there is one.
                /// </summary>
                /// <param name="parentBTree"> </param>
                /// <param name="index">Will be updated of the pullable item's index in the slot</param>
                /// <returns>true if there is pullable item, else false</returns>
                private bool SearchForPullableItem(BTreeAlgorithm<TKey, TValue> parentBTree, out byte index)
                {
                    index = 0;
                    byte slotLength = parentBTree.SlotLength;
                    if (Parent._count == 1)
                    {
                        index = (byte) (GetIndexOfNode(parentBTree) ^ 1);
                        return Parent.Children[index].Children != null ||
                               Parent.Children[index]._count > 1;
                    }
                    for (byte i = 0; i <= slotLength && Parent.Children[i] != null; i++)
                    {
                        if (this != Parent.Children[i])
                        {
                            index = i; // pick one in case the below statement won't be true.
                            if (Parent.Children[i]._count > 1)
                                break;
                        }
                    }
                    return true;
                }

                /// <summary>
                /// Slots of this TreeNode
                /// </summary>
                protected internal BTreeItem<TKey, TValue>[] Slots; // available Slots

                /// <summary>
                /// Count of items in this Node.
                /// </summary>
                protected byte _count;   //90;

                /// <summary>
                /// Parent of this TreeNode
                /// </summary>
                protected internal TreeNode Parent; // parent TreeNode node

                /// <summary>
                /// Children of this TreeNode
                /// </summary>
                protected TreeNode[] Children; // Children TreeNode nodes
            }

            // end of TreeNode
            /// <summary>
            /// The root node class. Encapsulates behavior specific to root nodes. 
            /// Also, since we support tree reuse, this adds attributes to support reuse.
            /// </summary>
            internal class TreeRootNode : TreeNode
            {
                /// <summary>
                /// Constructor
                /// </summary>
                /// <param name="parentTree">Paren BTree</param>
                protected internal TreeRootNode(BTreeAlgorithm<TKey, TValue> parentTree) : base(parentTree)
                {
                }

                /// <summary>
                /// Get: returns the number of loaded items in the tree<br/>
                /// Set: assigns the number of loaded items in the tree
                /// </summary>
                internal int TreeCount = 0;

                /// <summary>
                /// Destroy all collected items and shell(slots) excluding the root shell. This renders the btree empty.
                /// </summary>
                protected internal new void Clear()
                {
                    if (this.Children != null)
                    {
                        for (byte i = 0; i <= _count; i++)
                            // Clear children nodes.
                            Children[i].Clear();
                    }
                    Children = null;
                    ResetArray(Slots, null);
                    // reset to 0 the treenode count...
                    _count = 0;
                }
            }

            //end of TreeRootNode
        }
    }
}