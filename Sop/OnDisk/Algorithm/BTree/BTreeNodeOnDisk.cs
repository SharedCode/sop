// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using Sop.Collections.BTree;
using Sop.OnDisk.Algorithm.SortedDictionary;
using Sop.Persistence;
using Sop.Utility;

namespace Sop.OnDisk.Algorithm.BTree
{
    /// <summary>
    /// B-Tree Node On Disk
    /// </summary>
    internal partial class BTreeNodeOnDisk : IInternalPersistent, Recycling.IRecyclable, IBTreeNodeOnDisk, IDisposable, ICloneable
    {
        public object Clone()
        {
            var r = new BTreeNodeOnDisk
            {
                Count = Count,
                DiskBuffer = (Sop.DataBlock)DiskBuffer.Clone(),
                HintSizeOnDisk = HintSizeOnDisk,
                IsDirty = IsDirty,
                ParentAddress = ParentAddress,
                _indexOfNode = _indexOfNode
            };
            r.Slots = new BTreeItemOnDisk[Slots.Length];
            Slots.CopyTo(r.Slots, 0);
            if (ChildrenAddresses == null)
                return r;
            r.ChildrenAddresses = new long[ChildrenAddresses.Length];
            ChildrenAddresses.CopyTo(r.ChildrenAddresses, 0);
            return r;
        }

        private short GetIndex(BTree.BTreeAlgorithm bTree, BTreeItemOnDisk item, out bool dupeDetected)
        {
            dupeDetected = false;
            short index = 0;
            if (Count > 1)
            {
                if (bTree.Comparer != null)
                    index = (short) Array.BinarySearch(Slots, 0, Count, item, bTree.ComparerWrapper);
                else
                {
#if !DEVICE
                    try
                    {
                        index = (short) Array.BinarySearch(Slots, 0, Count, item);
                    }
                    catch
                    {
#endif
                        try
                        {
                            index = (short) Array.BinarySearch(Slots, item);
                        }
                        catch //(Exception innerE)
                        {
                            throw new InvalidOperationException("No Comparer Error.");
                        }
#if !DEVICE
                    }
#endif
                }
                if (index < 0)
                    index = (short)~index;
                if (bTree.IsUnique && index >= 0)
                {
                    short i = index;
                    if (i >= Slots.Length)
                        i--;
                    var result = Compare(bTree, Slots[i], item);
                    if (result == 0)
                    {
                        dupeDetected = true;
                        return i;
                    }
                }
            }
            else if (Count == 1)
            {
                var result = Compare(bTree, Slots[0], item);
                if (result < 0)
                    index = 1;
                // check for uniqueness if req'd...
                else if (bTree.IsUnique && result == 0)
                {
                    dupeDetected = true;
                    return 0;
                }
            }
            return index;
        }
        private int Compare(BTree.BTreeAlgorithm bTree, BTreeItemOnDisk a, BTreeItemOnDisk b)
        {
            if (a == null && b == null) return 0;
            if (a == null) return -1;
            if (b == null) return 1;

            if (bTree.Comparer != null)
            {
                return bTree.ComparerWrapper.Compare(a, b);
            }
            else
            {
                bTree.Comparer = new SystemDefaultComparer();
                try
                {
                    return bTree.ComparerWrapper.Compare(a, b);
                }
                catch (Exception)
                {
                    bTree.Comparer = new BTree.BTreeDefaultComparer();
                    return bTree.ComparerWrapper.Compare(a, b);
                }
            }
        }

        private static void RemoveFromCache(BTree.BTreeAlgorithm bTree, BTreeNodeOnDisk node)
        {
            if (node == null)
                return;
            bTree.MruManager.Remove(node.GetAddress(bTree), true);
        }

        internal static BTreeNodeOnDisk ReadNodeFromDisk(BTreeAlgorithm bTree, long id)
        {
            BTreeNodeOnDisk node;
            if (bTree.InMaintenanceMode)
            {
                node = bTree.PromoteLookup[id];
                if (node != null)
                    return node;
            }
            node = (BTreeNodeOnDisk) bTree.MruManager[id];
            if (node == null || node.Slots == null || node.DiskBuffer == null)
                node = (BTreeNodeOnDisk)bTree.OnRead(id);
            else
            {
                if (bTree.InMaintenanceMode)
                {
                    // move node from MRU to promote lookup...
                    bTree.MruManager.Remove(id, true);
                    bTree.PromoteLookup[id] = node;
                }
            }
            return node;
        }

        internal void SaveNodeToDisk(BTreeAlgorithm bTree)
        {
            IsDirty = true;
            bTree.SaveNode(this, !bTree.InMaintenanceMode);
            if (bTree.InMaintenanceMode)
            {
                RemoveFromCache(bTree, this);
                IsDirty = true;
                bTree.PromoteLookup[bTree.GetId(DiskBuffer)] = this;
            }
        }

        private void RemoveFromBTreeBlocksCache(BTree.BTreeAlgorithm bTree, BTreeNodeOnDisk node)
        {
            Sop.DataBlock headBlock = node.DiskBuffer;
            if (headBlock != null)
            {
                headBlock.ClearData();

                if (headBlock.DataAddress >= 0 &&
                    !(bTree.RootNode.DiskBuffer == headBlock ||
                      bTree.RootNode.GetAddress(bTree) == headBlock.DataAddress))
                {
                    headBlock.RemoveFromCache(bTree);
                }
            }
        }

        private bool SynchronizeCount()
        {
            if (Count > 0 && Slots[Count - 1] == null)
            {
                for (short ctr = 0; ctr < Slots.Length; ctr++)
                {
                    if (Slots[ctr] == null)
                    {
                        Count = ctr;
                        return true;
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// Recursive Add function.
        /// NOTE: Actual addition of node item happens at the
        /// outermost (ie - nodes having no children) level.
        /// </summary>
        /// <param name="bTree">Parent BTree</param>
        /// <param name="item">Item to add to the tree</param>
        /// <param name="parent"> </param>
        /// <throws>Exception if No Comparer or Mem Alloc err is encountered.</throws>
        protected internal bool Add(BTreeAlgorithm bTree, BTreeItemOnDisk item, BTreeNodeOnDisk parent = null)
        {
            BTreeNodeOnDisk currentNode = this;
            short index;
            while (true)
            {
                currentNode.SynchronizeCount();
                // Save this BTreeNodeOnDisk
                bool dupe;
                index = currentNode.GetIndex(bTree, item, out dupe);
                if (dupe)
                {
                    // set the Current item pointer to the duplicate item.
                    bTree.SetCurrentItemAddress(currentNode.GetAddress(bTree), index);
                    return false;
                }
                if (currentNode.ChildrenAddresses != null)
                {
                    parent = null;
                    // if not an outermost node let next lower level node do the 'Add'.
                    currentNode = currentNode.GetChild(bTree, index);
                    if (currentNode == null)
                        return false;
                }
                else
                    break;
            }
            if (bTree.IsUnique && currentNode.Count > 0)
            {
                var dupeIndex = index;
                if (index > 0 && index >= currentNode.Count)
                    dupeIndex--;
                if (Compare(bTree, currentNode.Slots[dupeIndex], item) == 0)
                {
                    // set the Current item pointer to the duplicate item.
                    bTree.SetCurrentItemAddress(currentNode.GetAddress(bTree), dupeIndex);
                    return false;
                }
            }
            currentNode.Add(bTree, item, index, parent);
            return true;
        }

        private void Add(BTreeAlgorithm bTree, BTreeItemOnDisk item, short index, BTreeNodeOnDisk parent)
        {
            // outermost node, the end of the recursive traversing thru all inner nodes of the Btree.. 
            // Correct Node is reached at this point!
            // if node is not yet full..
            if (Count < bTree.SlotLength)
            {
                // insert the Item in available slot
                ShiftSlots(Slots, index, Count);
                Slots[index] = item;
                Count++;
                //*** save this TreeNode and HeaderData
                SaveNodeToDisk(bTree);
                return;
            }

            // node is full, distribute or breakup the node (use temp slots in the process)...
            Slots.CopyTo(bTree.TempSlots, 0);

            // Index now contains the correct array element number to insert item into.
            ShiftSlots(bTree.TempSlots, index, bTree.SlotLength);
            bTree.TempSlots[index] = item;

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
        }

        /// <summary>
        /// Clear reinitialize this Node
        /// </summary>
        internal void Clear()
        {
            DiskBuffer.ClearData();
            ResetArray(Slots, null);
            this.ChildrenAddresses = null;
            this.Count = 0;
        }

        public void Dispose()
        {
            Dispose(true);
        }

        internal void Dispose(bool autoDisposeItem, bool forRecycling = false, bool disposeNodeShellOnly = false)
        {
            // no need to dispose if already disposed...
            if ((DiskBuffer == null || DiskBuffer.DataAddress == -1))
            {
                if (Count == 0)
                    return;
                Log.Logger.Instance.Log(Log.LogLevels.Warning, "Dispose: BTreeNodeOnDisk with null or no Data Address DiskBuffer was detected.");
            }
            if (Count <= 0)
            {
                Log.Logger.Instance.Log(Log.LogLevels.Warning, "Dispose: BTreeNodeOnDisk({0}) with Count = 0 was detected.", 
                    DiskBuffer.DataAddress);
                return;
            }
            for (int i = 0; i < Count; i++)
            {
                if (Slots[i] == null) continue;
                if (!disposeNodeShellOnly)
                {
                    // dispose the key if it is disposable
                    if (IsDisposable(autoDisposeItem, Slots[i].Key))
                    {
                        // lock in case key is used in another thread somewhere
                        DisposeItem(Slots[i].Key);
                    }
                    Slots[i].Key = null;
                    // dispose the value if it is disposable
                    if (Slots[i].Value != null && IsDisposable(autoDisposeItem, Slots[i].Value.Data))
                    {
                        // lock in case object is used in another thread somewhere
                        DisposeItem(Slots[i].Value.Data);
                    }
                    if (Slots[i].Value != null)
                    {
                        if (Slots[i].Value.diskBuffer != null)
                        {
                            Slots[i].Value.diskBuffer.Orphaned = true;
                            Slots[i].Value.diskBuffer = null;
                        }
                        Slots[i].Value.Data = null;
                        Slots[i].Value = null;
                    }
                }
                Slots[i] = null;
            }
            ChildrenAddresses = null;
            Count = 0;
            IsDirty = false;
            _indexOfNode = -1;
            ParentAddress = -1;
            if (DiskBuffer != null)
            {
                if (forRecycling)
                    DiskBuffer.Orphaned = true;
                DiskBuffer = null;
            }
            if (!forRecycling)
            {
                Slots = null;
            }
        }
        private void DisposeItem(object item)
        {
            lock (item)
            {
                if (item is ISortedDictionary)
                    ((ISortedDictionary)item).AutoDisposeItem = true;
                ((IDisposable)item).Dispose();
                if (item is SpecializedDataStore.SpecializedStoreBase)
                {
                    ((SpecializedDataStore.SpecializedStoreBase)item).Collection = null;
                }
            }
        }
        private bool IsDisposable(bool autoDisposeItem, object data)
        {
            if (!(data is IDisposable))
            {
                return false;
            }
            if (!(data is Client.ISortedDictionary))
            {
                return autoDisposeItem;
            }
            if (((Client.ISortedDictionary) data).IsDisposed)
            {
                if (!(data is SpecializedDataStore.SpecializedStoreBase)) return false;
                ((SpecializedDataStore.SpecializedStoreBase)data).Collection = null;
                return false;
            }
            if (data is SpecializedDataStore.SpecializedStoreBase)
                ((SpecializedDataStore.SpecializedStoreBase) data).Collection.Container = null;

            return ((Client.ISortedDictionary) data).AutoDispose;
        }

        private BTreeNodeOnDisk CreateNode(BTree.BTreeAlgorithm bTree, long parentAddress)
        {
            var n = (BTreeNodeOnDisk) bTree.MruManager.GetRecycledObject();
            if (n == null)
                n = new BTreeNodeOnDisk(bTree, parentAddress);
            else
                n.ParentAddress = parentAddress;
            return n;
        }

        /// <summary>
        /// Remove the current item from the tree
        /// </summary>
        /// <param name="bTree">Parent BTree</param>
        /// <returns>Always returns true</returns>
        protected internal bool Remove(BTree.BTreeAlgorithm bTree)
        {
            // check if there are children nodes.
            BTreeItemOnDisk deletedItem = null;
            BTreeNodeOnDisk nod = this;
            short index = bTree.CurrentItem.NodeItemIndex;
            if (ChildrenAddresses != null)
            {
                if (!bTree.IsDataInKeySegment)
                    deletedItem = Slots[index];
                // The below code allows the BTreeAlgorithm mngr to do virtually, all deletion to
                // happen in the outermost nodes' slots.
                MoveNext(bTree);
                // sure to succeed since Children nodes are always in pairs(left & right).
                // Make the new current item the occupant of the slot occupied by the deleted item.
                nod = bTree.CurrentItem.GetNode(bTree);
                Slots[index] = nod.Slots[bTree.CurrentItem.NodeItemIndex];
                SaveNodeToDisk(bTree);
                // Thus, the above code has the effect that the current item's slot is the deleted slot, 
                // so, the succeeding code that will remove the current slot will be fine..
            }
            else if (!bTree.IsDataInKeySegment)
                deletedItem = Slots[index];

            // delete the block from disk
            if (!bTree.IsDataInKeySegment)
            {
                bTree.RemoveItem(deletedItem);

                if (deletedItem.Key is IDisposable &&
                    bTree.AutoDisposeItem)
                    ((IDisposable) deletedItem.Key).Dispose();
                deletedItem.Key = null;

                if (deletedItem.Value.Data is IDisposable &&
                    bTree.AutoDisposeItem)
                    ((IDisposable) deletedItem.Value.Data).Dispose();

                deletedItem.Value.Data = null;
                deletedItem.Value.diskBuffer.Data = null;
                deletedItem.Value.diskBuffer.Next = null;
                deletedItem.Value.diskBuffer = null;
                deletedItem.Value = null;
            }
            // Always true since we expect the caller code to check if there is current item
            // to delete and therefore, every Delete call will succeed.
            return true;
        }

        /// <summary>
        /// Recursively pull item from left side. Modified to process unbalanced branch - 10/31/97.
        /// Pull an item from the left siblings. Used when this node run out of loaded items and 
        /// instead of destroying itself, will pull an item from the left siblings to maintain the balanceness of this tree branch
        /// </summary>
        /// <param name="bTree">Parent BTree</param>
        internal void PullFromLeft(BTree.BTreeAlgorithm bTree)
        {
            short i = Count;
            if (i > 1) // more than 1 item.
            {
                Count--;
                // we only need to nullify the last item since the caller code should have 
                // moved it to the slot, which item just got deleted or pulled.
                Slots[i - 1] = null;
                IsDirty = true;
                //SaveNodeToDisk(bTree);
                return;
            }
            // check if there is a left sibling and if it has children node.
            BTreeNodeOnDisk leftSibling = GetLeftSibling(bTree);
            BTreeNodeOnDisk parent;
            if (leftSibling != null && leftSibling.ChildrenAddresses != null)
            {
                parent = GetParent(bTree);
                //** Unbalanced left sibling branch processing
                i = (short)(GetIndexOfNode(bTree) - 1);
                Slots[0] = parent.Slots[i];
                IsDirty = true;
                //this.SaveNodeToDisk(bTree);
                bTree.SetCurrentItemAddress(parent.GetAddress(bTree), i);
                BTreeNodeOnDisk node = bTree.CurrentItem.GetNode(bTree);
                node.MovePrevious(bTree);
                node = bTree.CurrentItem.GetNode(bTree);
                node.IsDirty = true;
                //parent = GetParent(bTree);
                parent.Slots[i] = node.Slots[bTree.CurrentItem.NodeItemIndex];
                parent.IsDirty = true;
                //parent.SaveNodeToDisk(bTree);

                bTree.FixVacatedSlot = true;
                //if (Node.Count > 0)
                //    Node.SaveNodeToDisk(BTree);
                //else
                //    Logger.Instance.LogLine("Node is empty");

                //** End of Unbalanced right sibling branch processing
                return;
            }
            i = GetIndexOfNode(bTree);
            // There is only 1 item in the slot and there is no unbalanced left sibling.
            if (i == 1 && leftSibling.Count == 1)
            {
                parent = GetParent(bTree);
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
                leftSibling.Slots[1] = parent.Slots[0];
                leftSibling.Count = 2;
                leftSibling.IsDirty = true;
                //leftSibling.SaveNodeToDisk(bTree);
                i = parent.Count;
                MoveArrayElements(parent.Slots, 1, 0, (short)(i - 1));
                parent.Slots[i - 1] = null;
                parent.IsDirty = true;
                MoveArrayElements(parent.ChildrenAddresses, 2, 1, (short)(i - 1));
                parent.ChildrenAddresses[i] = -1;
                parent.Count--;
                parent.IsDirty = true;
                //parent.SaveNodeToDisk(bTree);
                //** remove this node block from disk
                bTree.RemoveFromCache(this);
                bTree.RemoveBlock(bTree.KeySet, this.DiskBuffer);

                //90;
                this.Dispose(false, true, true);

                return;
            }
            parent = GetParent(bTree);
            IsDirty = true;
            Slots[0] = parent.Slots[i - 1];
            //SaveNodeToDisk(bTree);
            parent.Slots[i - 1] = leftSibling.Slots[leftSibling.Count - 1];
            parent.IsDirty = true;
            //parent.SaveNodeToDisk(bTree);

            bTree.PullSibling = leftSibling;
            bTree.PullLeftDirection = true;
            //leftSibling.PullFromLeft(bTree);
        }

        /// <summary>
        /// Recursively pull item from right side. Modified to process unbalanced branch - 10/31/97
        /// Same as above except that the pull is from the right siblings.
        /// </summary>
        /// <param name="bTree">Paren BTree</param>
        internal void PullFromRight(BTree.BTreeAlgorithm bTree)
        {
            short i = Count;
            if (i > 1)
            {
                Count--;
                MoveArrayElements(Slots, 1, 0, Count);
                Slots[i - 1] = null;
                IsDirty = true;
                //SaveNodeToDisk(bTree);
            }
            else
            {
                // check if there is a right sibling and if it has children node.
                BTreeNodeOnDisk rightSibling = GetRightSibling(bTree);
                i = GetIndexOfNode(bTree);
                if (rightSibling != null && rightSibling.ChildrenAddresses != null)
                {
                    BTreeNodeOnDisk parent = GetParent(bTree);
                    // Unbalanced right sibling branch processing...
                    Slots[0] = parent.Slots[i];
                    IsDirty = true;
                    //SaveNodeToDisk(bTree);
                    bTree.SetCurrentItemAddress(parent.GetAddress(bTree), i);
                    BTreeNodeOnDisk node = bTree.CurrentItem.GetNode(bTree);
                    node.MoveNext(bTree);
                    node = bTree.CurrentItem.GetNode(bTree);
                    parent.Slots[i] = node.Slots[bTree.CurrentItem.NodeItemIndex];
                    parent.IsDirty = true;
                    //parent.SaveNodeToDisk(bTree);

                    bTree.FixVacatedSlot = true;
                    //Node.FixTheVacatedSlot(BTree);
                    node.IsDirty = true;
                    //if (Node.Count > 0)
                    //    Node.SaveNodeToDisk(BTree);

                    // End of Unbalanced right sibling branch processing
                }
                else
                {
                    BTreeNodeOnDisk parent = GetParent(bTree);
                    if (i == parent.Count - 1 && rightSibling.Count == 1)
                    {
                        // we need to combine the Rightmost sibling's item with the parent's last item and make them the rightmost node's items.
                        rightSibling.Slots[1] = rightSibling.Slots[0];
                        rightSibling.Slots[0] = parent.Slots[parent.Count - 1];
                        rightSibling.IsDirty = true;
                        parent.ChildrenAddresses[i] = rightSibling.GetAddress(bTree);
                        parent.ChildrenAddresses[i + 1] = -1;
                        parent.Slots[i] = null;
                        parent.Count--;
                        parent.IsDirty = true;
                        //parent.SaveNodeToDisk(bTree);
                        rightSibling.Count = 2;
                        rightSibling.IsDirty = true;
                        //rightSibling.SaveNodeToDisk(bTree);
                        //** remove this node block from disk
                        bTree.RemoveFromCache(this);
                        bTree.RemoveBlock(bTree.KeySet, this.DiskBuffer);

                        //90;
                        Dispose(false, true, true);

                    }
                    else
                    {
                        Slots[0] = parent.Slots[i];
                        IsDirty = true;
                        //SaveNodeToDisk(bTree);
                        parent.Slots[i] = rightSibling.Slots[0];
                        parent.IsDirty = true;
                        //parent.SaveNodeToDisk(bTree);

                        bTree.PullSibling = rightSibling;
                        bTree.PullLeftDirection = false;
                        //rightSibling.PullFromRight(bTree);
                    }
                }
            }
        }

        /// <summary>
        /// Overwrite the current item with the item from the next or previous slot.
        /// Attempts to free the TreeNode object by setting Parent, Children and Slots to null.
        /// </summary>
        /// <param name="bTree">Parent BTree</param>
        internal void FixTheVacatedSlot(BTree.BTreeAlgorithm bTree)
        {
            short c = Count;
            if (c > 1) // if there are more than 1 items in slot then..
            {
                //***** We don't fix the children since there are no children at this scenario.
                if (bTree.CurrentItem.NodeItemIndex < c - 1)
                    MoveArrayElements(Slots,
                                      (int) (bTree.CurrentItem.NodeItemIndex + 1),
                                      bTree.CurrentItem.NodeItemIndex,
                                      (short) (c - 1 - bTree.CurrentItem.NodeItemIndex));

                #region recycling block not used, may be removed later...
                //if (Slots[c - 2].Key != Slots[c - 1].Key)
                //{
                //    Mru.MruItem itm = (Mru.MruItem)BTree.MruManager.Remove(Slots[c - 1].Key, true);
                //    if (itm != null)
                //        BTree.MruManager.Recycle((IInternalPersistent)itm.Value);
                //}
                #endregion

                Count--;
                Slots[Count] = null; // nullify the last slot.
                IsDirty = true;
                //SaveNodeToDisk(bTree);
                return;
            }
            // only 1 item in slot
            if (ParentAddress != -1)
            {
                short ucIndex;
                // if there is a pullable item from sibling nodes.
                if (SearchForPullableItem(bTree, out ucIndex))
                {
                    short ion = GetIndexOfNode(bTree);
                    if (ion == -1)
                    {
                        bTree.RemoveFromCache(this);

                        BTreeNodeOnDisk thisNode = GetNode(bTree, GetAddress(bTree));
                        //bTree.RemoveBlock(bTree.KeySet, DiskBuffer);
                        //Dispose(false);
                        return;
                    }
                    if (ucIndex < ion)
                        PullFromLeft(bTree); // pull an item from left
                    else
                        PullFromRight(bTree); // pull an item from right

                    //if (Count > 0)
                    //  IsDirty = true;
                    //    SaveNodeToDisk(bTree);
                    return;
                }
                // Parent has only 2 children nodes
                BTreeNodeOnDisk parent = GetParent(bTree);
                BTreeNodeOnDisk[] c2 = parent.GetChildren(bTree);
                if (c2[0] == this ||
                    (DiskBuffer.DataAddress >= 0 &&
                     parent.ChildrenAddresses[0] == GetAddress(bTree)))
                {
                    // this is left node
                    BTreeNodeOnDisk rightSibling = GetRightSibling(bTree);
                    parent.Slots[1] = rightSibling.Slots[0];
                    parent.Count = 2;
                    parent.IsDirty = true;
                    bTree.RemoveFromCache(rightSibling);
                    bTree.RemoveBlock(bTree.KeySet, rightSibling.DiskBuffer);

                    //90;
                    rightSibling.Dispose(false, true, true);

                }
                else
                {
                    // this is right node
                    parent.Slots[1] = parent.Slots[0];
                    parent.Count = 2;
                    parent.IsDirty = true;
                    BTreeNodeOnDisk leftSibling = GetLeftSibling(bTree);
                    parent.Slots[0] = leftSibling.Slots[0];
                    bTree.RemoveFromCache(leftSibling);
                    //RemoveFromBTreeBlocksCache(BTree, LeftSibling);
                    bTree.RemoveBlock(bTree.KeySet, leftSibling.DiskBuffer);

                    //90;
                    leftSibling.Dispose(false, true, true);
                }
                bTree.RemoveFromCache(this);
                //RemoveFromBTreeBlocksCache(BTree, this);
                bTree.RemoveBlock(bTree.KeySet, this.DiskBuffer);
                //90;
                Dispose(false, true, true);
                parent.ChildrenAddresses = null;
                parent.IsDirty = true;
                //parent.SaveNodeToDisk(bTree);
                return;
            }
            // delete the single item in root node
            Count = 0;
            Slots[0] = null;
            IsDirty = true;
            //this.SaveNodeToDisk(bTree);
            bTree.SetCurrentItemAddress(-1, 0); // Point the current item pointer to end of tree
        }

        //****** end of modifed binary search functions
        /// <summary>
        /// Search for a pullable item from sibling nodes of this node. Modified for unbalanced branch's correct detection of pullable item. -10/31/97
        /// Find a pullable item. Will return true if there is one.
        /// </summary>
        /// <param name="bTree"> </param>
        /// <param name="index">Will be updated of the pullable item's index in the slot</param>
        /// <returns>true if there is pullable item, else false</returns>
        private bool SearchForPullableItem(BTree.BTreeAlgorithm bTree, out short index)
        {
            index = 0;
            BTreeNodeOnDisk parent = GetParent(bTree);
            BTreeNodeOnDisk[] children = null;
            bool r = true;
            if (parent.Count == 1)
            {
                index = (byte) (GetIndexOfNode(bTree) ^ 1);
                children = parent.GetChildren(bTree);
                r = children[index].ChildrenAddresses != null ||
                    (children[index] != null && children[index].Count > 1);
            }
            else
            {
                children = parent.GetChildren(bTree);
                for (byte i = 0; i <= bTree.SlotLength && children[i] != null; i++)
                {
                    if (this != children[i] &&
                        this.GetAddress(bTree) != children[i].GetAddress(bTree))
                    {
                        index = i; // pick one in case the below statement won't be true.
                        if (children[i].Count > 1)
                            break;
                    }
                }
            }
            return r;
        }

        /// <summary>
        /// Returns index of this node relative to parent. 
        /// Note: you must call this after you check that there is a parent node.
        /// </summary>
        /// <param name="bTree"> </param>
        /// <returns>Index of this node per its parent</returns>
        private short GetIndexOfNode(BTree.BTreeAlgorithm bTree)
        {
            BTreeNodeOnDisk parent = this.GetParent(bTree);
            if (parent != null)
            {
                long thisId = this.GetAddress(bTree);
                // Make sure we don't access an invalid memory address
                if (parent.ChildrenAddresses != null &&
                    (_indexOfNode == -1 || thisId != parent.ChildrenAddresses[_indexOfNode]))
                {
                    for (_indexOfNode = 0;
                         _indexOfNode <= bTree.SlotLength && parent.ChildrenAddresses[_indexOfNode] > 0;
                         _indexOfNode++)
                    {
                        if (parent.ChildrenAddresses[_indexOfNode] == thisId)
                        {
                            break;
                        }
                    }
                }
                return _indexOfNode;
            }
            // Just return 0 if called in the root node, anyway, 
            // the caller code should check if it is the root node and not call this function if it is!
            return 0;
        }

        private short _indexOfNode = -1;

        /// <summary>
        /// Search BTreeAlgorithm for the item pointed to by Item. 
        /// NOTE: this should be invoked from root node.
        /// </summary>
        /// <param name="bTree">BTree this Node is a branch of</param>
        /// <param name="item">Item to search in tree</param>
        /// <param name="goToFirstInstance">true tells BTree to go to First Instance of Key, else any key instance matching will match</param>
        /// <returns>true if item found, else false</returns>
        protected internal bool Search(BTree.BTreeAlgorithm bTree, object item, bool goToFirstInstance)
        {
            short i = 0;
            long foundNodeAddress = -1;
            BTreeNodeOnDisk currentNode = this;
            short foundIndex = 0;
            while (true)
            {
                i = 0;
                currentNode.SynchronizeCount();
                if (currentNode.Count > 0)
                {
                    int result;
                    if (bTree.Comparer != null)
                    {
                        if (!goToFirstInstance)
                            result = Array.BinarySearch(currentNode.Slots, 0, currentNode.Count, item,
                                                        bTree.ComparerWrapper);
                        else
                            result = BinarySearch(currentNode.Slots, 0, currentNode.Count, item, bTree.ComparerWrapper);
                    }
                    else
                    {
#if !DEVICE
                        try
                        {
                            result = goToFirstInstance ? BinarySearch(currentNode.Slots, 0, currentNode.Count, item, null) : 
                                Array.BinarySearch(currentNode.Slots, 0, currentNode.Count, item);
                        }
                        catch
                        {
#endif
                            try
                            {
                                if (goToFirstInstance)
                                    result = BinarySearch(currentNode.Slots, -1, -1, item, null);
                                else
                                    result = Array.BinarySearch(currentNode.Slots, item);
                            }
                            catch //(Exception innerE)
                            {
                                throw new InvalidOperationException("No Comparer Error.");
                            }
#if !DEVICE
                        }
#endif
                    }
                    if (result >= 0) // if found...
                    {
                        i = (short) result;
                        // Make this node the current node of the Owner Btree.
                        foundNodeAddress = currentNode.GetAddress(bTree);
                        foundIndex = i;
                        if (goToFirstInstance)
                        {
                            BTreeNodeOnDisk[] children = currentNode.GetChildren(bTree);
                            if (children != null)
                            {
                                currentNode = children[i];
                                continue;
                            }
                        }
                        else
                            break;
                    }
                    else
                        i = (short) ~result;
                }
                // not found, check if inner node(has Children nodes!).
                if (currentNode.ChildrenAddresses != null)
                {
                    // now, Search next lower level node
                    currentNode = currentNode.GetChild(bTree, i);
                    if (currentNode == null)
                        return false;
                }
                else
                    break;
            }
            if (foundNodeAddress >= 0)
            {
                bTree.SetCurrentItemAddress(foundNodeAddress, foundIndex);
                return true;
            }
            // this must be the outermost node
            // This block will make this item the current one to give chance to the Btree 
            // caller the chance to check the items having the nearest key to the one it is interested at.
            if (i == bTree.SlotLength) i--; // make sure i points to valid item
            if (currentNode.Slots[i] != null)
                bTree.SetCurrentItemAddress(currentNode.GetAddress(bTree), i);
            else
            {
                i--;
                // Update Current Item of this Node and nearest to the Key in sought Slot index
                bTree.SetCurrentItemAddress(currentNode.GetAddress(bTree), i);
                // Make the next item the current item. This has the effect of positioning making the next greater item the current item.
                currentNode.MoveNext(bTree);
                /*
						ItemAddress c = BTree.CurrentItem;
						c.Node = this;
						c.NodeItemIndex = i;
						*/
            }
            return false;
        }

        /// <summary>
        /// Make the first item the current item. This member should be called from Root.
        /// </summary>
        /// <param name="bTree">BTree instance this Node is a part of</param>
        protected internal bool MoveFirst(BTree.BTreeAlgorithm bTree)
        {
            if (Count > 0)
            {
                BTreeNodeOnDisk node = this;
                BTreeNodeOnDisk prev = null;
                while (node.ChildrenAddresses != null)
                {
                    prev = node;
                    long da = node.ChildrenAddresses[0];
                    node = GetNode(bTree, da);
                    if (node == null)
                        break;
                }
                if (node != null)
                    prev = node;
                bTree.SetCurrentItemAddress(prev.GetAddress(bTree), 0);
                return true;
            }
            return false;
        }

        internal static BTreeNodeOnDisk GetNode(BTree.BTreeAlgorithm bTree, long id)
        {
            if (bTree.RootNode.GetAddress(bTree) == id)
                return bTree.RootNode;
            return ReadNodeFromDisk(bTree, id);
        }

        protected internal bool MoveLast(BTree.BTreeAlgorithm bTree)
        {
            BTreeNodeOnDisk node = this;
            while (node.ChildrenAddresses != null)
            {
                long da = node.ChildrenAddresses[node.Count];
                node = GetNode(bTree, da);
            }
            bTree.SetCurrentItemAddress(node.GetAddress(bTree), (short) (node.Count - 1));
            return bTree.CurrentItem.NodeAddress != -1;
        }

        /// <summary>
        /// Make the next item in the tree the current item.
        /// </summary>
        /// <param name="bTree">Parent BTree</param>
        /// <returns>true if successful, else false</returns>
        protected internal bool MoveNext(BTree.BTreeAlgorithm bTree)
        {
            BTreeNodeOnDisk currentNode = this;
            short slotIndex = bTree.CurrentItem.NodeItemIndex;
            slotIndex++;
            bool goRightDown = ChildrenAddresses != null;
            if (goRightDown)
            {
                while (true)
                {
                    if (currentNode == null)
                    {
                        bTree.SetCurrentItemAddress(-1, 0);
                        return false;
                    }
                    if (currentNode.ChildrenAddresses != null)
                    {
                        currentNode = currentNode.GetChild(bTree, slotIndex);
                        slotIndex = 0;
                    }
                    else
                    {
                        // 'SlotIndex -1' since we are now using SlotIndex as index to pSlots.
                        bTree.SetCurrentItemAddress(currentNode.GetAddress(bTree), 0);
                        return true;
                    }
                }
            }
            while (true)
            {
                if (currentNode == null)
                {
                    bTree.SetCurrentItemAddress(-1, 0);
                    return false;
                }
                // check if SlotIndex is within the maximum slot items and if it is, will index an occupied slot.
                if (slotIndex < currentNode.Count)
                {
                    bTree.SetCurrentItemAddress(currentNode.GetAddress(bTree), slotIndex);
                    return true;
                }
                if (currentNode.ParentAddress != -1) // check if this is not the root node. (Root nodes don't have parent node.)
                {
                    slotIndex = currentNode.GetIndexOfNode(bTree);
                    currentNode = currentNode.GetParent(bTree);
                }
                else
                {
                    // this is root node. set to null the current item(End of Btree is reached)
                    bTree.SetCurrentItemAddress(-1, 0);
                    return false;
                }
            }
        }

        protected internal bool MovePrevious(BTree.BTreeAlgorithm bTree)
        {
            short slotIndex = bTree.CurrentItem.NodeItemIndex;
            bool goLeftDown = ChildrenAddresses != null;
            BTreeNodeOnDisk currentNode = this;
            if (goLeftDown)
            {
                while (true)
                {
                    if (currentNode.ChildrenAddresses != null)
                    {
                        BTreeNodeOnDisk c = currentNode.GetChild(bTree, slotIndex);
                        currentNode = c;
                        slotIndex = c.Count;
                    }
                    else
                    {
                        // 'SlotIndex -1' since we are now using SlotIndex as index to pSlots.
                        bTree.SetCurrentItemAddress(currentNode.GetAddress(bTree), (short) (slotIndex - 1));
                        return true;
                    }
                }
            }
            slotIndex--;
            while (true)
            {
                // check if SlotIndex is within the maximum slot items and if it is, will index an occupied slot.
                if (slotIndex >= 0)
                {
                    bTree.SetCurrentItemAddress(bTree.GetId(currentNode.DiskBuffer), slotIndex);
                    return true;
                }
                if (currentNode.ParentAddress != -1) // check if this is not the root node. (Root nodes don't have parent node.)
                {
                    short i = currentNode.GetIndexOfNode(bTree);
                    currentNode = currentNode.GetParent(bTree);
                    slotIndex = (short) (i - 1);
                }
                else
                {
                    // this is root node. set to null the current item(End of Btree is reached)
                    bTree.SetCurrentItemAddress(-1, 0);
                    return false;
                }
            }
        }

        /// <summary>
        /// This gets called when the node's slots are overflowed and break up
        ///	is needed. This does the necessary recursive promotion of the 
        ///	newly born nodes as affected by the break up.<br/>
        ///	Uses caller Btree object's Temporary Slots and Children nodes
        ///	which are accessible via GetTempSlot() and _GetTempParentChildren()
        ///	as temp storage of Parent and newly born siblings as nodes are 
        /// re-arranged.
        /// </summary>
        /// <param name="bTree">parent BTree</param>
        /// <param name="position">Position of the broken apart node in its parent node's slots</param>
        internal void Promote(BTree.BTreeAlgorithm bTree, short position)
        {

            short noOfOccupiedSlots = Count, index = position;
            IsDirty = true;
            if (noOfOccupiedSlots < bTree.SlotLength)
            {
                // node is not yet full.. insert the parent.
                ShiftSlots(Slots, index, noOfOccupiedSlots);
                if (index > noOfOccupiedSlots)
                    index = noOfOccupiedSlots;
                Slots[index] = bTree.TempParent;
                // insert the left child

                ChildrenAddresses[index] = bTree.TempParentChildren[(int) ChildNodes.LeftChild];
                // insert the right child
                ShiftSlots(ChildrenAddresses, (short) (index + 1), (short) (noOfOccupiedSlots + 1));
                Count++;

                ChildrenAddresses[index + 1] = bTree.TempParentChildren[(int) ChildNodes.RightChild];
                IsDirty = true;
                //SaveNodeToDisk(bTree);

                // successful
                return;
            }
            // *** Insert to temp slots.. node is full, use TempSlots

            // NOTE: ensure node & its children being promoted will point to the correct new ParentAddress as recursive node breakup occurs...

            CopyArrayElements(Slots, 0, bTree.TempSlots, 0, bTree.SlotLength);
            ShiftSlots(bTree.TempSlots, index, bTree.SlotLength);
            bTree.TempSlots[index] = bTree.TempParent;
            CopyArrayElements(ChildrenAddresses, 0, bTree.TempChildren, 0, (short) (bTree.SlotLength + 1));

            // insert the left child
            bTree.TempChildren[index] = bTree.TempParentChildren[(int) ChildNodes.LeftChild];
            // insert the right child
            ShiftSlots(bTree.TempChildren, (short) (index + 1), (short) (noOfOccupiedSlots + 1));
            bTree.TempChildren[index + 1] = bTree.TempParentChildren[(int) ChildNodes.RightChild];

            // *** Try to break up the node into 2 siblings.
            BTreeNodeOnDisk rightNode;
            short slotsHalf = (short) (bTree.SlotLength >> 1);
            if (ParentAddress != -1)
            {
                //** prepare this and the right node sibling and promote the temporary parent node(pTempSlot). 
                //** this is the left sibling !
                try
                {
                    //if (bTree.InMaintenanceMode)
                    //{
                    //    IsDirty = true;
                    //    //bTree.PromoteLookup.SetNode(bTree.GetId(DiskBuffer), this);
                    //}
                    rightNode = CreateNode(bTree, ParentAddress);
                    rightNode.ChildrenAddresses = new long[bTree.SlotLength + 1];
                    ResetArray(rightNode.ChildrenAddresses, -1);
                    // zero out the current slot.
                    ResetArray(Slots, null);
                    RemoveFromBTreeBlocksCache(bTree, this);
                    // zero out this children node pointers.
                    ResetArray(ChildrenAddresses, -1);
                    // copy the left half of the slots to left sibling(this)
                    CopyArrayElements(bTree.TempSlots, 0, Slots, 0, slotsHalf);
                    Count = slotsHalf;
                    // copy the right half of the slots to right sibling
                    CopyArrayElements(bTree.TempSlots, (short) (slotsHalf + 1), rightNode.Slots, 0, slotsHalf);
                    rightNode.Count = slotsHalf;
                    // copy the left half of the children nodes.
                    CopyArrayElements(bTree.TempChildren, 0, ChildrenAddresses, 0, (short) (slotsHalf + 1));

                    // copy the right half of the children nodes.
                    CopyArrayElements(bTree.TempChildren, (short) (slotsHalf + 1), rightNode.ChildrenAddresses, 0,
                                      (short) (slotsHalf + 1));

                    // make sure this node(leftNode)'s children has this node as parent...
                    //UpdateChildrenParent(bTree, this);

                    rightNode.SaveNodeToDisk(bTree);
                    // left sibling is already parent of its children. make the right sibling parent of its children.
                    UpdateChildrenParent(bTree, rightNode);

                    // copy the middle slot
                    bTree.TempParent = bTree.TempSlots[slotsHalf];
                    // assign the new children nodes.
                    bTree.TempParentChildren[(int) ChildNodes.LeftChild] = GetAddress(bTree);
                    bTree.TempParentChildren[(int) ChildNodes.RightChild] = rightNode.GetAddress(bTree);

                    IsDirty = true; 
                    //SaveNodeToDisk(bTree);

                    bTree.PromoteParent = GetParent(bTree);
                    bTree.PromoteIndexOfNode = GetIndexOfNode(bTree);
                    //Parent.Promote(BTree, GetIndexOfNode(BTree));
                    
                    return;
                }
                catch (Exception e)
                {
                    throw new Exception("Error in attempt to promote parent of a splitted node.", e);
                }
            }
            //** no parent, break up this node into two children & make this new root...
            long thisAddress = GetAddress(bTree);
            BTreeNodeOnDisk leftNode = CreateNode(bTree, thisAddress);
            rightNode = CreateNode(bTree, thisAddress);
            // copy the left half of the slots
            CopyArrayElements(bTree.TempSlots, 0, leftNode.Slots, 0, slotsHalf);
            leftNode.Count = slotsHalf;
            // copy the right half of the slots
            CopyArrayElements(bTree.TempSlots, (short) (slotsHalf + 1), rightNode.Slots, 0, slotsHalf);
            rightNode.Count = slotsHalf;
            leftNode.ChildrenAddresses = new long[bTree.SlotLength + 1];
            ResetArray(leftNode.ChildrenAddresses, -1);
            rightNode.ChildrenAddresses = new long[bTree.SlotLength + 1];
            ResetArray(rightNode.ChildrenAddresses, -1);
            // copy the left half of the children nodes.
            CopyArrayElements(bTree.TempChildren, 0, leftNode.ChildrenAddresses, 0, (short) (slotsHalf + 1));
            // copy the right half of the children nodes.
            CopyArrayElements(bTree.TempChildren, (short) (slotsHalf + 1),
                              rightNode.ChildrenAddresses, 0, (short) (slotsHalf + 1));

            // reset this Node...
            ResetArray(Slots, null);
            RemoveFromBTreeBlocksCache(bTree, this);

            //children = null;
            ResetArray(ChildrenAddresses, -1);

            leftNode.SaveNodeToDisk(bTree);
            // make the left sibling parent of its children.
            UpdateChildrenParent(bTree, leftNode);

            rightNode.SaveNodeToDisk(bTree);
            // make the right sibling parent of its children.
            UpdateChildrenParent(bTree, rightNode);

            // copy the middle slot
            Slots[0] = bTree.TempSlots[slotsHalf];
            this.Count = 1;
            // assign the new children nodes.

            ChildrenAddresses[(int) ChildNodes.LeftChild] = leftNode.GetAddress(bTree);
            ChildrenAddresses[(int) ChildNodes.RightChild] = rightNode.GetAddress(bTree);
            IsDirty = true;
            //SaveNodeToDisk(bTree);
            // successful
        }
        private void UpdateChildrenParent(BTreeAlgorithm bTree, BTreeNodeOnDisk node)
        {
            if (node.ChildrenAddresses != null)
            {
                BTreeNodeOnDisk[] children = node.GetChildren(bTree);
                long nodeAddress = node.GetAddress(bTree);
                // make the right sibling parent of its children.
                for (int index = 0; index < children.Length && children[index] != null; index++)
                {
                    children[index].ParentAddress = nodeAddress;
                    children[index].IsDirty = true;
                    //children[index].SaveNodeToDisk(bTree);
                }
            }
        }

        /// <summary>
        /// Distribute to left siblings the item if the current slots are all filled up.
        /// Used when balancing the nodes' load of the current sub-tree.
        /// </summary>
        /// <param name="item">Item to distribute to left sibling node</param>
        internal void DistributeToLeft(BTree.BTreeAlgorithm bTree, BTreeItemOnDisk item)
        {
            if (IsFull(bTree.SlotLength))
            {
                // counter-clockwise rotation..					
                //	----
                //	|  |
                //	-> |
                // NOTE: we don't check for null returns as this method is called only when there is vacant in left
                BTreeNodeOnDisk parent = GetParent(bTree);

                short indexOfNode = GetIndexOfNode(bTree);
                if (indexOfNode > parent.Count)
                    return;

                bTree.DistributeSibling = GetLeftSibling(bTree);
                bTree.DistributeItem = parent.Slots[indexOfNode - 1];
                bTree.DistributeLeftDirection = true;
                //BTreeNodeOnDisk leftSibling = GetLeftSibling(bTree);
                //leftSibling.DistributeToLeft(bTree, parent.Slots[indexOfNode - 1]);

                //*** Update Parent (remove node and add updated one).
                parent.Slots[indexOfNode - 1] = Slots[0];
                parent.IsDirty = true;
                //parent.SaveNodeToDisk(bTree);
                MoveArrayElements(Slots, 1, 0, (short) (bTree.SlotLength - 1));
            }
            else
                Count++;
            Slots[Count - 1] = item;
            IsDirty = true; 
            //SaveNodeToDisk(bTree);
        }

        /// <summary>
        /// Distribute to right siblings the item if the current slots are all filled up.
        /// Used when balancing the nodes' load of the current sub-tree.
        /// </summary>
        /// <param name="bTree"> </param>
        /// <param name="item">Item to distribute to right sibling</param>
        internal void DistributeToRight(BTree.BTreeAlgorithm bTree, BTreeItemOnDisk item)
        {
            if (IsFull(bTree.SlotLength))
            {
                // clockwise rotation..
                //	----
                //	|  |
                //	| <-
                BTreeNodeOnDisk parent = GetParent(bTree);
                int i = GetIndexOfNode(bTree);

                //IsDirty = true;
                bTree.DistributeSibling = GetRightSibling(bTree);
                bTree.DistributeItem = parent.Slots[i];
                bTree.DistributeLeftDirection = false;
                //GetRightSibling(bTree).DistributeToRight(bTree, parent.Slots[i]);

                parent.Slots[i] = Slots[Count - 1];
                parent.IsDirty = true;
                //parent.SaveNodeToDisk(bTree);
            }
            else
                this.Count++;
            ShiftSlots(Slots, 0, (short) (bTree.SlotLength - 1));
            Slots[0] = item;
            IsDirty = true; 
            //SaveNodeToDisk(bTree);
        }

        /// <summary>
        /// Returns left sibling or null if finished traversing left nodes.
        /// </summary>
        /// <returns>Left sibling BTreeNodeOnDisk reference</returns>
        private BTreeNodeOnDisk GetLeftSibling(BTree.BTreeAlgorithm bTree)
        {
            int index = GetIndexOfNode(bTree);
            BTreeNodeOnDisk p = GetParent(bTree);
            // if we are not at the leftmost sibling yet..
            if (index > 0 && p != null && index <= p.Count)
            {
                BTreeNodeOnDisk r = p.GetChild(bTree, (short) (index - 1));
                return r;
            }
            // leftmost was already reached..
            return null;
        }

        /// <summary>
        /// Returns right sibling or null if finished traversing right nodes.
        /// </summary>
        /// <returns>Right sibling BTreeNodeOnDisk reference</returns>
        private BTreeNodeOnDisk GetRightSibling(BTree.BTreeAlgorithm bTree)
        {
            int index = GetIndexOfNode(bTree);
            BTreeNodeOnDisk p = GetParent(bTree);
            if (p != null)
            {
                // if we are not at the Rightmost sibling yet..
                if (index < p.Count)
                {
                    BTreeNodeOnDisk r = p.GetChild(bTree, (short) (index + 1));
                    return r;
                }
            }
            // rightmost was already reached..
            return null;
        }

        /// <summary>
        /// Returns true if a slot is available in left side siblings of this node modified to suit possible unbalanced branch.
        /// </summary>
        /// <param name="bTree">Parent BTree</param>
        /// <param name="isUnBalanced">Will be updated to true if this branch is detected to be "unbalanced", else false</param>
        /// <returns>true if there is a vacant slot, else false</returns>
        private bool IsThereVacantSlotInLeft(BTree.BTreeAlgorithm bTree, ref bool isUnBalanced)
        {
            isUnBalanced = false;
            // start from this node.
            BTreeNodeOnDisk temp = this;
            while ((temp = temp.GetLeftSibling(bTree)) != null)
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

        public BTreeItemOnDisk[] Slots { get; set; }

        protected internal BTreeNodeOnDisk GetParent(BTree.BTreeAlgorithm bTree)
        {
            return this.ParentAddress == -1 ? null : GetNode(bTree, this.ParentAddress);
        }

        protected internal long ParentAddress = -1;

        //internal BTreeNodeOnDisk[] children = null;
        /// <summary>
        /// Reads the Children Nodes from Disk and return them in Array
        /// </summary>
        /// <param name="bTree"></param>
        /// <returns></returns>
        protected BTreeNodeOnDisk[] GetChildren(BTree.BTreeAlgorithm bTree)
        {
            BTreeNodeOnDisk[] children = null;
            if (this.ChildrenAddresses != null)
            {
                short slotLength = (short) this.Slots.Length;
                children = new BTreeNodeOnDisk[slotLength + 1];
                for (int i = 0; i < ChildrenAddresses.Length && ChildrenAddresses[i] > 0; i++)
                    children[i] = GetNode(bTree, ChildrenAddresses[i]);
            }
            return children;
        }

        /// <summary>
        /// Read the Child from Cache of from Disk
        /// </summary>
        /// <param name="bTree"></param>
        /// <param name="slotIndex"></param>
        /// <returns></returns>
        protected internal BTreeNodeOnDisk GetChild(BTree.BTreeAlgorithm bTree, short slotIndex)
        {
            if (ChildrenAddresses[slotIndex] == bTree.GetId(DiskBuffer))
            {
                Log.Logger.Instance.Log(Log.LogLevels.Fatal, "Child can't be of same Address as this Node's(Address={0}).", 
                    ChildrenAddresses[slotIndex]);
                return null;
            }
            return GetNode(bTree, ChildrenAddresses[slotIndex]);
        }

        internal long GetAddress(BTree.BTreeAlgorithm bTree)
        {
            return DiskBuffer != null ? bTree.GetId(DiskBuffer) : -1;
            //if (ValueDataBlockSetInfo != null)
            //    return ValueDataBlockSetInfo.DataBlockAddresses[0];
        }

        internal long[] ChildrenAddresses;
        //private readonly Logger _logger = Logger.Instance;
    }
}