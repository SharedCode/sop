using System;
using System.Collections;
using System.ComponentModel;
using System.Collections.Generic;
using Sop.Collections.BTree;
using Sop.Synchronization;

namespace Sop.Collections.Generic.BTree
{
    /// <summary>
    /// BTreeAlgorithm is the core BTree class wrapper and implements BTree Collection interface.
    /// B-Tree data structure and algorithm are implemented in <see>"TreeNode"
    ///                                                          <cref>BTreeAlgorithm.TreeNode</cref>
    ///                                                        </see> class
    /// </summary>
    internal partial class BTreeAlgorithm<TKey, TValue> // : IBTreeAlgorithm<TKey, TValue>
    {
        public BTreeAlgorithm(Comparer<TKey> comparer)
        {
            this.Comparer = comparer;
        }

        /// <summary>
        /// Constructor to use if you want to provide the number of slots per node of the tree
        /// </summary>
        /// <param name="slotLen">number of slots per node</param>
        public BTreeAlgorithm(byte slotLen)
        {
            Initialize(slotLen);
            Comparer = Comparer<TKey>.Default;
        }

        /// <summary>
        /// Constructor to use if you want to use default number of slots per node (6)
        /// </summary>
        public BTreeAlgorithm()
        {
            Initialize(DefaultSlotLength);
            Comparer = Comparer<TKey>.Default;
        }

        /// <summary>
        /// Constructor to use if you want to provide your own Comparer object that defines
        /// how your records will be sorted/arranged
        /// </summary>
        /// <param name="comparer">IComparer implementation that defines how records will be sorted</param>
        public BTreeAlgorithm(IComparer<TKey> comparer)
        {
            Initialize(DefaultSlotLength);
            if (comparer == null)
                comparer = Comparer<TKey>.Default;
            this.Comparer = comparer;
        }

        /// <summary>
        /// Constructor to use if you want to provide number of slots per node and your comparer object
        /// </summary>
        /// <param name="slotLen">Number of slots per node</param>
        /// <param name="comparer">compare object defining how records will be sorted</param>
        public BTreeAlgorithm(byte slotLen, IComparer<TKey> comparer)
        {
            Initialize(slotLen);
            if (comparer == null)
                comparer = Comparer<TKey>.Default;
            this.Comparer = comparer;
        }
        /// <summary>
        /// In-memory B-Tree default slot length.
        /// </summary>
        public const byte DefaultSlotLength = 12;

        internal BTreeAlgorithm(BTreeAlgorithm<TKey, TValue> bTree)
        {
            Initialize(bTree);
        }

        private const int MaxRecycleCount = 200;
        private readonly List<TreeNode> _recycledNodes = new List<TreeNode>(MaxRecycleCount);

        internal void AddRecycleNode(TreeNode node)
        {
            node.Clear(true);
            if (_recycledNodes.Count < MaxRecycleCount)
                _recycledNodes.Add(node);
        }

        internal TreeNode GetRecycleNode(TreeNode parent)
        {
            TreeNode r = null;
            if (_recycledNodes.Count == 0)
                r = new TreeNode(this, parent);
            else
            {
                r = _recycledNodes[0];
                _recycledNodes.RemoveAt(0);
                r.Initialize(this, parent);
            }
            return r;
        }

        /// <summary>
        /// Returns a shallow copy of this BTreeAlgorithm
        /// </summary>
        /// <returns>Value of type BTreeAlgorithm</returns>
        public object Clone()
        {
            return (object) new BTreeAlgorithm<TKey, TValue>(this);
        }

        /// <summary>
        /// Implement to copy items from source onto this instance.
        /// </summary>
        /// <param name="source"></param>
        public void Copy(BTreeAlgorithm<TKey, TValue> source)
        {
            if (source == null)
                throw new ArgumentNullException("source");
            if (source.MoveFirst())
            {
                for (; source.CurrentEntry != null; source.MoveNext())
                    Add(source.CurrentEntry.Key, source.CurrentEntry.Value);
            }
        }


        // Call this after the default constructor was invoked.
        private bool Initialize(byte bySlotLen)
        {
            if (bySlotLen < 2)
                throw new ArgumentOutOfRangeException("bySlotLen", "Slot Length needs to be >= 2");
            if (bySlotLen%2 != 0)
            {
                if (bySlotLen == byte.MaxValue)
                    bySlotLen--;
                else
                    bySlotLen++;
            }

#if (!DEBUG && TRIALWARE)
		            const string ExpireMsg = "BTreeGold trial period has expired.\nVisit 4A site(http://www.4atech.net) to get details in getting a license.";
		            if (!System.IO.File.Exists("Trialware.dll") || 
		                Trialware.ExpirationManager.Instance == null ||
		                Trialware.ExpirationManager.Instance.IsExpired())
		                throw new InvalidOperationException(ExpireMsg);
#endif

            this.SlotLength = bySlotLen;
            Root = new TreeRootNode(this);
            SetCurrentItemAddress(Root, 0);
            _tempSlots = new BTreeItem<TKey, TValue>[SlotLength + 1];
            _tempChildren = new TreeNode[SlotLength + 2];
            return true; // successful
        }

        /// <summary>
        /// Remove "Item" from the tree. Doesn't throw exception if "Item" is not found
        /// </summary>
        /// <param name="key"> </param>
        public bool Remove(TKey key) // return true if found, else false
        {
            if (Count > 0)
            {
                var item = new BTreeItem<TKey, TValue>(key, default(TValue));
                if (CurrentEntry == null)
                {
                    if (Root.Search(this, item, false))
                    {
                        Remove();
                        return true;
                    }
                }
                else if (Comparer.Compare(CurrentEntry.Key, key) == 0)
                {
                    Remove();
                    return true;
                }
                else
                {
                    if (Root.Search(this, item, false))
                    {
                        Remove();
                        return true;
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// Set to null all collected items and their internal buffers.
        /// </summary>
        public void Clear()
        {
            if (Root != null)
            {
                Root.Clear();
                SetCurrentItemAddress(null, 0);
                Root.TreeCount = 0;
                if (_tempChildren != null)
                    TreeNode.ResetArray(_tempChildren, null);
                _tempParent = null;
                if (_tempParentChildren != null)
                    TreeNode.ResetArray(_tempParentChildren, null);
                if (_tempSlots != null)
                    TreeNode.ResetArray(_tempSlots, null);
            }
        }

        /// <summary>
        /// Search btree for a certain record (Item). If current record is equal
        /// to Item then true will be returned without doing any search operation.
        /// This minimizes unnecessary BTree traversal. If Item is found, it becomes the current item.
        /// </summary>
        /// <param name="key"> </param>
        /// <returns>Returns true if found else, false</returns>
        public bool Search(TKey key)
        {
            return this.Search(key, false);
        }

        /// <summary>
        /// Search btree for a certain record (Item). If current record is equal
        /// to Item then true will be returned without doing any search operation.
        /// This minimizes unnecessary BTree traversal. If Item is found, it becomes the current item.
        /// </summary>
        /// <param name="key"> </param>
        /// <param name="goToFirstInstance">if true, will make first instance of duplicated keys the current record</param>
        /// <returns>Returns true if found else, false</returns>
        public bool Search(TKey key, bool goToFirstInstance)
        {
            if (Count > 0)
            {
                var o = CurrentEntry;
                if (o == null || Comparer.Compare(o.Key, key) != 0 ||
                    goToFirstInstance)
                {
                    var item = new BTreeItem<TKey, TValue>(key, default(TValue));
                    bool r = Root.Search(this, item, goToFirstInstance);
                    //TreeNode.ResetArray(_tempSlots, null);
                    _tempParent = null;
                    return r;
                }
                return true; // current entry is equal to ObjectToSearch!!
            }
            // tree is empty
            return false;
        }

        /// <summary>
        /// Go to 1st item of the tree
        /// </summary>
        /// <returns>returns true if successful, else false</returns>
        public bool MoveFirst()
        {
            return this.Count > 0 && Root.MoveFirst(this);
        }

        /// <summary>
        /// Go to next item of the tree
        /// </summary>
        /// <returns>Returns true if successful, else, false. Also returns false if Current record is null.</returns>
        public bool MoveNext()
        {
            if (
                !(CurrentItem.Node == null || CurrentItem.Node.Slots == null ||
                  CurrentItem.Node.Slots[CurrentItem.NodeItemIndex] == null))
                return CurrentItem.Node.MoveNext(this);
            return false;
        }

        /// <summary>
        /// Go to previous item of the tree
        /// </summary>
        /// <returns>Returns true if successful, else false</returns>
        public bool MovePrevious()
        {
            if (!(CurrentItem.Node == null || CurrentItem.Node.Slots == null ||
                  CurrentItem.Node.Slots[CurrentItem.NodeItemIndex] == null))
                return CurrentItem.Node.MovePrevious(this);
            return false;
        }

        /// <summary>
        /// Go to last item of the tree. If there is no item, returns false.
        /// </summary>
        /// <returns>Returns true if successful, else false</returns>
        public bool MoveLast()
        {
            return this.Count > 0 && Root.MoveLast(this);
        }

        /// <summary>
        /// Returns the number of collected items
        /// </summary>
        public int Count
        {
            get { return Root.TreeCount; }
        }

        /// <summary>
        /// Insert "Item" to the correct location in the tree. Tree is maintained to be balanced and sorted.
        /// Add and Delete methods cause Current Record to be invalid (set to null).
        /// </summary>
        /// <param name="key"> </param>
        /// <param name="value"> </param>
        public void Add(TKey key, TValue value)
        {
            Root.Add(this, new BTreeItem<TKey, TValue>(key, value));
            ProcessDistribution();
            while (_promoteParent != null)
            {
                TreeNode n = _promoteParent;
                _promoteParent = null;
                n.Promote(this, (byte) _promoteIndexOfNode);
            }
            _promoteIndexOfNode = 0;

            // Make the current item pointer point to null since we will add an item and addition to a
            // balanced Btree will re-arrange the slots and nodes thereby invalidating the current item pointer.
            // nullifying it is the simpler behavior. The higher level code will have to implement a different
            // approach to updating the current item pointer if it needs to.
            SetCurrentItemAddress(null, 0);
            Root.TreeCount++;
            TreeNode.ResetArray(_tempSlots, null);
            _tempParent = null;
        }

        // Needed for cloning (shallow copy) this BTree.
        private void Initialize(BTreeAlgorithm<TKey, TValue> bTree)
        {
            this.SlotLength = bTree.SlotLength;
            this.Comparer = bTree.Comparer;
            this.Root = new TreeRootNode(this);
            //this.Root = bTree.Root;
            //Copy CurrentItem. "Copy" as CurrentItem is value type.
            this.CurrentItem = bTree.CurrentItem;
            // create another set of temporary slots for thread safe 'Search' operation support
            _tempSlots = new BTreeItem<TKey, TValue>[SlotLength + 1];
            _tempChildren = new TreeNode[SlotLength + 2];

            // copy the tree graph.
            bTree.Locker.Invoke(() => { Copy(bTree); });
        }

        /// <summary>
        /// Returns current item, null if end of Btree.
        /// </summary>
        public BTreeItem<TKey, TValue> CurrentEntry
        {
            get
            {
                if (CurrentItem.Node != null)
                {
                    var o = CurrentItem.Node.Slots[CurrentItem.NodeItemIndex];
                    if (o != null) return o;
                    SetCurrentItemAddress(null, 0);
                }
                return null;
            }
        }

        /// <summary>
        /// Delete the current item from the tree. Tree is maintained to be balanced and sorted.
        /// </summary>
        protected internal void Remove()
        {
            BTreeItem<TKey, TValue> temp = null;
            if (CurrentItem.Node != null)
                temp = CurrentItem.Node.Slots[CurrentItem.NodeItemIndex];
            if (temp == null) return;
            CurrentItem.Node.Remove(this);
            ProcessFixAndPull();
            // Make the current item pointer point to null since we just deleted the current item. There is no efficient way to point the current item
            // pointer to point to the next or previous item. In BPlus this is possible but since this is not BPLus..
            SetCurrentItemAddress(null, 0);
            Root.TreeCount--;
            TreeNode.ResetArray(_tempSlots, null);
            _tempParent = null;
        }
        private void ProcessFixAndPull()
        {
            // looping equivalent of recursive FixVacatedSlot and Pullxxx
            bool pull;
            FixVacatedSlot = true;
            do
            {
                while (FixVacatedSlot)
                {
                    FixVacatedSlot = false;
                    CurrentItem.Node.FixTheVacatedSlot(this);
                }
                pull = false;
                while (PullSibling != null)
                {
                    pull = true;
                    var n = PullSibling;
                    PullSibling = null;
                    if (PullLeftDirection)
                        n.PullFromLeft(this);
                    else
                        n.PullFromRight(this);
                }
            } while (pull);
            // end
        }

        /// <summary>
        /// Get: returns the number of slots per node of all "TreeNodes"
        /// Set: assigns the number of slots per node of "TreeNodes"
        /// </summary>
        public byte SlotLength
        {
            get { return _slotLength; }
            set { _slotLength = value; }
        }

        public System.Collections.Generic.IComparer<TKey> Comparer
        {
            get { return _comparer; }
            set
            {
                _comparer = value;
                SlotsComparer = new BTreeSlotComparer<TKey, TValue>(value);
            }
        }

        /// <summary>
        /// This holds the Current Item Address (Current Node and Current Slot index)
        /// </summary>
        public TreeNode.ItemAddress CurrentItem
        {
            get { return _currentItem; }
            private set { _currentItem = value; }
        }
        TreeNode.ItemAddress _currentItem;

        /// <summary>
        /// This holds the Root Node (parentmost) of the TreeNodes
        /// </summary>
        internal TreeRootNode Root = null;

        /// <summary>
        /// Returns the object used for thread synchronization on multiple threads' access to this BTree object.
        /// </summary>
        public ISynchronizer Locker
        {
            get
            {
                return _syncRoot; 
            }
        }
        private readonly ISynchronizer _syncRoot = new Synchronizer<SynchronizerSingleReaderWriterBase>();

        public IComparer<BTreeItem<TKey, TValue>> SlotsComparer { get; private set; }

        /// <summary>
        /// Utility function to assign/replace current item w/ a new item.
        /// </summary>
        /// <param name="itemNode">node of the new item</param>
        /// <param name="itemIndex">slot index of the new item</param>
        public void SetCurrentItemAddress(TreeNode itemNode, byte itemIndex)
        {
            _currentItem.Node = itemNode;
            _currentItem.NodeItemIndex = itemIndex;
        }

        private IComparer<TKey> _comparer;
        private byte _slotLength = DefaultSlotLength;
        private BTreeItem<TKey, TValue>[] _tempSlots;
        private BTreeItem<TKey, TValue> _tempParent;
        // Temp Children nodes. Only 2 since only left & right child nodes will be handled.
        private TreeNode[] _tempChildren;
        private readonly TreeNode[] _tempParentChildren = new TreeNode[2];
    }
}