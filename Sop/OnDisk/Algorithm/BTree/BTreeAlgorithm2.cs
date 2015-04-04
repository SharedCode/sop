// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Sop.Collections.Generic.BTree;
using Sop.Mru;
using Sop.OnDisk.Algorithm.Collection;
using Sop.OnDisk.Algorithm.LinkedList;
using Sop.OnDisk.Algorithm.SortedDictionary;
using Sop.OnDisk.DataBlock;
using Sop.OnDisk.File;
using Sop.Persistence;
using Sop.SpecializedDataStore;
using System.Threading.Tasks;

namespace Sop.OnDisk.Algorithm.BTree
{
    /// <summary>
    /// B-Tree Algorithm collection class
    /// </summary>
    internal partial class BTreeAlgorithm
    {

        protected internal override void InternalDispose()
        {
            if (Comparer == null) return;
            Container = null;
            HintSequentialRead = false;
            //compareNode = null;
            // do cleanup, housekeeping...
            Comparer = null;

            base.InternalDispose();
            MruManager = null;
            _comparerWrapper = null;
            currentDataBlock = null;
            currentEntry = null;
            CurrentItem = null;
            if (DataSet != null)
            {
                DataSet.Dispose();
                DataSet = null;
            }
            if (KeySet != null)
            {
                KeySet.Dispose();
                KeySet = null;
            }
            PromoteLookup = null;
            RootNode = null;
            TempChildren = null;
            TempParent = null;
            TempParentChildren = null;
            TempSlots = null;
        }

        /// <summary>
        /// Initialize the tree's members preparing for use.
        /// </summary>
        /// <param name="file"></param>
        /// <param name="parameters"></param>
        protected internal override sealed void Initialize(File.IFile file, params KeyValuePair<string, object>[] parameters)
        {
            base.Initialize(file, parameters);

            // default to recommended slots given this PC's Disk Sector Size
            if (Comparer == null)
            {
                if (parameters != null && parameters.Length > 0)
                {
                    foreach (KeyValuePair<string, object> o in parameters)
                    {
                        var comparer = o.Value as IComparer;
                        if (comparer == null) continue;
                        Comparer = comparer;
                        break;
                    }
                }
            }
            if (Comparer == null)
                Comparer = new BTreeDefaultComparer();
            if (SlotLength == 0)
                this.SlotLength = file.Profile.BTreeSlotLength;

            if (DataSet == null || KeySet == null)
            {
                object f = GetParamValue(parameters, "DataBlockFactory");
                KeyValuePair<string, object>[] _Params;
                if (f != null)
                    _Params = new[]
                                  {
                                      new KeyValuePair<string, object>("HeaderData", null),
                                      new KeyValuePair<string, object>("DataBlockFactory", f)
                                  };
                else
                    _Params = new[]
                                  {
                                      new KeyValuePair<string, object>("HeaderData", null)
                                  };
                if (!IsDataInKeySegment)
                    Initialize(ref DataSet, _Params);
                _Params[0] = new KeyValuePair<string, object>("HeaderData", HeaderData);
                Initialize(ref KeySet, _Params);
            }

            if (IndexBlockSize == 0)
                this.IndexBlockSize = file.Profile.DataBlockSize;
            if (TempSlots == null)
                TempSlots = new BTreeItemOnDisk[SlotLength + 1];
            if (TempChildren == null)
                TempChildren = new long[SlotLength + 2];
            if (RootNode == null)
                RootNode = new BTreeNodeOnDisk(this);
            if (DataBlockSize == DataBlockSize.Unknown)
                DataBlockSize = file.DataBlockSize;
            MaxBlocks = file.Profile.MaxInMemoryBlockCount;
        }
        private void Initialize(ref LinkedListOnDisk dataSet,
                                KeyValuePair<string, object>[] parameters)
        {
            if (dataSet != null) return;
            dataSet = new SharedBlockOnDiskList(File, parameters) {Parent = this};
            dataSet.Initialize(File);
        }

        /// <summary>
        /// Read the Block with 'ID' from disk or virtual source.
        /// </summary>
        /// <param name="dataSet"> </param>
        /// <param name="id"></param>
        /// <returns></returns>
        internal protected virtual Sop.DataBlock GetDiskBlock(LinkedListOnDisk dataSet, long id)
        {
            return dataSet.DataBlockDriver.ReadBlockFromDisk(dataSet, id, false);
        }

        /// <summary>
        /// Calls GetDiskBlock for reading the Block to memory,
        /// add block to cache, and mark the items for load from disk upon access
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        protected internal override object OnRead(long address)
        {
            if (IsRoot(address))
                return RootNode;
            // read Node including Keys
            Sop.DataBlock d = GetDiskBlock(KeySet, address);

            var node = (BTreeNodeOnDisk) ReadFromBlock(d);
            if (node == null || RootNeedsReload)
                return node;

            // nullify DiskBuffer to save memory.
            node.DiskBuffer.Fold();

            // for data that are stored on data segments, delay their "fetch from disk" until the 1st access
            for (short i = 0; i < node.Count; i++)
            {
                BTreeItemOnDisk itm = node.Slots[i];
                if (!IsDataInKeySegment &&
                    itm.Value != null && GetId(itm.Value.DiskBuffer) > -1)
                    itm.ValueLoaded = false;
            }
            if (InMaintenanceMode)
            {
#if (DEBUG)
                if (MruManager.Contains(address))
                    Log.Logger.Instance.Error("BTreeAlgorithm.OnRead(address): node ({0}) found in MruManager.", address);
#endif
                PromoteLookup[address] = node;
            }
            else
                MruManager.Add(address, node);
            return node;
        }

        /// <summary>
        /// Save the serialized data on blocks to disk
        /// </summary>
        /// <returns></returns>
        protected override int SaveBlocks(bool clear)
        {
            return SaveBlocks(KeySet, 1, clear) + SaveBlocks(DataSet, 1, clear);
        }

        /// <summary>
        /// Tells whether Address references the Root Node
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        protected internal virtual bool IsRoot(long address)
        {
            return RootNode != null && address == RootNode.GetAddress(this);
        }

        /// <summary>
        /// Tells whether Node is a Root Node
        /// </summary>
        /// <param name="node"></param>
        /// <returns></returns>
        protected internal virtual bool IsRoot(BTreeNodeOnDisk node)
        {
            return RootNode != null &&
                   (node == RootNode ||
                    node.GetAddress(this) == RootNode.GetAddress(this));
        }

        /// <summary>
        /// true means data is saved in Key segment, otherwise on its own segment.
        /// </summary>
        public bool IsDataInKeySegment
        {
            get
            {
                if (_IsDataInKeySegment == null)
                    return IsDataLongInt;
                return _IsDataInKeySegment.Value;
            }
            set
            {
                if (_IsDataInKeySegment == value) return;
                if (Count > 0)
                    throw new InvalidOperationException(
                        "IsDataInKeySegment can only be set once when Collection is empty (Count = 0).");
                _IsDataInKeySegment = value;
            }
        }

        /// <summary>
        /// Gets invoked when MRU Maximum capacity of the tree is reached and 
        /// offloads to disk the least used data blocks in MRU
        /// </summary>
        /// <param name="countOfBlocksUnloadToDisk"></param>
        /// <returns></returns>
        public override int OnMaxCapacity(int countOfBlocksUnloadToDisk)
        {
            int ctr = 0;
            if (InMaintenanceMode)
                _onMaxBlocksCount = countOfBlocksUnloadToDisk;
            else
            {
                if (SaveState == SaveTypes.Default)
                {
                    //** retrieves the least used Objects in cache, persist them to disk
                    //** and shrink the MRU Cache to Minimum Capacity
                    SaveState |= SaveTypes.DataPoolInMaxCapacity;
                    MruItem m = MruManager.RemoveInTail(false);
                    while (m != null)
                    {
                        var node = (BTreeNodeOnDisk) m.Value;
                        if (!IsRoot(node) &&
                            (node.Slots == null ||
                             (node.Count == 1 &&
                              (node.Slots[0] == null || node.Slots[0].Value == null))))
                        {
                            if (node.DiskBuffer != null)
                                RemoveBlock(KeySet, node.DiskBuffer);
                            m.IndexToMruList = null;
                            m.Key = null;
                            m.Value = null;
                            m = MruManager.RemoveInTail(false);
                            continue;
                        }
                        bool serialized = OnMaxCapacity(node);
                        if (!IsRoot(node))
                        {
                            if (serialized && node.DiskBuffer != null)
                            {
                                ctr += node.DiskBuffer.CountMembers();
                            }
                            RemoveFromCache(node, true);
                            node.Dispose(AutoDisposeItem, true);
                        }
                        if (!(MruManager.Count > 0 && (Blocks.Count + ctr) < countOfBlocksUnloadToDisk))
                            break;
                        m.IndexToMruList = null;
                        m.Key = null;
                        m.Value = null;
                        m = MruManager.RemoveInTail(false);
                    }
                    ctr += Blocks.Count;
                    SaveState ^= SaveTypes.DataPoolInMaxCapacity;
                }
                if (ctr < countOfBlocksUnloadToDisk - 10)
                {
                    Log.Logger.Instance.Log(Log.LogLevels.Verbose, "OnMaxCapacity: calling base.OnMaxCapacity(countOfBlocksUnloadToDisk - ctr)");
                    ctr += base.OnMaxCapacity(countOfBlocksUnloadToDisk - ctr);
                }
                else if (Blocks.Count > 0)
                    SaveBlocks(true);
            }
            return ctr;
        }

        /// <summary>
        /// Assign an Address or ID to the disk block if DataAddress == 1.
        /// NOTE to Implementors: SetDiskBlock will be invoked even if DataAddress or ID had already been set,
        /// so you need to assign a new ID or Address only if the said field is -1.
        /// </summary>
        /// <param name="headBlock"></param>
        public virtual void SetDiskBlock(Sop.DataBlock headBlock)
        {
            DataBlockDriver.SetDiskBlock(this, headBlock, false);
        }

        /// <summary>
        /// Make DiskBuffer the Parent Collection's DiskBuffer
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="diskBuffer"></param>
        protected virtual void SetDiskBlock(Collection.ICollectionOnDisk parent, Sop.DataBlock diskBuffer)
        {
            DataBlockDriver.SetDiskBlock(parent, diskBuffer, false);
        }

        #region Packaging Event handler related

        public bool IsOnPackEventHandlerSet
        {
            get { return onValuePack != null; }
        }

        internal OnObjectPack onValuePack;
        internal OnObjectUnpack onValueUnpack;
        internal OnObjectPack onKeyPack;
        internal OnObjectPack onInnerMemberKeyPack;
        internal OnObjectUnpack onInnerMemberValueUnpack;
        internal OnObjectUnpack onKeyUnpack;

        public event OnObjectUnpack OnValueUnpack
        {
            add { onValueUnpack += value; }
            remove { onValueUnpack -= value; }
        }

        public event OnObjectPack OnValuePack
        {
            add { onValuePack += value; }
            remove { onValuePack -= value; }
        }

        public event OnObjectPack OnKeyPack
        {
            add { onKeyPack += value; }
            remove { onKeyPack -= value; }
        }

        public event OnObjectUnpack OnKeyUnpack
        {
            add { onKeyUnpack += value; }
            remove { onKeyUnpack -= value; }
        }

        public event OnObjectPack OnInnerMemberKeyPack
        {
            add { onInnerMemberKeyPack += value; }
            remove { onInnerMemberKeyPack -= value; }
        }

        internal OnObjectUnpack onInnerMemberKeyUnpack;

        public event OnObjectUnpack OnInnerMemberKeyUnpack
        {
            add { onInnerMemberKeyUnpack += value; }
            remove { onInnerMemberKeyUnpack -= value; }
        }

        internal OnObjectPack onInnerMemberValuePack;

        public event OnObjectPack OnInnerMemberValuePack
        {
            add { onInnerMemberValuePack += value; }
            remove { onInnerMemberValuePack -= value; }
        }

        public event OnObjectUnpack OnInnerMemberValueUnpack
        {
            add { onInnerMemberValueUnpack += value; }
            remove { onInnerMemberValueUnpack -= value; }
        }
        #endregion

        /// <summary>
        /// Delete the current item from the tree. Tree is maintained to be balanced and sorted.
        /// </summary>
        public void Remove()
        {
            if (HintSequentialRead)
                HintSequentialRead = false;
            BTreeItemOnDisk temp = null;
            BTreeNodeOnDisk node = null;
            bool needEnd = false;
            if (CurrentItem.NodeAddress != -1)
            {
                BeginTreeMaintenance();
                needEnd = true;
                node = CurrentItem.GetNode(this);
                temp = node.Slots[CurrentItem.NodeItemIndex];
            }
            if (temp != null)
            {
                node.Remove(this);
                FixAndPull();
                EndTreeMaintenance();
                SetCurrentItemAddress(-1, 0);
                UpdateCount(UpdateCountType.Decrement);
                if (Transaction != null)
                    ((Transaction.TransactionBase)Transaction).Register(
                        Sop.Transaction.ActionType.Remove, this, 0, 0);
                RegisterChange(true);
                if (temp.Value != null && temp.Value.Data is IDisposable && AutoDisposeItem)
                    ((IDisposable)temp.Value.Data).Dispose();
                BTreeNodeOnDisk.ResetArray(TempSlots, null);
                TempParent = null;
            }
            else
            {
                if (needEnd) EndTreeMaintenance();
                throw new InvalidOperationException(
                    "There is no CurrentItem to remove, use Move or Search functions to select an Item for removal."
                    );
            }
        }

        #region Remove's load distribution
        private void FixAndPull()
        {
            bool pull;
            FixVacatedSlot = true;
            // looping equivalent of recursive FixVacatedSlot and Pullxxx
            do
            {
                while (FixVacatedSlot)
                {
                    FixVacatedSlot = false;
                    var r = CurrentItem.GetNode(this);
                    r.FixTheVacatedSlot(this);
                }
                pull = false;
                while (PullSibling != null)
                {
                    pull = true;
                    BTreeNodeOnDisk n = PullSibling;
                    PullSibling = null;
                    if (PullLeftDirection)
                        n.PullFromLeft(this);
                    else
                        n.PullFromRight(this);
                }
            } while (pull);
            // end
        }
        internal BTreeNodeOnDisk PullSibling;
        internal bool PullLeftDirection;
        #endregion

        /// <summary>
        /// RemoveBlock is used for deleting a Node or a Node item's datablock(s).
        /// </summary>
        /// <param name="dataSet"></param>
        /// <param name="d"></param>
        /// <returns></returns>
        internal bool RemoveBlock(LinkedListOnDisk dataSet, Sop.DataBlock d)
        {
            if (d == null || dataSet == null) return false;
            if (d.DataAddress < 0 || (RootNode.DiskBuffer == d || RootNode.GetAddress(this) == d.DataAddress)) return false;
            long id = GetId(d);
            currentDataBlock = null;
            PromoteLookup.Remove(id);
            return dataSet.DataBlockDriver.Remove(dataSet, d);
        }

        // Serialize Node to the Block Set buffer...
        private bool OnMaxCapacity(BTreeNodeOnDisk node)
        {
            if (node.IsDirty)
            {
                // save Items' Data
                for (short i = 0; i < node.Count; i++)
                {
                    BTreeItemOnDisk itm = node.Slots[i];
                    if (!IsDataInKeySegment)
                    {
                        // if Data is not saved in Key Segment...
                        if (itm != null && itm.IsDirty)
                        {
                            if (GetId(itm.Value.DiskBuffer) == -1 || itm.Value.IsDirty)
                            {
                                bool flushed = false;
                                var fileEntity = itm.Value.Data as IFileEntity;
                                if (fileEntity != null)
                                {
                                    fileEntity.Flush();
                                    flushed = true;
                                }
                                if (!flushed)
                                {
                                    var fileSetEntity = itm.Value.Data as Sop.OnDisk.File.IFileSet;
                                    if (fileSetEntity != null)
                                    {
                                        fileSetEntity.Flush();
                                        flushed = true;
                                    }
                                }
                                if (!flushed)
                                {
                                    var sortedDict = itm.Value.Data as ISortedDictionary;
                                    if (sortedDict != null && !sortedDict.IsDisposed &&
                                        sortedDict.RealObject is SortedDictionaryOnDisk &&
                                        ((SortedDictionaryOnDisk)sortedDict.RealObject).DataAddress == -1)
                                    {
                                        sortedDict.Flush();
                                        flushed = true;
                                    }
                                }
                                if (!flushed)
                                {
                                    var sortedDictOnDisk = itm.Value.Data as Sop.ISortedDictionaryOnDisk;
                                    if (sortedDictOnDisk != null && !((SortedDictionaryOnDisk)sortedDictOnDisk).IsDisposed &&
                                        sortedDictOnDisk.DataAddress == -1)
                                    {
                                        sortedDictOnDisk.Flush();
                                        flushed = true;
                                    }
                                }
                                //var specialStore = itm.Value.Data as SpecializedDataStore.SpecializedStoreBase;
                                //if (specialStore != null && !((SortedDictionaryOnDisk)specialStore.Collection).IsDisposed &&
                                //    specialStore.Collection.DataAddress == -1)
                                //    sortedDict.Flush();

                                SetBigDataValue(itm);
                            }
                            else
                            {
                                itm.Value.IsDirty = false;
                                itm.IsDirty = false;
                            }
                        }
                    }
                }

                node.DiskBuffer.Unfold(this);
                // save Node including Keys
                // update Node to save references to data addresses assigned above.
                WriteToBlock(node);
                DataBlockDriver.SetDiskBlock(KeySet, node.DiskBuffer, false, false);
                //node.DiskBuffer.ProcessHeadSets();
                AddToBlocks(node.DiskBuffer, Blocks);

                // fold blocks to conserve memory...
                node.DiskBuffer.Fold();

                node.IsDirty = false;
                return true;
            }
            return false;
        }

        /// <summary>
        /// MRU on max capacity overload. This version is invoked during Low RAM
        /// situation and collections are given chance to free up cache to increase
        /// available RAM once again.
        /// </summary>
        public override void OnMaxCapacity()
        {
            SaveBlocks(false);
        }

        /// <summary>
        /// MRU on max capacity overload. This version is invoked in cases code
        /// knows which set of Nodes can be offloaded from memory
        /// </summary>
        /// <param name="nodes"></param>
        /// <returns></returns>
        public override int OnMaxCapacity(IEnumerable nodes)
        {
            Log.Logger.Instance.Log(Log.LogLevels.Verbose, "BTreeAlgorithm.OnMaxCapacity(IEnumerable nodes) {0}: Enter.", Name);

            if (InMaintenanceMode)
            {
                foreach (BTreeNodeOnDisk node in nodes)
                    PromoteLookup[node.GetAddress(this)] = node;
                if (_onMaxBlocksCount == 0)
                    _onMaxBlocksCount++;
                Log.Logger.Instance.Log(Log.LogLevels.Verbose, "BTreeAlgorithm.OnMaxCapacity(IEnumerable nodes) {0}: Exit 1.", Name);
                return 0;
            }
            if ((SaveState & SaveTypes.CollectionSave) == SaveTypes.CollectionSave ||
                SaveState == SaveTypes.Default)
            {
                int r = 0;
                SaveState |= SaveTypes.DataPoolInMaxCapacity;
                SaveTypes savingObjects = SaveState;
                if (!(nodes is ICollection<BTreeNodeOnDisk>) &&
                    !(nodes is Collections.Generic.SortedDictionary<object, object>))    // Collections.BTree.BTree))
                {
                    foreach (object o in nodes)
                    {
                        if (o is Sop.DataBlock)
                        {
                            var b3 = new Collections.Generic.SortedDictionary<long, Sop.DataBlock>();
                            foreach (Sop.DataBlock db in nodes)
                                AddToBlocks(db, b3);
                            WriteBlocksToDisk(KeySet, b3, false);
                            SaveState ^= SaveTypes.DataPoolInMaxCapacity;
                            Log.Logger.Instance.Log(Log.LogLevels.Verbose, "BTreeAlgorithm.OnMaxCapacity(IEnumerable nodes) {0}: Exit 2.", Name);
                            return b3.Count;
                        }
                        break;
                    }
                }
                int ctr = 0;
                foreach (BTreeNodeOnDisk node in nodes)
                {
                    ctr++;
                    if (node.Slots == null ||
                        (node.Count == 1 &&
                         (node.Slots[0] == null || node.Slots[0].Value == null)))
                    {
                        if (node.DiskBuffer != null)
                            RemoveBlock(KeySet, node.DiskBuffer);
                        Log.Logger.Instance.Log(Log.LogLevels.Warning, "An empty Node (Address={0}) was detected.", node.GetAddress(this));
                        continue;
                    }
                    bool serialized = OnMaxCapacity(node);
                    if ((savingObjects & SaveTypes.CollectionSave) != SaveTypes.CollectionSave)
                    {
                        if (node.DiskBuffer != null)
                        {
                            if (!serialized)
                                r += node.DiskBuffer.CountMembers();
                            RemoveFromCache(node, true);
                            if (!IsRoot(node))
                                node.Dispose(AutoDisposeItem, true);
                        }
                    }
                }
                if ((ctr == 1 && Blocks.Count > MruManager.MaxCapacity - MruManager.MinCapacity) ||
                    ctr > 1 && Blocks.Count > 0)
                    r += SaveBlocks(false);
                SaveState ^= SaveTypes.DataPoolInMaxCapacity;
                Log.Logger.Instance.Log(Log.LogLevels.Verbose, "BTreeAlgorithm.OnMaxCapacity(IEnumerable nodes) {0}: Exit 3.", Name);
                return r;
            }
            Log.Logger.Instance.Log(Log.LogLevels.Verbose, "BTreeAlgorithm.OnMaxCapacity(IEnumerable nodes) {0}: Exit 4.", Name);
            return 0;
        }

        /// <summary>
        /// Save Nodes to disk/virtual destination
        /// </summary>
        /// <param name="nodes"></param>
        internal virtual void SaveNode(Collections.Generic.ISortedDictionary<long, BTreeNodeOnDisk> nodes)
        {
            if (!nodes.MoveFirst()) return;
            //bool f = MruManager.GeneratePruneEvent;
            //MruManager.GeneratePruneEvent = false;
            BTreeNodeOnDisk lastNode = null, currentNode = nodes.CurrentValue;
            while(!nodes.EndOfTree())
            {
                lastNode = currentNode;
                if (!nodes.MoveNext())
                    break;
                else
                    SaveNode(lastNode);
                currentNode = nodes.CurrentValue;
            }
            //MruManager.GeneratePruneEvent = f;
            // generate prune event upon save of last node...
            if (lastNode != null)
                SaveNode(lastNode);
        }

        /// <summary>
        /// Save Node to disk/virtual destination
        /// </summary>
        /// <param name="node"></param>
        /// <param name="addToMru">true will add to cache the Node, else will not</param>
        internal virtual void SaveNode(BTreeNodeOnDisk node, bool addToMru = true)
        {
            if (IsRoot(node) && addToMru)
                addToMru = false;
            long diskBuffId = node.GetAddress(this);
            if (diskBuffId >= 0 &&
                (InMaintenanceMode || SaveState == SaveTypes.Default)
                )
            {
                if (addToMru)
                    MruManager.Add(diskBuffId, node);
            }
            else if (node.IsDirty)
            {
                //** save Node including Keys
                if (node.GetAddress(this) == -1)
                    OnMaxCapacity(node);
                if (addToMru)
                    MruManager.Add(node.GetAddress(this), node);
            }
        }

        /// <summary>
        /// Begin Tree Maintenance mode.
        /// </summary>
        /// <returns></returns>
        protected int BeginTreeMaintenance()
        {
            if (++_maintenanceCallCount == 1)
            {
                if (!InMaintenanceMode)
                {
                    InMaintenanceMode = true;
                    _onMaxBlocksCount = 0;
                }
            }
            return _maintenanceCallCount;
        }

        /// <summary>
        /// End Tree Maintenance mode.
        /// </summary>
        /// <returns></returns>
        protected int EndTreeMaintenance()
        {
            if (_maintenanceCallCount > 0 && --_maintenanceCallCount == 0)
            {
                InMaintenanceMode = false;
                if (PromoteLookup.Count > 0)
                {
                    if (_onMaxBlocksCount > 0)
                    {
                        if (_onMaxBlocksCount == 1)
                        {
                            Log.Logger.Instance.Log(Log.LogLevels.Information, "EndTreeMaintenance: calling OnMaxCapacity(PromoteLookup.Values).");
                            OnMaxCapacity(PromoteLookup.Values);
                        }
                        else
                        {
                            var pl = PromoteLookup;
                            foreach (KeyValuePair<long, BTreeNodeOnDisk> item in pl)
                                MruManager.Add(item.Key, item.Value);
                            Log.Logger.Instance.Log(Log.LogLevels.Information, "EndTreeMaintenance: calling OnMaxCapacity(_onMaxBlocksCount).");
                            OnMaxCapacity(_onMaxBlocksCount);
                        }
                        ClearPromoteLookup();
                        _onMaxBlocksCount = 0;
                    }
                    else
                    {
                        if (PromoteLookup.Count >= MruManager.MinCapacity)
                        {
                            Log.Logger.Instance.Log(Log.LogLevels.Information, "EndTreeMaintenance: calling SaveNode(PromoteLookup).");
                            SaveNode(PromoteLookup);
                            ClearPromoteLookup();
                            if (Blocks.Count >= MaxBlocks)
                                Log.Logger.Instance.Log(Log.LogLevels.Information, "EndTreeMaintenance: calling SaveBlocks(MaxBlocks, false).");
                        }
                        if (Blocks.Count >= MaxBlocks)
                            SaveBlocks(MaxBlocks, false);
                    }
                }
                else if (Blocks.Count >= MaxBlocks)
                {
                    Log.Logger.Instance.Log(Log.LogLevels.Information, "EndTreeMaintenance: PromoteLookup.Count = 0, calling SaveBlocks(MaxBlocks, false).");
                    SaveBlocks(MaxBlocks, false);
                }
                TempParent = null;
            }
            return _maintenanceCallCount;
        }

        private void ClearPromoteLookup()
        {
            PromoteLookup.Clear();
        }
        
        internal bool InMaintenanceMode = false;
        internal Collections.Generic.ISortedDictionary<long, BTreeNodeOnDisk> PromoteLookup =
            new Collections.Generic.SortedDictionary<long, BTreeNodeOnDisk>();

        /// <summary>
        /// Address on disk/virtual store of this B-Tree
        /// </summary>
        public override long DataAddress
        {
            get { return DiskBuffer.DataAddress; }
            set { DiskBuffer.DataAddress = value; }
        }

        /// <summary>
        /// true means this B-Tree can only contain Key/Value pair 
        /// entries with unique keys.
        /// </summary>
        public bool IsUnique { get; set; }

        /// <summary>
        /// Query the B-Tree for each Keys submitted, retrieve their values
        /// and store them in the array out parameter Values.
        /// </summary>
        /// <param name="items"></param>
        /// <param name="results"></param>
        /// <returns>true if found at least one key, otherwise false</returns>
        public bool Query(QueryExpression[] items, out QueryResult[] target)
        {
            if (items == null)
                throw new ArgumentNullException("items");
            var results = target = new QueryResult[items.Length];
            if (items.Length == 0) return false;
            var batchedIDs = new Collections.Generic.SortedDictionary<long, KeyValuePair<BTreeItemOnDisk, int>>();
            bool r = false;
            BeginTreeMaintenance();

            // applicable cases for doing bulk read:
            // - no Value filter anonymous function
            // - BTree Keys are unique

            bool valueFilterFuncDefined = false;
            if (!IsUnique)
            {
                for (int i = 0; i < items.Length; i++)
                {
                    if (items[i].Key == null) continue;
                    if (items[i].ValueFilterFunc != null)
                    {
                        valueFilterFuncDefined = true;
                        break;
                    }
                }
            }
            if (IsUnique || !valueFilterFuncDefined)
            {
                #region Fast, Bulk read 
                var itemsForBulkRead = new List<KeyValuePair<int, BTreeItemOnDisk>>(items.Length);
                for (int i = 0; i < items.Length; i++)
                {
                    if (items[i].Key == null) continue;
                    if (!Search(items[i].Key, valueFilterFuncDefined)) continue;
                    results[i].Found = r = true;
                    var entry = (BTreeItemOnDisk)this.CurrentEntry;
                    if (!entry.ValueLoaded || entry.IsDisposed)
                    {
                        itemsForBulkRead.Add(new KeyValuePair<int,BTreeItemOnDisk>(i, entry));
                    }
                    else
                    {
                        results[i].Value = entry.Value.Data;
                    }
                }
                if (itemsForBulkRead.Count > 0)
                {
                    List<BTreeItemOnDisk> itemsList = new List<BTreeItemOnDisk>(itemsForBulkRead.Count);
                    foreach(var o in itemsForBulkRead)
                    {
                        itemsList.Add(o.Value);
                    }
                    System.Func<int, bool> readCallback = (index) =>
                    {
                        var resultIndex = itemsForBulkRead[index].Key;
                        var o = GetValue(itemsForBulkRead[index].Value);
                        results[resultIndex].Value = o;
                        return true;
                    };
                    DataBlockDriver.ReadBlockFromDisk(DataSet, itemsList, readCallback);
                }
                #endregion
            }
            else
            {
                #region not so Fast, non-bulk read
                for (int i = 0; i < items.Length; i++)
                {
                    if (items[i].Key == null) continue;
                    valueFilterFuncDefined = items[i].ValueFilterFunc != null;
                    if (!Search(items[i].Key, valueFilterFuncDefined)) continue;
                    if (valueFilterFuncDefined)
                    {
                        do
                        {
                            var cv = CurrentValue;
                            if (!items[i].ValueFilterFunc(cv)) continue;
                            results[i].Found = true;
                            results[i].Value = cv;
                            break;
                        } while (MoveNext() && Comparer.Compare(CurrentKey, items[i].Key) == 0);
                        if (!results[i].Found)
                            continue;
                    }
                    else
                    {
                        results[i].Value = CurrentValue;
                        results[i].Found = true;
                    }
                    r = true;
                }
                #endregion
            }
            EndTreeMaintenance();
            return r;
        }


        /// <summary>
        /// Returns the Root Node of this B-Tree
        /// </summary>
        public BTreeNodeOnDisk RootNode { get; protected set; }


        /// <summary>
        /// Create a shallow copy of this tree for iteration purposes.
        /// Data on disk are not cloned, 'just a copy of this tree for use
        /// iteration us created.
        /// </summary>
        /// <returns></returns>
        public object Clone()
        {
            var r = new BTreeAlgorithm
                        {
                            Name = this.Name,
                            DataBlockDriver = this.DataBlockDriver,
                            DataBlockSize = this.DataBlockSize,
                            IndexBlockSize = this.IndexBlockSize,
                            IsDirty = IsDirty,
                            IsDataLongInt = this.IsDataLongInt,
                            PersistenceType = this.PersistenceType,
                            _IsDataInKeySegment = this.IsDataInKeySegment,
                            onKeyPack = onKeyPack,
                            onKeyUnpack = onKeyUnpack,
                            onValuePack = onValuePack,
                            onValueUnpack = onValueUnpack
                        };
            r.DataBlockSize = DataBlockSize;
            r.HintKeySizeOnDisk = HintKeySizeOnDisk;
            r.HintSizeOnDisk = HintSizeOnDisk;
            r.HintValueSizeOnDisk = HintValueSizeOnDisk;
            r.CustomBlockAddress = CustomBlockAddress;
            r.CurrentItem.NodeAddress = CurrentItem.NodeAddress;
            r.CurrentItem.NodeItemIndex = CurrentItem.NodeItemIndex;
            r._slotLength = this.SlotLength;
            r.Comparer = this.Comparer;
            r._comparerWrapper = this._comparerWrapper;
            r.currentDataBlock = this.currentDataBlock;
            r.currentEntry = this.currentEntry;
            SetId(r.DiskBuffer, GetId(this.DiskBuffer));
            r.DataAddress = this.DataAddress;
            r.MruManager = this.MruManager;
            r.File = this.File;
            r.IsCloned = true;
            r.RootNode = RootNode;
            if (DataSet != null)
                r.DataSet = (LinkedListOnDisk) this.DataSet.Clone();

            r.KeySet = (LinkedListOnDisk) this.KeySet.Clone();
            int systemDetectedBlockSize;
            r.FileStream = File.UnbufferedOpen(out systemDetectedBlockSize);
            r.isOpen = IsOpen;

            if (DataSet != null)
                r.DataSet.HeaderData = DataSet.HeaderData;
            r.KeySet.HeaderData = HeaderData;

            r.MruMinCapacity = MruMinCapacity;
            r.MruMaxCapacity = MruMaxCapacity;

            r.TempSlots = TempSlots;
            r.TempChildren = TempChildren;
            r.Blocks = Blocks;
            r.PromoteLookup = PromoteLookup;
            return r;
        }

        /// <summary>
        /// RemoveItem deletes the item's storage blocks on disk.
        /// These blocks go to recycling bin and are merged with other 
        /// deleted blocks to form a big segment that can be recycled.
        /// 
        /// Recycling bigger data segments prevents fragmentation thus,
        /// prevents or minimizes fragmentation in SOP's data files.
        /// </summary>
        /// <param name="deletedItem"></param>
        internal void RemoveItem(BTreeItemOnDisk deletedItem)
        {
            if (GetId(deletedItem.Value.DiskBuffer) >= 0)
            {
                bool removedFromBlocksCache = false;
                if (!deletedItem.Value.diskBuffer.IsFolded)
                {
                    var d = deletedItem.Value.diskBuffer;
                    removedFromBlocksCache = true;
                    while(d != null)
                    {
                        long l = GetId(d);
                        if (l >= 0)
                            Blocks.Remove(l);
                        d = d.Next;
                    }
                    deletedItem.Value.diskBuffer.Fold();
                }
                if (deletedItem.Value.diskBuffer.IsFolded)
                {
                    if (DataSet != null)
                    {
                        foreach(var bi in deletedItem.Value.diskBuffer.foldedDataAddresses)
                        {
                            _deletedDataBlocks.Add(bi.Address, bi.BlockCount);
                        }
                    }
                    if (!removedFromBlocksCache)
                    {
                        // remove the deletedItem Value's DiskBuffer blocks from this Tree's Blocks cache...
                        foreach (var bi in deletedItem.Value.diskBuffer.foldedDataAddresses)
                        {
                            var l = bi.Address;
                            for (int i2 = 0; i2 < bi.BlockCount; i2++)
                            {
                                Blocks.Remove(l);
                                l += (int)DataBlockSize;
                            }
                        }
                    }
                }
                else
                    _deletedDataBlocks.Add(GetId(deletedItem.Value.DiskBuffer), 0);

                int i = HintBatchCount;
                if (i < MaxDeletedBlocksCount)
                    i = MaxDeletedBlocksCount;
                if (_deletedDataBlocks.Count >= i)
                    RemoveDeletedDataBlocks();
            }
        }

        private void RemoveDeletedDataBlocks()
        {
            if (_deletedDataBlocks.Count > 0 && RootNode != null && DataBlockDriver != null)
            {
                foreach (var bi in _deletedDataBlocks)
                {
                    if (bi.Value == 0)
                    {
                        var metaInfo = ((DataBlockDriver)DataBlockDriver).ReadBlockInfoFromDisk(this, bi.Key);
                        var db = new Sop.DataBlock(DataBlockSize, metaInfo)
                        {
                            DataAddress = bi.Key
                        };
                        if (DataSet != null)
                        {
                            RemoveBlock(DataSet, db);
                        }
                        continue;
                    }
                    var mi = new Sop.DataBlock.Info()
                    {
                        Address = bi.Key,
                        BlockCount = bi.Value
                    };
                    var db2 = new Sop.DataBlock(DataBlockSize, new List<Sop.DataBlock.Info>() { mi })
                    {
                        DataAddress = bi.Key
                    };
                    if (DataSet != null)
                    {
                        RemoveBlock(DataSet, db2);
                    }
                }
                _deletedDataBlocks.Clear();
            }
        }

        private const int MaxDeletedBlocksCount = 650;
        private readonly Collections.Generic.SortedDictionary<long, int> _deletedDataBlocks = new Collections.Generic.SortedDictionary<long, int>();

        public override void RegisterChange(bool partialRegister = false)
        {
            base.RegisterChange(partialRegister);
            if (DataSet != null)
                DataSet.IsDirty = true;
            KeySet.IsDirty = true;
        }


        public override void Load()
        {
            SetCurrentItemAddress(-1, 0);
            if (currentDataBlock != null)
                currentDataBlock.ClearData();
            currentEntry = null;

            RemoveDeletedDataBlocks();
            ClearPromoteLookup();
            //PromoteLookup.Clear();
            base.Load();
            if (KeySet.DataAddress < 0)
            {
                if (KeySet.IsDirty)
                    KeySet.IsDirty = false;
            }
            else
                KeySet.Load();
            if (DataSet != null)
            {
                if (DataSet.DataAddress < 0)
                {
                    if (DataSet.IsDirty)
                        DataSet.IsDirty = false;
                }
                else
                    DataSet.Load();
            }
            if (RootNode != null && RootNeedsReload)
                ReloadRoot();
        }

        protected internal override void CloseStream()
        {
            if (IsOpen)
            {
                base.CloseStream();
                if (DataSet != null)
                    ((IInternalFileEntity) DataSet).CloseStream();
                ((IInternalFileEntity) KeySet).CloseStream();
            }
        }


        public override void OnCommit()
        {
            base.OnCommit();
            if (DataSet != null && DataSet.IsOpen)
            {
                if (DataSet.MruManager != null && DataSet.OnDiskBinaryReader != null)
                    DataSet.OnCommit();
            }
            if (KeySet.IsOpen)
            {
                if (KeySet.MruManager != null && KeySet.OnDiskBinaryReader != null)
                    KeySet.OnCommit();
            }
        }

        public override void OnRollback()
        {
            // if close (promote lookup is null, just return... (nothing to rollback).
            if (PromoteLookup == null) return;
            ClearPromoteLookup();
            base.OnRollback();
            if (DataSet != null && DataSet.IsOpen)
            {
                if (DataSet.MruManager != null && DataSet.OnDiskBinaryReader != null)
                    DataSet.OnRollback();
            }
            if (KeySet.IsOpen)
            {
                if (KeySet.MruManager != null && KeySet.OnDiskBinaryReader != null)
                    KeySet.OnRollback();
            }
            _deletedDataBlocks.Clear();
            SetCurrentItemAddress(-1, 0);
            MruManager.Clear();
            IsDirty = false;
            ReloadRoot();
        }

        internal long CustomBlockAddress = -1;

        /// <summary>
        /// Returns Current Sequence Number
        /// </summary>
        /// <returns></returns>
        public long CurrentSequence;

        /// <summary>
        /// Go to Next Sequence and return it
        /// </summary>
        /// <returns></returns>
        public long GetNextSequence()
        {
            return ++CurrentSequence;
        }

        internal volatile SortOrderType CurrentSortOrder = SortOrderType.Ascending;

        /// <summary>
        /// Persist this tree.
        /// Write to stream the meta information of this tree so
        /// it can be read back from disk.
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="writer"></param>
        public override void Pack(IInternalPersistent parent, BinaryWriter writer)
        {
            writer.Write(RootNode.GetAddress(this));
            base.Pack(parent, writer);
            writer.Write((int) this.IndexBlockSize);
            writer.Write(SlotLength);
            writer.Write(HintKeySizeOnDisk);
            writer.Write(HintValueSizeOnDisk);
            writer.Write(IsDataLongInt);
            writer.Write(IsDataInKeySegment);
            writer.Write(IsUnique);
            writer.Write((char) PersistenceType);
            if (DataSet != null)
                DataSet.Pack(parent, writer);
            KeySet.HeaderData = null;
            KeySet.Pack(parent, writer);
            KeySet.HeaderData = HeaderData;
            writer.Write(CustomBlockAddress);
            writer.Write(CurrentSequence);
        }

        /// <summary>
        /// Load this tree from disk.
        /// Read from stream the meta information useful for bringing
        /// back to its previous state in-memory
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="reader"></param>
        public override void Unpack(IInternalPersistent parent, System.IO.BinaryReader reader)
        {
            long rootAddress = reader.ReadInt64();
            if (DataBlockDriver == null)
            {
                reader.ReadInt64();
                DataBlockSize = (DataBlockSize) reader.ReadInt32();
                reader.BaseStream.Seek(
                    -(sizeof (int) + sizeof (long)), SeekOrigin.Current);
                DataBlockDriver = new DataBlockDriver(this);
            }

            base.Unpack(parent, reader);
            IndexBlockSize = (DataBlockSize) reader.ReadInt32();
            _slotLength = reader.ReadInt16();

            HintKeySizeOnDisk = reader.ReadInt32();
            HintValueSizeOnDisk = reader.ReadInt32();
            IsDataLongInt = reader.ReadBoolean();
            _IsDataInKeySegment = reader.ReadBoolean();
            IsUnique = reader.ReadBoolean();
            PersistenceType = (PersistenceType) reader.ReadChar();
            var kvp = new[]
                            {
                                new KeyValuePair<string, object>("HeaderData", null)
                            };
            if (IsDataInKeySegment)
            {
                if (DataSet != null)
                {
                    DataSet.Dispose();
                    DataSet = null;
                }
            }
            else
            {
                if (DataSet == null)
                {
                    DataSet = new SharedBlockOnDiskList(File, kvp) {Parent = this};
                }
                DataSet.Unpack(parent, reader);
            }
            if (KeySet == null)
            {
                kvp[0] = new KeyValuePair<string, object>("HeaderData", HeaderData);
                KeySet = new SharedBlockOnDiskList(File, kvp) {Parent = this};
            }
            KeySet.Unpack(parent, reader);

            CustomBlockAddress = reader.ReadInt64();
            CurrentSequence = reader.ReadInt64();
            Initialize(File);
            if (rootAddress >= 0)
            {
                if (DataSet != null)
                    DataSet.Open();
                KeySet.Open();
                Open();
                object rt = BTreeNodeOnDisk.ReadNodeFromDisk(this, rootAddress);
                if (rt != null)
                {
                    RootNode = (BTreeNodeOnDisk) rt;
                    if (RootNode.Count == 0 && Count == 1)
                    {
                        HeaderData.Count = 0;
                        //IsDirty = true;
                        RegisterChange();
                    }
                }
                if (RootNode == null)
                    RootNode = new BTreeNodeOnDisk(this);
            }
            else
            {
                if (RootNode != null)
                    RootNode.Clear();
            }
        }

        /// <summary>
        /// Make the root Item the current item
        /// </summary>
        /// <returns></returns>
        internal bool MoveToRootItem()
        {
            if (Count > 0)
            {
                CurrentItem.NodeAddress = RootNode.GetAddress(this);
                CurrentItem.NodeItemIndex = 0;
                return true;
            }
            return false;
        }


        /// <summary>
        /// Copy items, starting from the current item pointer, of this B-Tree to destination array.
        /// </summary>
        /// <param name="destArray"></param>
        /// <param name="startIndex"></param>
        public override void CopyTo(Array destArray, int startIndex)
        {
            if (destArray == null)
                throw new ArgumentNullException("destArray");

            HintSequentialRead = true;
            if (CurrentKey != null)
            {
                int i = startIndex;
                do
                {
                    destArray.SetValue(CurrentEntry, i++);
                } while (MoveNext() && i < destArray.Length);
            }
        }

        /// <summary>
        /// comparer wrapper
        /// </summary>
        internal IComparer ComparerWrapper
        {
            get
            {
                if (_comparerWrapper == null)
                    _comparerWrapper = new BTreeComparer(Comparer);
                return _comparerWrapper; 
            }
        }

        /// <summary>
        /// get/set the change registry
        /// </summary>
        public override bool ChangeRegistry
        {
            get { return base.ChangeRegistry; }
            set
            {
                base.ChangeRegistry = value;
                if (DataSet != null)
                    this.DataSet.ChangeRegistry = value;
                this.KeySet.ChangeRegistry = value;
            }
        }

        /// <summary>
        /// Current Node of the tree
        /// </summary>
        public BTreeNodeOnDisk CurrentNode
        {
            get
            {
                if (CurrentItem != null && CurrentItem.NodeAddress != -1)
                {
                    BeginTreeMaintenance();
                    BTreeNodeOnDisk r = this.CurrentItem.GetNode(this);
                    EndTreeMaintenance();
                    return r;
                }
                return null;
            }
        }

        /// <summary>
        /// Remove Node from Objects' cache
        /// </summary>
        /// <param name="node"></param>
        internal void RemoveFromCache(BTreeNodeOnDisk node, bool removeFromPromoteLookup = false)
        {
            if (node == null || node.DiskBuffer == null)
                return;
            long address = node.GetAddress(this);
            if (address >= 0)
            {
                MruManager.Remove(address, true);
                //if (removeFromPromoteLookup)
                //    PromoteLookup.Remove(address);
            }
        }

        internal override bool RemoveInMemory(long dataAddress, Transaction.ITransactionLogger transaction)
        {
            return RemoveInMemory(dataAddress, transaction, true);
        }

        /// <summary>
        /// Remove all objects with DataAddress from memory.
        /// Remove from Data blocks, Objects' cache, collection's Header buffer,...
        /// </summary>
        /// <param name="dataAddress"></param>
        /// <param name="transaction"> </param>
        /// <param name="reloadIfNotFound"> </param>
        /// <returns></returns>
        internal bool RemoveInMemory(long dataAddress, Transaction.ITransactionLogger transaction,
                                     bool reloadIfNotFound)
        {
            BTreeNodeOnDisk rn = RootNode;
            bool r = base.RemoveInMemory(dataAddress, transaction);
            if (DataSet != null)
                r = DataSet.RemoveInMemory(dataAddress, transaction) || r;
            if (KeySet != null)
                r = KeySet.RemoveInMemory(dataAddress, transaction) || r;
            if (RootNode != null)
            {
                bool reloaded = false;
                if (rn == RootNode && RootNode.DiskBuffer.IsBlockOfThis(dataAddress))
                {
                    reloaded = true;
                    ReloadRoot();
                }
                else
                {
                    //** check if DataAddress is in rootnode's slots
                    for (int i = 0; i < RootNode.Count; i++)
                    {
                        if (RootNode.Slots[i].Value != null &&
                            RootNode.Slots[i].Value.DiskBuffer != null &&
                            RootNode.Slots[i].Value.DiskBuffer.IsBlockOfThis(dataAddress))
                        {
                            reloaded = true;
                            ReloadRoot();
                            break;
                        }
                        var itemOnDisk = RootNode.Slots[i].Value;
                        if (itemOnDisk != null && (itemOnDisk.Data is SortedDictionaryOnDisk &&
                                                   ((SortedDictionaryOnDisk) itemOnDisk.Data).DataAddress == dataAddress))
                        {
                            var onDisk = RootNode.Slots[i].Value;
                            if (onDisk != null)
                            {
                                onDisk.Data = null;
                                onDisk.IsDirty = false;
                            }
                            RootNode.Slots[i].ValueLoaded = false;
                            RootNode.Slots[i].IsDirty = false;
                            return false;
                        }
                    }
                }
                if (reloadIfNotFound && !reloaded)
                    ReloadRoot();
            }
            return r;
        }

        internal void ReloadRoot()
        {
            long rootNodeAddress = RootNode.GetAddress(this);
            if (rootNodeAddress < 0)
                return;
            RemoveFromCache(RootNode);
            PromoteLookup.Remove(RootNode.GetAddress(this));

            RootNode.Dispose();
            RootNode = null;
            object rt = BTreeNodeOnDisk.ReadNodeFromDisk(this, rootNodeAddress);
            if (rt != null)
                RootNode = (BTreeNodeOnDisk) rt;
        }

        /// <summary>
        /// Current Entry (Key and Value pair) of the tree.
        /// NOTE: current entry is cached so succeeding get will not read from disk or virtual source
        /// </summary>
        public override object CurrentEntry
        {
            get
            {
                if (HintSequentialRead && _sequentialReadBatchedIDs.Count > 0)
                {
                    if (_sequentialReadIndex < _sequentialReadBatchedIDs.Count)
                        return _sequentialReadBatchedIDs[_sequentialReadIndex];
                }
                if (CurrentItem != null && CurrentItem.NodeAddress > -1 && CurrentItem.NodeItemIndex > -1)
                {
                    BeginTreeMaintenance();
                    object r = this.CurrentItem.GetNode(this).Slots[this.CurrentItem.NodeItemIndex];
                    EndTreeMaintenance();
                    return r;
                }
                return null;
            }
        }

        public virtual object CurrentKey
        {
            get
            {
                LoadSequentialReadBatchedIDs();
                var itm = (BTreeItemOnDisk) CurrentEntry;
                if (itm != null)
                    return itm.Key;
                return null;
            }
        }

        /// <summary>
        /// true will allow SOP to optimize reading of the sequential
        /// records of the B-Tree on disk
        /// </summary>
        public bool HintSequentialRead
        {
            get { return _hintSequentialRead; }
            set
            {
                if (Count < 5000)
                    value = false;
                _hintSequentialRead = value;
                if (!value)
                {
                    if (_sequentialReadBatchedIDs != null)
                        _sequentialReadBatchedIDs.Clear();
                    _sequentialReadIndex = 0;
                    _realDirection = CurrentSortOrder;
                    this._justMoveToEdge = false;
                }
            }
        }

        /// <summary>
        /// Hint Batch Count. When set, B-Tree will
        /// read ahead this number of records and thus,
        /// provide a more optimal sequential read access.
        /// </summary>
        public int HintBatchCount
        {
            get { return _hintBatchCount; }
            set
            {
                if (value > 550)
                    value = 550;
                HintSequentialRead = value > 0;
                if (value > 0 && _hintBatchCount == value) return;
                if (value > 0 && _hintBatchCount != value)
                {
                    _sequentialReadBatchedIDs = new List<BTreeItemOnDisk>(value);
                    _hintBatchCount = value;
                }
                else if (_sequentialReadBatchedIDs != null)
                    _sequentialReadBatchedIDs.Clear();
            }
        }


        private void LoadSequentialReadBatchedIDs(bool? moveToEdge = null)
        {
            if (!HintSequentialRead) return;
            SortOrderType sot = this.CurrentSortOrder;
            if (moveToEdge != null)
                this.CurrentSortOrder = SortOrderType.Ascending;
            if (_sequentialReadBatchedIDs.Count == 0 ||
                _sequentialReadBatchedIDs.Count == _sequentialReadIndex ||
                _sequentialReadIndex < 0)
            {
                _hintSequentialRead = false;
                SortOrderType currentSortOrder = this.CurrentSortOrder;
                if (_sequentialReadBatchedIDs.Count > 0)
                {
                    if (_sequentialReadIndex == _sequentialReadBatchedIDs.Count)
                        currentSortOrder = SortOrderType.Ascending;
                    else if (_sequentialReadIndex < 0)
                        currentSortOrder = SortOrderType.Descending;
                }
                var list = new List<BTreeItemOnDisk>(_hintBatchCount);
                if (moveToEdge != null)
                {
                    if (moveToEdge.Value)
                    {
                        currentSortOrder = SortOrderType.Descending;
                        if (CurrentEntry == null)
                            MoveLast();
                    }
                    else
                    {
                        currentSortOrder = SortOrderType.Ascending;
                        if (CurrentEntry == null)
                            MoveFirst();
                    }
                }
                LoadToList(list, currentSortOrder);
                if (moveToEdge == null &&
                    (_realDirection != currentSortOrder ||
                     _justMoveToEdge))
                {
                    if (!_justMoveToEdge)
                    {
                        if (currentSortOrder == SortOrderType.Ascending)
                            MoveNext();
                        else
                            MovePrevious();
                    }
                    _sequentialReadBatchedIDs.Clear();
                    LoadToList(_sequentialReadBatchedIDs, currentSortOrder);
                }
                else
                {
                    _sequentialReadBatchedIDs.Clear();
                    _sequentialReadBatchedIDs.AddRange(list);
                }
                if (currentSortOrder == SortOrderType.Descending)
                    _sequentialReadIndex = _sequentialReadBatchedIDs.Count - 1;
                else
                    _sequentialReadIndex = 0;

                _realDirection = currentSortOrder;

                if (moveToEdge != null)
                {
                    if (moveToEdge.Value)
                        MoveLast();
                    else
                        MoveFirst();
                }
                _hintSequentialRead = true;
                _justMoveToEdge = moveToEdge != null;
            }
            if (moveToEdge != null)
                this.CurrentSortOrder = sot;
        }

        private void LoadToList(List<BTreeItemOnDisk> list, SortOrderType currentSortOrder)
        {
            while (CurrentEntry != null && list.Count < _hintBatchCount)
            {
                var itm = (BTreeItemOnDisk) CurrentEntry;
                list.Add(itm);
                if (currentSortOrder == SortOrderType.Ascending)
                {
                    if (!MoveNext())
                        break;
                }
                else
                {
                    if (!MovePrevious())
                        break;
                }
            }
            // todo: verify validity of this...
            if (currentSortOrder == SortOrderType.Descending)
                SwapItems(list);
        }

        private void SwapItems(List<BTreeItemOnDisk> itemList)
        {
            if (itemList.Count <= 1) return;
            int frontEndCtr = 0;
            int backEndCtr = itemList.Count - 1;
            for (int i = 0; i < itemList.Count/2; i++)
            {
                BTreeItemOnDisk o = itemList[frontEndCtr];
                itemList[frontEndCtr++] = itemList[backEndCtr];
                itemList[backEndCtr--] = o;
            }
        }

        internal void ValueUnpack(BinaryReader reader, BTreeItemOnDisk targetItem)
        {
            if (onValueUnpack != null)
                targetItem.Value.Data = onValueUnpack(reader);
            else if (CurrentOnValueUnpack != null)
                targetItem.Value.Data = CurrentOnValueUnpack(reader);
            else
            {
                throw new InvalidOperationException(
                    "No Value DeSerializer (OnValueUnpack) can be found for use to unpack.");
            }
        }

        /// <summary>
        /// Update Currently selected Item of B-Tree.
        /// This version expects Current Item to be Sop.DataBlock type.
        /// </summary>
        /// <param name="source">DataBlock Source</param>
        /// <returns></returns>
        internal bool UpdateCurrentItem(Sop.DataBlock source)
        {
            BTreeNodeOnDisk n = CurrentNode;
            BTreeItemOnDisk itm = n.Slots[CurrentItem.NodeItemIndex];

            if (itm == null) return false;
            if (!itm.ValueLoaded)
            {
                long da = GetId(itm.Value.DiskBuffer);
                if (itm.Value != null && da > -1)
                {
                    Sop.DataBlock d = GetDiskBlock(DataSet, da);
                    //DataBlockDriver.ReadBlockFromDisk(DataSet, da, false);
                    itm.Value.DiskBuffer = d;
                    var iod = (ItemOnDisk) ReadFromBlock(d);
                    d = null;
                    if (iod != null)
                    {
                        iod.DiskBuffer = itm.Value.DiskBuffer;
                        itm.Value = iod;
                    }
                }
                itm.ValueLoaded = true;
            }
            if (itm.Value == null)
                return false;
            var db = (Sop.DataBlock)itm.Value.Data;
            source.Copy(db);
            itm.IsDirty = true;
            itm.Value.IsDirty = true;
            n.IsDirty = true;
            MruManager.Add(CurrentItem.NodeAddress, n);
            return true;
        }


        /// <summary>
        /// Get/Set the tree's Slots' count of items.
        /// </summary>
        public short SlotLength
        {
            get { return _slotLength; }
            set
            {
                if (value <= 0)
                    throw new ArgumentOutOfRangeException("value", value, "SlotLength should be > 0");
                if (value%2 != 0)
                    value++;
                _slotLength = value;
            }
        }

        /// <summary>
        /// Utility function to assign/replace current item w/ a new item.
        /// </summary>
        /// <param name="itemNodeAddress"> </param>
        /// <param name="itemIndex">slot index of the new item</param>
        protected internal void SetCurrentItemAddress(long itemNodeAddress, short itemIndex)
        {
            if (CurrentItem == null) return;
            CurrentItem.NodeAddress = itemNodeAddress;
            CurrentItem.NodeItemIndex = itemIndex;
        }

        /// <summary>
        /// MRU Segments on disk parent
        /// </summary>
        //internal MruSegmentsOnDisk ParentMruSegments = null;
        /// <summary>
        /// get/set whether this tree is modified(dirty) or not
        /// </summary>
        public override bool IsDirty
        {
            get
            {
                return !RootNeedsReload &&
                       IsOpen && !IsCloned &&
                       !IsUnloading && (
                                           (DataSet != null && DataSet.IsDirty && DataSet.Count > 0) ||
                                           (KeySet != null && KeySet.IsDirty && KeySet.Count > 0) ||
                                           (RootNode != null && RootNode.IsDirty && RootNode.Count > 0 &&
                                            RootNode.GetAddress(this) >= 0) ||
                                           base.IsDirty);
            }
            set { base.IsDirty = value; }
        }


        /// <summary>
        /// B-Tree default number of slots
        /// </summary>
        public const short DefaultSlotLength = 200;
        /// <summary>
        /// Data Block size of the index
        /// </summary>
        public DataBlockSize IndexBlockSize { get; set; }
        /// <summary>
        /// This holds the Current Item Address (Current Node and Current Slot index)
        /// </summary>
        public BTreeNodeOnDisk.ItemAddress CurrentItem = new BTreeNodeOnDisk.ItemAddress();

        /// <summary>
        /// true will save long int data with Key, thus, is a lot more optimized.
        /// NOTE: you can up-convert short, int to long int to take advantage of 
        /// this more optimized method.
        /// </summary>
        public bool IsDataLongInt;
        public PersistenceType PersistenceType = PersistenceType.Unknown;

        /// <summary>
        /// Hint: Key size on disk(in bytes)
        /// </summary>
        public int HintKeySizeOnDisk
        {
            get
            {
                return _hintKeySizeOnDisk;
            }
            set
            {
                if (value > _hintKeySizeOnDisk)
                    _hintKeySizeOnDisk = value;
            }
        }
        private int _hintKeySizeOnDisk;

        /// <summary>
        /// Hint: Value size on disk(in bytes)
        /// </summary>
        public int HintValueSizeOnDisk
        {
            get
            {
                return _hintValueSizeOnDisk;
            }
            set
            {
                if (value > _hintValueSizeOnDisk)
                    _hintValueSizeOnDisk = value;
            }
        }
        private int _hintValueSizeOnDisk;

        internal bool? _IsDataInKeySegment;
        internal SortedDictionaryOnDisk Container;
        internal bool RootNeedsReload;

        internal volatile bool AutoFlush;

        /// <summary>
        /// true will cause entry's Key and/or Value to be automatically disposed
        /// when it gets offloaded to disk from memory, false otherwise.
        /// </summary>
        internal volatile bool AutoDisposeItem;
        /// <summary>
        /// true will cause this BTreeAlgorithm to be automatically disposed
        /// when this gets offloaded to disk from memory, false otherwise.
        /// </summary>
        internal volatile bool AutoDispose;

        /// <summary>
        /// Data are stored in DataSet collection.
        /// DataSet manages storage/retrieval of data in datablocks to/from disk.
        /// </summary>
        internal LinkedListOnDisk DataSet = null;
        /// <summary>
        /// Key Data are stored on KeySet
        /// </summary>
        internal LinkedListOnDisk KeySet = null;

        [ThreadStatic]
        internal static OnObjectUnpack CurrentOnValueUnpack;

        /// <summary>
        /// Maximum count of blocks to be kept in-memory
        /// </summary>
        protected int MaxBlocks = 1000;

        private BTreeComparer _comparerWrapper = null;
        private IComparer _comparer;
        private SortOrderType _realDirection;
        private bool _justMoveToEdge;
        private int _hintBatchCount = 200;
        private List<BTreeItemOnDisk> _sequentialReadBatchedIDs = new List<BTreeItemOnDisk>(200);
        private int _sequentialReadIndex;
        private bool _hintSequentialRead;
        private int _onMaxBlocksCount = 0;
        private int _maintenanceCallCount = 0;

        #region temporary variables used in B-Tree promote node process
        internal BTreeNodeOnDisk PromoteParent;
        internal short PromoteIndexOfNode;
        internal bool FixVacatedSlot;

        // Slots for temporary use. When node is full, this Slots is used so that
        // auxiliary functions may be fooled as if they are still operating in a 
        // valid slots.
        // pTempSlots - temporarily holds SlotLength + 1 number of slots.
        // pTempParent - temporary parent node of the newly split nodes.
        // pTempChildren - temporary holds pointers to the left & right
        //	child nodes of pTempParent.
        internal BTreeItemOnDisk[] TempSlots = null;
        internal BTreeItemOnDisk TempParent = null;
        // Temp Children nodes.
        internal long[] TempChildren = null;
        // Only 2 since only left & right child nodes will be handled.
        internal long[] TempParentChildren = new long[2];
        private short _slotLength = 0;
        #endregion

        public override string ToString()
        {
            return string.Format("BTree {0}, HeaderData {1}, DeletedBlocks {2}", Name, HeaderData.ToString(),
                deletedBlocks != null ? DeletedBlocks.ToString() : "");
        }
    }
}
