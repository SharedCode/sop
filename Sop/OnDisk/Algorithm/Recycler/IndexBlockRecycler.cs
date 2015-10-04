// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)


//#define LogRecycledBlocksCount

using System;
using System.Collections;
using System.Collections.Generic;
using Sop.Collections.Generic;
using Sop.OnDisk.DataBlock;
using Sop.OnDisk.Geometry;
using Sop.Utility;

namespace Sop.OnDisk.Algorithm.BTree
{
    /// <summary>
    /// An efficient "indexed" DataBlockRecycler implementing 
    /// realtime deleted blocks merging (to create a bigger reusable segment).
    /// </summary>
    internal class IndexedBlockRecycler : BTreeAlgorithm, IDataBlockRecycler
    {
        public IndexedBlockRecycler(
            File.IFile file, IComparer comparer, string name
            )
        {
            IsDataLongInt = true;
            var param = new[]
                                                       {
                                                           new KeyValuePair<string, object>("Comparer", comparer),
                                                           new KeyValuePair<string, object>("DataBlockDriver", null)
                                                       };
            Name = name;
            Initialize(file, param);
            _segmentSize = file.StoreGrowthSizeInNob * (int)DataBlockSize;
        }

        #region Transaction event handlers
        public override void OnCommit()
        {
            if (_deletedBlocksStore.Count > 0)
            {
                foreach (KeyValuePair<long, long> itm in _deletedBlocksStore)
                    addAvailableBlock(itm.Key, itm.Value);
                _deletedBlocksStore.Clear();
            }
            Flush();
        }

        public override void OnRollback()
        {
            _deletedBlocksStore.Clear();
        }
        #endregion

        private bool DetectAndMerge(Collections.Generic.SortedDictionary<long, long> store,
                                           long dataAddress, long dataSize, int segmentSize)
        {
            return DetectAndMerge(store, dataAddress, dataSize, segmentSize, _region);
        }

        internal static bool DetectAndMerge(Collections.Generic.SortedDictionary<long, long> store,
                    long dataAddress, long dataSize, int segmentSize = DataBlock.DataBlockDriver.MaxSegmentSize, RegionLogic region = null)
        {
            if (store.Count == 0)
            {
                if (dataSize > segmentSize)
                    return false;
                store.Add(dataAddress, dataSize);
                return true;
            }
            if (store.Search(dataAddress))
            {
                long currSize = store.CurrentValue;
                if (currSize < dataSize)
                    store.CurrentValue = dataSize;
                return true;
            }
            //** Detect and merge contiguous deleted blocks
            short passCount = 0;
            if (!store.MovePrevious())
                store.MoveFirst();
            while (true)
            {
                KeyValuePair<long, long>? item = store.CurrentEntry;
                long k2 = item.Value.Key;
                long i = 0;
                long cv = store.CurrentValue;
                i = cv;
                if (region != null)
                {
                    if (region.Equals(dataAddress, dataSize, k2, i) ||
                        region.FirstWithinSecond(dataAddress, dataSize, k2, i))
                        return true;
                    if (region.FirstWithinSecond(k2, i, dataAddress, dataSize))
                    {
                        store.Remove(k2);
                        store.Add(dataAddress, dataSize);
                        return true;
                    }
                }
                if (dataAddress + dataSize == k2)
                {
                    long newSize = i + dataSize;
                    if (newSize <= segmentSize)
                    {
                        store.Remove(item.Value.Key);
                        store.Add(dataAddress, newSize);
                        return true;
                    }
                    return false;
                }
                if (k2 + i == dataAddress)
                {
                    if (i + dataSize <= segmentSize)
                    {
                        store.CurrentValue = i + dataSize;
                        return true;
                    }
                    return false;
                }
                if (++passCount >= 2)
                    break;
                if (!store.MoveNext())
                    break;
            }
            return false;
        }

        #region Add Available Block
        private void AddToTransCache(Collections.Generic.SortedDictionary<long, long> store,
                                            long dataAddress, long dataSize, int segmentSize)
        {
            if (store.Count == 0)
                store.Add(dataAddress, dataSize);
            else if (!DetectAndMerge(store, dataAddress, dataSize, segmentSize))
                store.Add(dataAddress, dataSize);
        }
        public void AddAvailableBlock(long dataAddress, long dataSize)
        {
            AddToTransCache(_deletedBlocksStore, dataAddress, dataSize, _segmentSize);
        }
        private long addAvailableBlock(long dataAddress, long dataSize)
        {
            if (_isDuringMaintenance)
            {
                long l;
                if (_duringMaintenanceItems.TryGetValue(dataAddress, out l))
                {
                    if (dataSize > l)
                        _duringMaintenanceItems[dataAddress] = dataSize;
                }
                else
                    _duringMaintenanceItems.Add(dataAddress, dataSize);
                return 0;
            }
            var itm = new BTreeItemOnDisk(File.DataBlockSize, dataAddress, dataSize) { Value = { DiskBuffer = CreateBlock() } };
            BeginTreeMaintenance();
            try
            {
                if (Count == 0 || RootNode.Count == 0)
                {
                    bool adjustCount = Count == 1 && RootNode.Count == 0;
                    _isDuringMaintenance = true;
                    Add(itm);
                    _isDuringMaintenance = false;
                    AddMaintenanceAddedItems();
                    if (adjustCount)
                        UpdateCount(UpdateCountType.Decrement);
                }
                else if (!this.Search(dataAddress))
                {
                    //** Detect and merge contiguous deleted blocks
                    short passCount = 0;
                    if (!this.MovePrevious())
                        this.MoveFirst();
                    while (true)
                    {
                        var item = (BTreeItemOnDisk)this.CurrentEntry;
                        long k2 = (long)item.Key;
                        long i = 0;
                        object cv = CurrentValue;
                        i = (long)cv;

                        if (_region.Equals(dataAddress, dataSize, k2, i) ||
                            _region.FirstWithinSecond(dataAddress, dataSize, k2, i))
                            return k2;
                        else if (_region.FirstWithinSecond(k2, i, dataAddress, dataSize))
                        {
                            _isDuringMaintenance = true;
                            Remove();
                            itm.Key = dataAddress;
                            itm.Value.Data = dataSize;
                            Add(itm);
                            _isDuringMaintenance = false;
                            AddMaintenanceAddedItems();
                            return dataAddress;
                        }
                        else if (dataAddress + dataSize == k2)
                        {
                            long newSize = i + dataSize;
                            if (newSize <= _segmentSize)
                            {
                                _isDuringMaintenance = true;
                                Remove();
                                itm.Key = dataAddress;
                                itm.Value.Data = newSize;
                                Add(itm);
                                _isDuringMaintenance = false;
                                AddMaintenanceAddedItems();
                                return dataAddress;
                            }
                            break;
                        }
                        else if (k2 + i == dataAddress)
                        {
                            if (i + dataSize <= _segmentSize)
                            {
                                _isDuringMaintenance = true;
                                CurrentValue = i + dataSize;
                                _isDuringMaintenance = false;
                                AddMaintenanceAddedItems();
                                return k2;
                            }
                            break;
                        }
                        else if (++passCount >= 2)
                            break;
                        if (!MoveNext())
                            break;
                    }
                    _isDuringMaintenance = true;
                    this.Add(itm);
                    _isDuringMaintenance = false;
                    AddMaintenanceAddedItems();
                }
                else
                {
                    long currSize = (long)CurrentValue;
                    if (currSize < dataSize)
                    {
                        _isDuringMaintenance = true;
                        CurrentValue = dataSize;
                        _isDuringMaintenance = false;
                        AddMaintenanceAddedItems();
                    }
                }
            }
            finally
            {
                EndTreeMaintenance();
            }
            return 0;
        }
        #endregion

        private void AddMaintenanceAddedItems()
        {
            if (!_isDuringMaintenance && _duringMaintenanceItems.Count > 0)
            {
                long[] b;
                var a = new long[_duringMaintenanceItems.Count];
                b = new long[_duringMaintenanceItems.Count];
                _duringMaintenanceItems.Keys.CopyTo(a, 0);
                _duringMaintenanceItems.Values.CopyTo(b, 0);
                _duringMaintenanceItems.Clear();
                for (int i = 0; i < a.Length; i++)
                    addAvailableBlock(a[i], b[i]);
                if (_duringMaintenanceItems.Count > 0)
                    AddMaintenanceAddedItems();
            }
        }

        #region Available Block management methods
        public bool GetAvailableBlock(bool isRequesterRecycler, int requestedBlockSize,
                                      out long dataAddress, out long dataSize)
        {
            dataAddress = 0;
            dataSize = 0;

            if (_isDuringMaintenance)
                return false;
            if (Count > 0)
            {
                BeginTreeMaintenance();
                try
                {
                    //** cycle through(10 per call) all recyclable items and find the one with the right size.
                    bool startedWithFirst = false;
                    if (_currentRecycleIndex == 0)
                    {
                        this.MoveFirst();
                        startedWithFirst = true;
                    }
                    else
                    {
                        if (!Search(_currentRecycleIndex) && CurrentEntry == null)
                        {
                            _currentRecycleIndex = 0;
                            this.MoveFirst();
                            startedWithFirst = true;
                        }
                    }

                    long previousAddress = -1;
                    long previousDataSize = -1;
                    long biggestMergedAddress = -1;
                    long biggestMergedChunk = 0;
                    int blockFactor = RecycleBlockFactor;
                    const int maxRecyclePassCount = 1;
                    if (_noRecyclePassCount >= maxRecyclePassCount)
                        blockFactor /= 2;
                    for (int ctr = 0; ctr < 7; ctr++)
                    {
                        object v = this.CurrentValue;
                        var i = (long)v;
                        var currentAddress = (long)CurrentKey;
                        if (i >= requestedBlockSize &&
                            ((!_segmentRemoved && _currentRecycleIndex > 0) ||
                             i >= (int)DataBlockSize * blockFactor))
                        {
                            _segmentRemoved = false;
                            dataAddress = currentAddress;
                            _currentRecycleIndex = dataAddress;
                            dataSize = i;
                            _noRecyclePassCount = 0;
                            return true;
                        }
                        if (previousAddress >= 0)
                        {
                            if (previousAddress + previousDataSize == currentAddress)
                            {
                                long newSize = previousDataSize + i;
                                _isDuringMaintenance = true;
                                Remove();
                                if (Search(previousAddress))
                                {
                                    _isDuringMaintenance = true;
                                    CurrentValue = newSize;
                                    _isDuringMaintenance = false;
                                    AddMaintenanceAddedItems();

                                    if (biggestMergedChunk < newSize)
                                    {
                                        biggestMergedChunk = newSize;
                                        biggestMergedAddress = previousAddress;
                                    }

                                    currentAddress = previousAddress;
                                    i = newSize;
                                }
                                else
                                    break;
                            }
                        }
                        if (biggestMergedAddress == -1 || i > biggestMergedChunk)
                        {
                            biggestMergedAddress = currentAddress;
                            biggestMergedChunk = i;
                        }
                        previousAddress = currentAddress;
                        previousDataSize = i;
                        if (!MoveNext())
                        {
                            if (startedWithFirst)
                                break;
                            else
                            {
                                MoveFirst();
                                startedWithFirst = true;
                            }
                        }
                    }
                    if (biggestMergedAddress >= 0 &&
                        biggestMergedChunk >= requestedBlockSize &&
                        ((!_segmentRemoved && _currentRecycleIndex > 0)
                         ||
                         biggestMergedChunk >= (int)DataBlockSize * blockFactor))
                    {
                        _noRecyclePassCount = 0;
                        _segmentRemoved = false;
                        dataAddress = biggestMergedAddress;
                        _currentRecycleIndex = dataAddress;
                        dataSize = biggestMergedChunk;
                        return true;
                    }
                    if (CurrentKey != null)
                        _currentRecycleIndex = (long)CurrentKey;
                    else
                        _currentRecycleIndex = 0;
                    _noRecyclePassCount++;
                }
                catch (Exception exc)
                {
                    Log.Logger.Instance.Log(Log.LogLevels.Error, exc);
                    throw;
                }
                finally
                {
                    EndTreeMaintenance();
                }
            }
            return false;
        }

        public bool SetAvailableBlock(long availableBlockAddress,
                                      long availableBlockNewAddress, long availableBlockNewSize)
        {
            if (_isDuringMaintenance)
            {
                if (_duringMaintenanceItems.ContainsKey(availableBlockAddress))
                {
                    _duringMaintenanceItems.Remove(availableBlockAddress);
                    _duringMaintenanceItems[availableBlockNewAddress] = availableBlockNewSize;
                    return true;
                }
                return false;
            }
            var itm = new BTreeItemOnDisk(File.DataBlockSize, availableBlockAddress, 0);
            BeginTreeMaintenance();
            try
            {
                if (Search(itm))
                {
                    _isDuringMaintenance = true;
                    Remove();
                    long r = addAvailableBlock(availableBlockNewAddress,
                                               availableBlockNewSize);
                    _currentRecycleIndex = r > 0 ? r : availableBlockNewAddress;
                    _isDuringMaintenance = false;
                    AddMaintenanceAddedItems();
                    return true;
                }
            }
            finally
            {
                EndTreeMaintenance();
            }
            return false;
        }

        public void RemoveAvailableBlock(long dataAddress)
        {
            if (_isDuringMaintenance)
            {
                _duringMaintenanceItems.Remove(dataAddress);
                return;
            }
            var itm = new BTreeItemOnDisk(File.DataBlockSize, dataAddress, 0);
            _isDuringMaintenance = true;
            BeginTreeMaintenance();
            Remove(itm);
            _segmentRemoved = true;
            _isDuringMaintenance = false;
            AddMaintenanceAddedItems();
            EndTreeMaintenance();
        }
        #endregion

        private const int RecycleBlockFactor = 10;
        private long _currentRecycleIndex;
        private bool _segmentRemoved;

#if (LogRecycledBlocksCount)
        int CountBeforeLog;
#endif

        private int _noRecyclePassCount = 0;
        private readonly Collections.Generic.SortedDictionary<long, long> _deletedBlocksStore =
            new Collections.Generic.SortedDictionary<long, long>();

        private readonly Dictionary<long, long> _duringMaintenanceItems = new Dictionary<long, long>();
        private bool _isDuringMaintenance = false;
        private readonly int _segmentSize;

        //private readonly Logger _logger = Logger.Instance;
        private readonly RegionLogic _region = new RegionLogic();
    }
}