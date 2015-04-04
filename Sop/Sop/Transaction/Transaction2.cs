// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.IO;
using Sop.Collections.Generic;
using Sop.OnDisk.Algorithm.Collection;
using Sop.OnDisk.Algorithm.SortedDictionary;
using Sop.OnDisk.Geometry;
using Sop.OnDisk.IO;
using Sop.Utility;
using FileStream = Sop.OnDisk.File.FileStream;

namespace Sop.Transaction
{
    using OnDisk;

    /// <summary>
    /// Transaction management class.
    /// Our transaction model:
    /// 1) Two phase commit: 
    ///		- 1st phase, save all transaction changed records in the 
    /// collection on disk's transaction segment. Save the transaction log
    ///		- 2nd phase, update the changed records' current pointers to reference
    /// the updated records in transaction segment.
    /// 2) Mark the transaction as completed. ie - delete the transaction table records,
    /// </summary>
    internal partial class Transaction
    {
        internal static RecordKey CreateKey(ICollectionOnDisk collection)
        {
            return CreateKey(collection, -1);
        }

        internal static RecordKey CreateKey(ICollectionOnDisk collection, long address)
        {
            var key = new RecordKey();
            key.ServerSystemFilename = collection.File.Server.Filename;
            key.Filename = collection.File.Filename;
            key.CollectionName = collection.Name;
            key.Address = address;
            return key;
        }

        internal bool IsTransactionStore(CollectionOnDisk collection)
        {
            return DiskBasedMetaLogging && collection.IsTransactionStore;
        }

        internal static bool RegisterFileGrowth(Collections.Generic.ISortedDictionary<RecordKey, long> fileGrowthStore,
                                                CollectionOnDisk collection, long segmentAddress, long segmentSize,
                                                bool recycleCollection)
        {
            RecordKey key = CreateKey(collection, segmentAddress);
            if (!fileGrowthStore.ContainsKey(key))
            {
                if (!recycleCollection)
                {
                    if (collection.Transaction is Transaction)
                        ((Transaction)collection.Transaction).AppendLogger.LogLine(
                            "{0}{1} {2} {3}", GrowToken, collection.File.Filename, segmentAddress, segmentSize);
                }

                if (!fileGrowthStore.MovePrevious())
                    fileGrowthStore.MoveFirst();
                short moveNextCount = 0;
                RecordKey k2;
                KeyValuePair<RecordKey, long>? de;
                while (!fileGrowthStore.EndOfTree())
                {
                    de = fileGrowthStore.CurrentEntry;
                    k2 = de.Value.Key;
                    long i = de.Value.Value;
                    if (k2.ServerSystemFilename == key.ServerSystemFilename &&
                        k2.Filename == key.Filename)
                    {
                        if (segmentAddress + segmentSize == k2.Address)
                        {
                            long newSize = i + segmentSize;
                            fileGrowthStore.Remove(de.Value.Key);
                            k2.Address = segmentAddress;
                            fileGrowthStore.Add(k2, newSize);
                            return true;
                        }
                        if (k2.Address + i == segmentAddress)
                        {
                            long expandedSegmentSize = i + segmentSize;
                            if (expandedSegmentSize <= int.MaxValue)
                            {
                                fileGrowthStore.CurrentValue = expandedSegmentSize;
                                return true;
                            }
                        }
                    }
                    if (++moveNextCount >= 2)
                        break;
                    fileGrowthStore.MoveNext();
                }
                fileGrowthStore.Add(key, segmentSize);
                return true;
            }
            throw new InvalidOperationException(
                string.Format("File '{0}' _region '{1}' already expanded.", key.Filename, key.Address)
                );
        }

        /// <summary>
        /// Register file growth
        /// </summary>
        /// <param name="collection"></param>
        /// <param name="segmentAddress"></param>
        /// <param name="segmentSize"></param>
        protected internal override void RegisterFileGrowth(CollectionOnDisk collection, long segmentAddress,
                                                            long segmentSize)
        {
            if (IsTransactionStore(collection))
            {
                ((TransactionBase)collection.ParentTransactionLogger).RegisterFileGrowth(collection, segmentAddress,
                                                                                          segmentSize);
                return;
            }
            if (LogCollection != null)
                RegisterFileGrowth(_fileGrowthStore, collection, segmentAddress, segmentSize, false);
        }

        protected internal override void RegisterRemove(CollectionOnDisk collection)
        {
            if (IsTransactionStore(collection))
            {
                ((TransactionBase)collection.ParentTransactionLogger).RegisterRemove(collection);
                return;
            }
            if (LogCollection == null)
                return;
            if (_inCommit == 0)
                TrackModification(collection.GetTopParent());
        }

        internal static void RegisterAdd(
            Collections.Generic.ISortedDictionary<RecordKey, long> addStore,
            Collections.Generic.ISortedDictionary<RecordKey, long> fileGrowthStore,
            Collections.Generic.ISortedDictionary<RecordKey, long> recycledCollectionStore,
            CollectionOnDisk collection,
            long blockAddress, int blockSize, bool checkIfInGrowthSegments)
        {
            RecordKey key = CreateKey(collection, blockAddress);
            RegisterAdd(addStore, fileGrowthStore, recycledCollectionStore, key, blockSize,
                               checkIfInGrowthSegments);
        }

        /// <summary>
        /// Check whether a block is a newly added block or is in new segment or recycle store.
        /// </summary>
        /// <param name="addStore"></param>
        /// <param name="fileGrowthStore"></param>
        /// <param name="recycledCollectionStore"></param>
        /// <param name="key"></param>
        /// <param name="blockSize"></param>
        /// <param name="checkIfInGrowthSegments"></param>
        /// <returns>true if block is either new, in new segment or in recycle store, false otherwise</returns>
        internal static void RegisterAdd(
            Collections.Generic.ISortedDictionary<RecordKey, long> addStore,
            Collections.Generic.ISortedDictionary<RecordKey, long> fileGrowthStore,
            Collections.Generic.ISortedDictionary<RecordKey, long> recycledCollectionStore,
            RecordKey key, int blockSize, bool checkIfInGrowthSegments)
        {
            //** Check if Block is in Growth Segments
            if (checkIfInGrowthSegments &&
                (RegionLogic.IsSegmentInStore(fileGrowthStore, key, blockSize) ||
                 RegionLogic.IsSegmentInStore(recycledCollectionStore, key, blockSize)))
                return;

            //** Add Block to AddStore for use on Rollback...
            if (!addStore.ContainsKey(key))
            {
                short passCount = 0;
                //** Detect and merge contiguous blocks
                if (!addStore.MovePrevious())
                    addStore.MoveFirst();
                while (!addStore.EndOfTree())
                {
                    var de = addStore.CurrentEntry;
                    RecordKey k2 = de.Value.Key;
                    long i = de.Value.Value;
                    if (k2.ServerSystemFilename == key.ServerSystemFilename &&
                        k2.Filename == key.Filename &&
                        k2.CollectionName == key.CollectionName)
                    {
                        if (key.Address + blockSize == k2.Address)
                        {
                            long newSize = i + blockSize;
                            addStore.Remove(de.Value.Key);
                            k2.Address = key.Address;
                            addStore.Add(k2, newSize);
                            return;
                        }
                        if (k2.Address + i == key.Address)
                        {
                            addStore.CurrentValue = i + blockSize;
                            return;
                        }
                        if (key.Address >= k2.Address && key.Address + blockSize <= k2.Address + i)
                            //** if block is inclusive, don't do anything...
                            return;
                    }
                    else if (++passCount >= 2)
                        break;
                    addStore.MoveNext();
                }
                addStore.Add(key, blockSize);
            }
        }
        private bool InAddStore(RecordKey key, int blockSize)
        {
            Collections.Generic.ISortedDictionary<RecordKey, long> addStore = _addStore;
            if (!addStore.ContainsKey(key))
            {
                short passCount = 0;
                //** Detect and merge contiguous blocks
                if (!_addStore.MovePrevious())
                    addStore.MoveFirst();
                while (!addStore.EndOfTree())
                {
                    var de = addStore.CurrentEntry;
                    RecordKey k2 = de.Value.Key;
                    long i = de.Value.Value;
                    if (k2.ServerSystemFilename == key.ServerSystemFilename &&
                        k2.Filename == key.Filename &&
                        k2.CollectionName == key.CollectionName)
                    {
                        if (key.Address + blockSize == k2.Address)
                        {
                            return true;
                        }
                        if (k2.Address + i == key.Address)
                        {
                            return true;
                        }
                        if (key.Address >= k2.Address && key.Address + blockSize <= k2.Address + i)
                            //** if block is inclusive, don't do anything...
                            return true;
                    }
                    else if (++passCount >= 2)
                        break;
                    addStore.MoveNext();
                }
            }
            return false;
        }

        protected internal override void RegisterRecycleCollection(CollectionOnDisk collection,
                                                                   long blockAddress,
                                                                   int blockSize)
        {
            RecordKey key = CreateKey(collection, blockAddress);
            if (!_recycledCollectionStore.ContainsKey(key))
                RegisterFileGrowth(_recycledCollectionStore, collection, blockAddress, blockSize, true);
        }

        protected internal override void RegisterRecycle(CollectionOnDisk collection,
                                                         long blockAddress,
                                                         int blockSize)
        {
            RegisterRecycle(_addStore, collection, blockAddress, blockSize);
        }

        internal static void RegisterRecycle(
            Collections.Generic.ISortedDictionary<RecordKey, long> addStore,
            CollectionOnDisk collection,
            long blockAddress,
            int blockSize)
        {
            BackupDataLogKey logKey = new BackupDataLogKey();
            logKey.SourceFilename = collection.File.Filename;
            logKey.SourceDataAddress = blockAddress;

            IEnumerable<KeyValuePair<BackupDataLogKey, BackupDataLogValue>> intersectingLogs;
            long mergedBlockStartAddress, mergedBlockSize;
            if (GetIntersectingLogs(logKey, blockSize, out intersectingLogs,
                                    out mergedBlockStartAddress, out mergedBlockSize))
            {
                if (intersectingLogs != null)
                {
                    //** get area(s) outside each intersecting segment and back it up...
                    Region newRegion = new Region(blockAddress, blockSize);
                    bool wasIntersected = false;
                    foreach (KeyValuePair<BackupDataLogKey, BackupDataLogValue> entry in intersectingLogs)
                    {
                        if (newRegion.Subtract(entry.Key.SourceDataAddress, entry.Value.DataSize))
                            wasIntersected = true;
                    }
                    if (wasIntersected)
                    {
                        foreach (KeyValuePair<long, int> newArea in newRegion)
                            RegisterAdd(addStore, null, null, collection, newArea.Key, newArea.Value, false);
                    }
                    else
                        RegisterAdd(addStore, null, null, collection, blockAddress, blockSize, false);
                }
            }
            else
                RegisterAdd(addStore, null, null, collection, blockAddress, blockSize, false);
        }

        /// <summary>
        /// RegisterAdd will be called whenever a "new" block is allocated.
        /// Don't save block at this point as changes not saved yet.
        /// </summary>
        /// <param name="collection"></param>
        /// <param name="blockAddress"></param>
        /// <param name="blockSize"></param>
        protected internal override void RegisterAdd(CollectionOnDisk collection, long blockAddress, int blockSize)
        {
            if (IsTransactionStore(collection))
            {
                ((TransactionBase)collection.ParentTransactionLogger).RegisterAdd(collection, blockAddress, blockSize);
                return;
            }
            if (LogCollection == null)
                return;
            RecordKey key = CreateKey(collection, blockAddress);
            //** Check if Block is in Growth Segments
            if (RegionLogic.IsSegmentInStore(_fileGrowthStore, key, blockSize) ||
                RegionLogic.IsSegmentInStore(_recycledCollectionStore, key, blockSize))
            {
                if (_inCommit == 0)
                    TrackModification(collection.GetTopParent());
                return;
            }
            RegisterAdd(_addStore, _fileGrowthStore, _recycledCollectionStore, collection, blockAddress,
                                 blockSize, false);
            if (_inCommit == 0)
                TrackModification(collection.GetTopParent());
        }
        internal void TrackModification(CollectionOnDisk collection)
        {
            TrackModification(collection, false);
        }
        internal void TrackModification(CollectionOnDisk collection, bool untrack)
        {
            CollectionOnDisk p = collection; // Collection.GetTopParent();
            RecordKey key = CreateKey(p);
            if (!untrack)
            {
                ModifiedCollections[key] = p;
                return;
            }
            ModifiedCollections.Remove(key);
        }

        /// <summary>
        /// RegisterSave will be called when a block cache faulted from memory
        /// onto Disk. Resolution of Added blocks will be done here and only
        /// those "modified" blocks will be saved. Newly added block(s) will 
        /// not be saved.
        /// </summary>
        /// <param name="collection">Collection that is saving the block</param>
        /// <param name="blockAddress"></param>
        /// <param name="segmentSize"></param>
        /// <param name="readPool"> </param>
        /// <param name="writePool"> </param>
        protected internal override bool RegisterSave(CollectionOnDisk collection, long blockAddress,
            int segmentSize, ConcurrentIOPoolManager readPool, ConcurrentIOPoolManager writePool)
        {
            if (IsTransactionStore(collection))
            {
                return ((TransactionBase)collection.ParentTransactionLogger).RegisterSave(collection, blockAddress,
                                                                                           segmentSize, readPool,
                                                                                           writePool);
            }
            if (LogCollection == null) return false;

            /* Step 1. Remove Intersections with Added, Growth segments & Recycled Blocks from region as no need to backup 
					 new Blocks
			   Step 2. Copy or backup (any) remaining blocks (the Updated blocks) 
					 onto the Transaction Log file for restore on Rollback
			 */
            RecordKey key = CreateKey(collection, blockAddress);

            //// if in recycled or add store, don't register for save...
            //if (RegionLogic.IsSegmentInStore(_recycledCollectionStore, key, segmentSize) || InAddStore(key, segmentSize))
            //    return false;
            //** if in file growth segments, don't register for save...
            Region region = RegionLogic.RemoveIntersections(_fileGrowthStore, key, blockAddress, segmentSize);
            if (region == null || region.Count == 0)
            {
                if (_inCommit == 0)
                    TrackModification(collection.GetTopParent());
                return false;
            }
            //**

            int itemCount = region.Count / 2;
            if (itemCount < 5)
                itemCount = 5;
            var regionsForBackup = new List<KeyValuePair<RecordKey, Region>>(itemCount);
            foreach (KeyValuePair<long, int> area in region)
            {
                key.Address = area.Key;
                Region region2 = RegionLogic.RemoveIntersections(_recycledCollectionStore,
                                                                 key, area.Key, area.Value);
                if (region2 == null || region2.Count <= 0 ||
                    ((LogCollection is SortedDictionaryOnDisk) &&
                     key.Filename == ((SortedDictionaryOnDisk)LogCollection).File.Filename))
                    continue;
                foreach (KeyValuePair<long, int> area2 in region2)
                {
                    key.Address = area2.Key;
                    Region region3 = RegionLogic.RemoveIntersections(_addStore,
                                                                     key, area2.Key, area2.Value);
                    //** Step 2: Backup the "modified" portion(s) of data
                    if (region3 == null || region3.Count <= 0) continue;
                    if (_inCommit == 0)
                        TrackModification(collection.GetTopParent());
                    regionsForBackup.Add(new KeyValuePair<RecordKey, Region>(key, region3));
                }
            }
            if (readPool != null)
                BackupData(regionsForBackup, readPool, writePool);
            else
                BackupData(regionsForBackup);
            return true;
        }

        #region For Keeps: Detect And Merge blocks

        //BackupDataLogKey CurrentLogCollMergingIndex;
        //void DetectAndMergeBlocks()
        //{
        //    if (LogCollection == null || LogCollection.Count < 2)
        //        return;

        //    lock (LogCollection)
        //    {
        //        BackupDataLogValue CurrentValue = null;
        //        //** cycle through(10 per call) all recyclable items and find the one with the right size.
        //        if (CurrentLogCollMergingIndex == null)
        //        {
        //            LogCollection.MoveFirst();
        //            CurrentLogCollMergingIndex = (BackupDataLogKey)LogCollection.CurrentKey;
        //        }
        //        else
        //        {
        //            if (!LogCollection.Search(CurrentLogCollMergingIndex))
        //            {
        //                if (LogCollection.EndOfTree())
        //                    LogCollection.MoveFirst();
        //                CurrentLogCollMergingIndex = (BackupDataLogKey)LogCollection.CurrentKey;
        //            }
        //        }
        //        CurrentValue = (BackupDataLogValue)LogCollection.CurrentValue;
        //        if (!LogCollection.MoveNext() || CurrentValue == null)
        //        {
        //            CurrentLogCollMergingIndex = null;
        //            return;
        //        }
        //        DictionaryEntry de;
        //        BackupDataLogKey k;
        //        short PassCount = 0;
        //        //** Detect and merge contiguous blocks
        //        while (!LogCollection.EndOfTree())
        //        {
        //            de = LogCollection.CurrentEntry;
        //            k = (BackupDataLogKey)de.Key;
        //            BackupDataLogValue v = (BackupDataLogValue)de.Value;
        //            if (k.SourceFilename == CurrentLogCollMergingIndex.SourceFilename)
        //            {
        //                if (CurrentLogCollMergingIndex.SourceDataAddress + CurrentValue.DataSize ==
        //                    k.SourceDataAddress)
        //                {
        //                    long NewSize = CurrentValue.DataSize + v.DataSize;
        //                    if (NewSize <= int.MaxValue)
        //                    {
        //                        CurrentValue.DataSize = (int)NewSize;
        //                        if (LogCollection is SortedDictionaryOnDisk)
        //                            LogCollection[CurrentLogCollMergingIndex] = CurrentValue;
        //                        LogCollection.Remove(k);
        //                        //** no need to update on file as LogCollection is in-memory!
        //                        break;
        //                    }
        //                    else
        //                        throw new Exception(string.Format("NewSize '{0}' bigger than int.MaxValue", NewSize));
        //                }
        //            }
        //            CurrentLogCollMergingIndex = k;
        //            CurrentValue = v;
        //            if (!LogCollection.MoveNext() || ++PassCount >= 2)
        //                break;
        //        }
        //        if (LogCollection.CurrentKey == null)
        //            CurrentLogCollMergingIndex = null;
        //    }
        //}

        #endregion

        private static int _logBackupFilenameLookupCounter;

        private static void RemoveFromLogBackupLookup(string filename)
        {
            //** remove from backup streams list
            var fs = BackupStreams[filename];
            if (fs != null)
            {
                fs.Dispose();
                BackupStreams.Remove(filename);
            }
            //** remove from backup filename lookup list
            int i;
            if (LogBackupFilenameLookup.TryGetValue(filename, out i))
            {
                LogBackupFileHandleLookup.Remove(i);
                LogBackupFilenameLookup.Remove(filename);
            }
        }

        private static int GetLogBackupFileHandle(string filename)
        {
            int i;
            if (!LogBackupFilenameLookup.TryGetValue(filename, out i))
            {
                i = Interlocked.Increment(ref _logBackupFilenameLookupCounter);
                LogBackupFilenameLookup[filename] = i;
                LogBackupFileHandleLookup[i] = filename;
            }
            return i;
        }

        private static string GetLogBackupFilename(int fileHandle)
        {
            return (string)LogBackupFileHandleLookup[fileHandle];
        }

        private static readonly Collections.Generic.ISortedDictionary<int, string> LogBackupFileHandleLookup = new ConcurrentSortedDictionary<int, string>();
        private static readonly Collections.Generic.ISortedDictionary<string, int> LogBackupFilenameLookup = new ConcurrentSortedDictionary<string, int>();
        private static AsyncCallback _readCallback = null;

        private static AsyncCallback ReadCallback
        {
            get { return _readCallback ?? (_readCallback = ReadCallbackHandler); }
        }

        private static void ReadCallbackHandler(IAsyncResult result)
        {
            if (_writeCallback == null)
                _writeCallback = WriteCallback;

            if (!result.IsCompleted)
                result.AsyncWaitHandle.WaitOne();

            //** read in the parameters
            var objParams = (object[])result.AsyncState;
            var param = (ConcurrentIOData[])objParams[0];
            byte[] buffer = param[0].Buffer;
            int res = param[0].FileStream.EndRead(result);

            //** store in cache the block read... used only during backup operation
            if ((bool)objParams[1] && objParams.Length > 2)
            {
                if (objParams[2] is BackupDataLogKey)
                {
                    var lk = (BackupDataLogKey)objParams[2];
                    SetBackupData(lk, buffer, false);
                }
            }
            //** write 
            if (res < buffer.Length)
            {
                if (res == 0)
                {
                    if ((bool)objParams[1] && objParams.Length > 2 && objParams[2] is string)
                    {
                        Logger.LogLine("ReadCallbackHandler: Read from file {0}", objParams[2] as string);
                    }
                }
                //Logger.LogLine("ReadCallbackHandler: read {0} bytes, requested {1}", res, buffer.Length);
            }

            param[1].FileStream.BeginWrite(buffer, 0, res, _writeCallback, param);
        }
        private static Utility.GenericLogger Logger
        {
            get
            {
                if (_logger == null)
                    _logger = new GenericLogger();
                return _logger;
            }
        }
        [ThreadStatic]
        private static Utility.GenericLogger _logger;

        private static AsyncCallback _writeCallback = null;

        internal static void WriteCallback(IAsyncResult result)
        {
            if (!result.IsCompleted)
                result.AsyncWaitHandle.WaitOne();

            var param = (ConcurrentIOData[])result.AsyncState;
            var fs = param[1].FileStream;
            fs.EndWrite(result);
            fs.Flush();
            if (param[0] != null)
                param[0].Event.Set();
            if (param[1] != null)
                param[1].Event.Set();
        }

        private GenericLogger _appendLogger;

        private GenericLogger AppendLogger
        {
            get
            {
                if (_appendLogger == null)
                    _appendLogger = new GenericLogger(string.Format("{0}{1}{2}.txt",
                                                            Server.Path, AppendLogLiteral, Id));
                return _appendLogger;
            }
        }

        private GenericLogger _updateLogger;

        private GenericLogger UpdateLogger
        {
            get
            {
                if (_updateLogger == null)
                    _updateLogger = new GenericLogger(string.Format("{0}{1}{2}.txt",
                                                            Server.Path, UpdateLogLiteral, Id));
                return _updateLogger;
            }
        }

        public string DataBackupFilename;

        private const string UpdateLogLiteral = "UpdateLog";
        private const string AppendLogLiteral = "AppendLog";
        private const string DataBackupFilenameLiteral = "_SystemTransactionDataBackup";
    }
}