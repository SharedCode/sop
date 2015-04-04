// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
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
        // check whether Collection is a Transaction Store or not.
        internal bool IsTransactionStore(CollectionOnDisk collection)
        {
            return DiskBasedMetaLogging && collection.IsTransactionStore;
        }

        #region Register Methods
        internal static bool RegisterFileGrowth(Collections.Generic.ISortedDictionary<RecordKey, long> fileGrowthStore,
                                                CollectionOnDisk collection, long segmentAddress, long segmentSize,
                                                bool recycleCollection)
        {
            fileGrowthStore.Locker.Lock();
            try
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
                                    fileGrowthStore[de.Value.Key] = expandedSegmentSize;
                                    //fileGrowthStore.CurrentValue = expandedSegmentSize;
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
                    string.Format("File '{0}' region '{1}' already expanded.", key.Filename, key.Address)
                    );
            }
            finally
            {
                fileGrowthStore.Locker.Unlock();
            }
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

        protected internal override void RegisterRemove(CollectionOnDisk collection, long blockAddress, int blockSize)
        {
            if (IsTransactionStore(collection))
            {
                ((TransactionBase)collection.ParentTransactionLogger).RegisterRemove(collection, blockAddress, blockSize);
                return;
            }
            if (LogCollection == null) return;


            if (_inCommit == 0)
                TrackModification(collection.GetTopParent());

            // object o = 90;   // todo: remove return when ready...
            return;


            // Check if Block is in Growth, RecycledCollection, Add, Recycled blocks segments...
            RecordKey key = CreateKey(collection, blockAddress);
            if (RegionLogic.IsSegmentInStore(_fileGrowthStore, key, blockSize) ||
                RegionLogic.IsSegmentInStore(_recycledSegmentsStore, key, blockSize) ||
                RegionLogic.IsSegmentInStore(_addBlocksStore, key, blockSize))
                return;
            // check if block is in updated blocks...
            if (IsInUpdatedBlocks(collection, blockAddress, blockSize))
                return;
            AddMerge(_recycledBlocksStore, key, blockSize);
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
        /// Check whether a block is:
        /// - a newly added block
        /// - in new segment
        /// - in recycled store
        /// - in updated blocks
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
            /*  todo: complete the story for: RegisterAdd, ...Save, ...Remove, ...Recycle, ...FileGrowth
             * Logic table:
             * Add          Save (Update)/Remove       Recycle         FileGrowth
             * - FileGrowth blocks can be deleted, then re-allocated for Add
             * - Block can be allocated for Add, Deleted(will create Updated blocks) if item is deleted, then re-Allocated for Add.*/

            // Check if Block is in Growth, RecycledCollection, Recycled blocks segments...
            if (checkIfInGrowthSegments)
            {
                if (RegionLogic.IsSegmentInStore(fileGrowthStore, key, blockSize) ||
                    RegionLogic.IsSegmentInStore(recycledCollectionStore, key, blockSize))
                    return;
            }
            AddMerge(addStore, key, blockSize);
        }

        private static bool InStore(RecordKey key, int blockSize, 
            Collections.Generic.ISortedDictionary<RecordKey, long> addStore)
        {
            if (addStore == null)
                throw new ArgumentNullException("addStore");
            return addStore.Locker.Invoke(() =>
            {
                if (addStore.ContainsKey(key))
                    return true;
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
                return false;
            });
        }

        protected internal override void RegisterRecycleCollection(CollectionOnDisk collection,
                                                                   long blockAddress,
                                                                   int blockSize)
        {
            RecordKey key = CreateKey(collection, blockAddress);
            // add the collection segments to the Recycled Collection Store
            if (!_recycledSegmentsStore.ContainsKey(key))
                RegisterFileGrowth(_recycledSegmentsStore, collection, blockAddress, blockSize, true);
        }

        /// <summary>
        /// Register recycled block.
        /// </summary>
        /// <param name="collection"></param>
        /// <param name="blockAddress"></param>
        /// <param name="blockSize"></param>
        protected internal override bool RegisterRecycle(CollectionOnDisk collection,
                                                         long blockAddress,
                                                         int blockSize)
        {
            if (!RegisterRecycle(_addBlocksStore, _recycledBlocksStore, collection, blockAddress, blockSize))
            {
                using (var writePool = new ConcurrentIOPoolManager())
                {
                    using (var readPool = new ConcurrentIOPoolManager())
                    {
                        RegisterSave(collection, blockAddress, blockSize, readPool, writePool);
                    }
                }
            }
            return true;
        }

        internal static bool RegisterRecycle(
            Collections.Generic.ISortedDictionary<RecordKey, long> addStore,
            Collections.Generic.ISortedDictionary<RecordKey, long> recycleStore,
            CollectionOnDisk collection,
            long blockAddress,
            int blockSize)
        {
            var key = CreateKey(collection, blockAddress);
            //if (InStore(key, blockSize, recycleStore))
            //    return false;

            BackupDataLogKey logKey = new BackupDataLogKey();
            logKey.SourceFilename = collection.File.Filename;
            logKey.SourceDataAddress = blockAddress;
            IEnumerable<KeyValuePair<BackupDataLogKey, BackupDataLogValue>> intersectingLogs;
            long mergedBlockStartAddress, mergedBlockSize;
            if (GetIntersectingLogs(logKey, blockSize, out intersectingLogs,
                                    out mergedBlockStartAddress, out mergedBlockSize))
            {
                if (intersectingLogs == null)
                {
                    RegisterAdd(addStore, null, null, collection, blockAddress, blockSize, false);
                    return true;
                }
                // get area(s) outside each intersecting segment and back it up...
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
                    return true;
                }
            }
            RegisterAdd(addStore, null, null, collection, blockAddress, blockSize, false);
            return true;
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
                RegionLogic.IsSegmentInStore(_recycledSegmentsStore, key, blockSize))
            {
                if (_inCommit == 0)
                    TrackModification(collection.GetTopParent());
                return;
            }
            RegisterAdd(_addBlocksStore, _fileGrowthStore, _recycledSegmentsStore, collection, blockAddress,
                                 blockSize, false);
            if (_inCommit == 0)
                TrackModification(collection.GetTopParent());
        }
        override internal protected void TrackModification(CollectionOnDisk collection, bool untrack = false)
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
        /// those "modified" blocks will be registered & backed up.
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

            LogTracer.Verbose("Transactin.RegisterSave: Start for Thread {0}.", Thread.CurrentThread.ManagedThreadId);

            //Step 1. Remove Intersections with Added, Growth segments & Recycled Blocks from region as no need to backup 
            //         new Blocks
            //Step 2. Copy or backup remaining (Updated) blocks onto the Transaction Log file for restore on Rollback
            RecordKey key = CreateKey(collection, blockAddress);

            // if in file growth segments, don't register for save...
            Region region = RegionLogic.RemoveIntersections(_fileGrowthStore, key, blockAddress, segmentSize);
            if (region == null || region.Count == 0)
            {
                if (_inCommit == 0)
                    TrackModification(collection.GetTopParent());
                return false;
            }

            #region subtract any region intersecting with recycled and add Stores
            int itemCount = region.Count / 2;
            if (itemCount < 5)
                itemCount = 5;
            var regionsForBackup = new List<KeyValuePair<RecordKey, Region>>(itemCount);

            foreach (KeyValuePair<long, int> area in region)
            {
                // subtract regions intersecting with recycled segments
                key.Address = area.Key;
                Region region2 = RegionLogic.RemoveIntersections(_recycledSegmentsStore,
                                                                 key, area.Key, area.Value);

                LogTracer.Verbose("Transactin.RegisterSave: Thread {0}, _recycledSegmentsStore count {1}.", Thread.CurrentThread.ManagedThreadId, _recycledSegmentsStore.Count);

                if (region2 == null || region2.Count <= 0 ||
                    ((LogCollection is SortedDictionaryOnDisk) &&
                     key.Filename == ((SortedDictionaryOnDisk)LogCollection).File.Filename))
                    continue;
                // subtract regions intersecting with (new) add segments
                foreach (KeyValuePair<long, int> area2 in region2)
                {
                    key.Address = area2.Key;
                    var region3 = RegionLogic.RemoveIntersections(_addBlocksStore, key, area2.Key, area2.Value);

                    LogTracer.Verbose("Transactin.RegisterSave: Thread {0}, _addBlocksStore count {1}.", Thread.CurrentThread.ManagedThreadId, _addBlocksStore.Count);

                    if (region3 == null || region3.Count <= 0) continue;
                    foreach (KeyValuePair<long, int> area3 in region3)
                    {
                        key.Address = area3.Key;
                        var region4 = RegionLogic.RemoveIntersections(_recycledBlocksStore, key, area3.Key, area3.Value);

                        LogTracer.Verbose("Transactin.RegisterSave: Thread {0}, _recycledBlocksStore count {1}.", Thread.CurrentThread.ManagedThreadId, _recycledBlocksStore.Count);


                        if (region4 == null || region4.Count <= 0) continue;
                        // any remaining portions are marked for backup
                        if (_inCommit == 0)
                            TrackModification(collection.GetTopParent());
                        regionsForBackup.Add(new KeyValuePair<RecordKey, Region>(key, region4));
                    }
                }
            }

            #endregion
            if (readPool != null)
                BackupData(regionsForBackup, readPool, writePool);
            else
                BackupData(regionsForBackup);
            return true;
        }
        #endregion

        #region Transaction Log Backup helpers
        private static void RemoveFromLogBackupLookup(string filename)
        {
            // remove from backup streams list
            //BackupStreams.Locker.Lock();
            var fs = BackupStreams[filename];
            if (fs != null)
            {
                fs.Dispose();
                BackupStreams.Remove(filename);
            }
            //BackupStreams.Locker.Unlock();
            // remove from backup filename lookup list
            int i;
            //LogBackupFilenameLookup.Locker.Lock();
            if (LogBackupFilenameLookup.TryGetValue(filename, out i))
            {
                LogBackupFileHandleLookup.Remove(i);
                LogBackupFilenameLookup.Remove(filename);
            }
            //LogBackupFilenameLookup.Locker.Unlock();
        }
        private static int GetLogBackupFileHandle(string filename)
        {
            int i;
            //LogBackupFilenameLookup.Locker.Lock();
            if (!LogBackupFilenameLookup.TryGetValue(filename, out i))
            {
                i = Interlocked.Increment(ref _logBackupFilenameLookupCounter);
                LogBackupFilenameLookup[filename] = i;
                LogBackupFileHandleLookup[i] = filename;
            }
            //LogBackupFilenameLookup.Locker.Unlock();
            return i;
        }
        private static string GetLogBackupFilename(int fileHandle)
        {
            return (string)LogBackupFileHandleLookup[fileHandle];
        }
        private static int _logBackupFilenameLookupCounter;
        private static readonly Collections.Generic.ISortedDictionary<int, string> LogBackupFileHandleLookup = new ConcurrentSortedDictionary<int, string>();
        private static readonly Collections.Generic.ISortedDictionary<string, int> LogBackupFilenameLookup = new ConcurrentSortedDictionary<string, int>();
        #endregion

        #region Transaction Reader/Writer callbacks
        private static AsyncCallback ReadCallback
        {
            get
            {
                if (_readCallback == null)
                    _readCallback = ReadCallbackHandler;
                return _readCallback;
            }
        }
        private static void ReadCallbackHandler(IAsyncResult result)
        {
            if (_writeCallback == null)
                _writeCallback = WriteCallback;

            if (!result.IsCompleted)
                result.AsyncWaitHandle.WaitOne();

            // read in the parameters
            var objParams = (object[])result.AsyncState;
            var param = (ConcurrentIOData[])objParams[0];
            byte[] buffer = param[0].Buffer;

            int res;
            try
            {
                #region Uncomment to simulate IO async thread failure
                //if (writeCount >= 0 && writeCount++ > 1)
                //{
                //    writeCount = -1;
                //    throw new IOException("Simulated disk failure.");
                //}
                #endregion
                res = param[0].FileStream.EndRead(result);
            }
            catch (Exception exc)
            {
                param[0].PoolManager.AddException(exc);
                Transaction.LogTracer.Fatal(exc);
                param[0].Event.Set();
                if (param[1] != null)
                    param[1].Event.Set();
                return;
            }

            // signal reader so it can do another reading job...
            param[0].Buffer = null;
            var evt = param[0].Event;
            param[0] = null;
            evt.Set();

            // store in cache the block read... used only during backup operation
            if ((bool)objParams[1] && objParams.Length > 2)
            {
                if (objParams[2] is BackupDataLogKey)
                {
                    var lk = (BackupDataLogKey)objParams[2];
                    SetBackupData(lk, buffer, false);
                }
            }
            // write 
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
            param[1].FileStream.BeginWrite(buffer, 0, res, _writeCallback,
                objParams.Length > 3 && objParams[3] is Sop.VoidFunc ?
                    new object[] { param, objParams[3] } : param);
        }
        internal static void WriteCallback(IAsyncResult result)
        {
            if (!result.IsCompleted)
                result.AsyncWaitHandle.WaitOne();

            ConcurrentIOData[] readerWriter = null;
            Sop.VoidFunc logBackupDelegate = null;
            if (result.AsyncState is ConcurrentIOData[])
                readerWriter = (ConcurrentIOData[])result.AsyncState;
            else if (result.AsyncState is object[])
            {
                object[] parameters = (object[])result.AsyncState;
                if (parameters[0] is ConcurrentIOData[])
                    readerWriter = (ConcurrentIOData[])parameters[0];
                if (parameters.Length > 1 && parameters[1] is Sop.VoidFunc)
                    logBackupDelegate = (Sop.VoidFunc)parameters[1];
            }

            if (readerWriter == null)
                throw new SopException("WriteCallback:ConcurrentIOData parameter is missing. There is a bug!");

            var param = readerWriter;
            var fs = param[1].FileStream;
            try
            {

                #region Uncomment to simulate IO async thread failure
                //if (writeCount >= 0 && writeCount++ > 1)
                //{
                //    writeCount = -1;
                //    throw new IOException("Simulated disk failure.");
                //}
                #endregion

                fs.EndWrite(result, true);
                fs.Flush();
                if (param[0] != null)
                    param[0].Event.Set();
                if (param[1] != null)
                    param[1].Event.Set();
                if (logBackupDelegate != null)
                    logBackupDelegate();
            }
            catch(Exception exc)
            {
                // register to the Pool Manager the caught (IO) Exception 
                // to cause Trans rollback on the caller thread.
                if (param[0] != null)
                    param[0].PoolManager.AddException(exc);
                else if (param[1] != null)
                    param[1].PoolManager.AddException(exc);
                Transaction.LogTracer.Fatal(exc);

                // signal completion
                if (param[0] != null)
                    param[0].Event.Set();
                if (param[1] != null)
                    param[1].Event.Set();
            }
        }

        // comment when done testing IO async thread failure
        //private static int writeCount;

        private static AsyncCallback _writeCallback = null;
        private static AsyncCallback _readCallback = null;
        #endregion

        #region Transaction Loggers
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
        private GenericLogger _appendLogger;

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
        private GenericLogger _updateLogger;

        private const string UpdateLogLiteral = "UpdateLog";
        private const string AppendLogLiteral = "AppendLog";
        #endregion

        public string DataBackupFilename;
        private const string DataBackupFilenameLiteral = "_SystemTransactionDataBackup";
    }
}
