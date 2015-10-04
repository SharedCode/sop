// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
// Have fun Coding! ;)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.IO;
using Sop.Collections.Generic;
using Sop.Mru.Generic;
using Sop.OnDisk.Geometry;
using Sop.OnDisk.IO;
using Sop.Utility;
using File = Sop.OnDisk.File.File;
using FileStream = Sop.OnDisk.File.FileStream;

namespace Sop.Transaction
{
    using OnDisk;

    /// <summary>
    /// Transaction contains the Begin, Commit and Rollback
    /// transaction methods' implementation.
    /// </summary>
    internal partial class Transaction
    {
        private static byte[] GetBackupData(BackupDataLogKey logKey, int dataSize)
        {
            if (_backupCache != null)
            {
                return _backupCache[logKey];

                #region Under study: Bigger than Sop.DataBlock buffering!

                //byte[] r = (byte[])BackupCache[LogKey];
                //if (r == null)
                //{
                //    if (!BackupCache.CacheCollection.MovePrevious())
                //        BackupCache.CacheCollection.MoveFirst();
                //    for (int i = 0; i < 3; i++)
                //    {
                //        BackupDataLogKey lk = (BackupDataLogKey)BackupCache.CacheCollection.CurrentKey;

                //    if (Collections.OnDisk.Algorithm._region.FirstWithinSecond(Address1, Size1, Address2, Size2))
                //        return true;
                //    else if (Collections.OnDisk.Algorithm._region.Intersect(Address1, Size1, Address2, Size2)


                //    }
                //}
                //if (r != null)
                //{
                //    if (r.Length >= DataSize)
                //        return r;
                //}

                #endregion
            }
            return null;
        }

        private static void SetBackupData(BackupDataLogKey logKey, byte[] data, bool forceCached)
        {
            if (_backupCache != null &&
                data.Length == (int)logKey.DataBlockSize &&
                (forceCached || _backupCache.Count < _backupCache.MinCapacity))
            {
                _backupCache[logKey] = data;
            }
        }

        /// <summary>
        /// Read Block from log backup file
        /// </summary>
        /// <param name="collection"></param>
        /// <param name="dataAddress"></param>
        /// <param name="getForRemoval"></param>
        /// <param name="readMetaInfoOnly"></param>
        /// <returns></returns>
        public static byte[] ReadBlockFromBackup(OnDisk.Algorithm.Collection.ICollectionOnDisk collection,
                                                 long dataAddress,
                                                 bool getForRemoval,
                                                 bool readMetaInfoOnly)
        {
            if (!IsGlobal && LogCollection != null)
            {
                var LogKey = new BackupDataLogKey();
                LogKey.SourceFilename = collection.File.Filename;
                LogKey.SourceDataAddress = dataAddress;

                byte[] blockBuffer = GetBackupData(LogKey, (int) collection.DataBlockSize);
                if (blockBuffer == null)
                {
                    var lv = LogCollection[LogKey];
                    if (lv != null)
                    {
                        //** read from disk the block and encache it..
                        int BlockSize;
                        //BackupStreams.Locker.Lock();
                        string fname = GetLogBackupFilename(lv.BackupFileHandle);
                        var fs = BackupStreams[fname];
                        if (fs == null)
                        {
                            string fname2 = GetLogBackupFilename(lv.BackupFileHandle);
                            if (collection.File.Server != null)
                                fname2 = collection.File.Server.NormalizePath(fname2);
                            fs = File.UnbufferedOpen(fname2,
                                                     FileAccess.Read, (int)collection.DataBlockSize, out BlockSize);
                            BackupStreams[fname] = fs;
                        }
                        //BackupStreams.Locker.Unlock();
                        blockBuffer = new byte[(int)collection.DataBlockSize];
                        fs.Seek(dataAddress, SeekOrigin.Begin);
                        if (fs.Read(blockBuffer, 0, blockBuffer.Length) <= 0)
                            throw new SopException("Read failed on Transaction.ReadBlockFromBackup.");
                        SetBackupData(LogKey, blockBuffer, true);
                    }
                }
                return blockBuffer;
            }
            return null;
        }

        private void ClearBackupStreams()
        {
            //BackupStreams.Locker.Lock();
            foreach(var kvp in BackupStreams)
            {
                kvp.Value.Dispose();
            }
            BackupStreams.Clear();
            //BackupStreams.Locker.Unlock();
        }
        private static readonly Collections.Generic.ISortedDictionary<string, FileStream> BackupStreams = new ConcurrentSortedDictionary<string, FileStream>();

        //protected virtual void ProcessTransactionConflicts(BackupDataLogKey logKey, int logKeySize)
        //{
        //    //** process conflicts with other trans and register
        //}
        #region Backup Data from SOP Data file(s) onto transaction backup log file.
        /// <summary>
        /// Backup Data of a certain disk region onto the transaction log file
        /// </summary>
        internal void BackupData(List<KeyValuePair<RecordKey, Region>> dataRegions)
        {
            using(var writePool = new ConcurrentIOPoolManager())
            {
                using (var readPool = new ConcurrentIOPoolManager())
                {
                    BackupData(dataRegions, readPool, writePool);
                }
            }
        }

        /// <summary>
        /// Backup Data of a certain disk region onto the transaction log file
        /// </summary>
        internal void BackupData(List<KeyValuePair<RecordKey, Region>> dataRegions,
            ConcurrentIOPoolManager readPool, ConcurrentIOPoolManager writePool)
        {
            LogTracer.Verbose("BackupData: Start for Thread {0}.", Thread.CurrentThread.ManagedThreadId);
            foreach (KeyValuePair<RecordKey, Region> dataRegion in dataRegions)
            {
                RecordKey key = dataRegion.Key;
                Region region = dataRegion.Value;

                var f = (OnDisk.File.IFile)Server.GetFile(key.Filename);
                string fFilename = key.Filename;

                //** foreach disk area in region, copy it to transaction file
                foreach (KeyValuePair<long, int> area in region)
                {
                    // short circuit if IO exception was detected.
                    if (readPool.AsyncThreadException != null)
                        throw readPool.AsyncThreadException;
                    if (writePool.AsyncThreadException != null)
                        throw writePool.AsyncThreadException;

                    var logKey = new BackupDataLogKey();
                    logKey.SourceFilename = f == null ? fFilename : f.Filename;
                    logKey.SourceDataAddress = area.Key;

                    IEnumerable<KeyValuePair<BackupDataLogKey, BackupDataLogValue>> intersectingLogs;
                    long mergedBlockStartAddress, mergedBlockSize;

                    // todo: optimize LogCollection locking!

                    //LogCollection.Locker.Lock();
                    LogTracer.Verbose("Transactin.BackupData: Thread {0}, Locking LogCollection, count {1}.", Thread.CurrentThread.ManagedThreadId, LogCollection.Count);

                    bool isIntersectingLogs = GetIntersectingLogs(logKey, area.Value, out intersectingLogs, out mergedBlockStartAddress,
                                            out mergedBlockSize);
                    if (isIntersectingLogs)
                    {
                        BackupDataWithIntersection(intersectingLogs, logKey, area, f, fFilename, readPool, writePool,
                                                   key);
                    }
                    else
                    {
                        BackupDataWithNoIntersection(intersectingLogs, logKey, area, f, fFilename, readPool, writePool,
                                                     key);
                    }

                    LogTracer.Verbose("Transactin.BackupData: Thread {0}, Unlocking LogCollection, count {1}.", Thread.CurrentThread.ManagedThreadId, LogCollection.Count);
                    //LogCollection.Locker.Unlock();
                }
            }
        }

        #region Backup File Grow and Get Size related
        private long BackupFileSize;
        private long ActualBackupFileSize;
        private long GrowBackupFile(long growthSize, FileStream writer = null)
        {
            const int GrowthSizeChunk = 4 * 1024 * 1024;
            long newSize, r;
            lock (this)
            {
                r = BackupFileSize;
                BackupFileSize += growthSize;
                newSize = BackupFileSize;
                //r = Interlocked.Read(ref BackupFileSize);
                //newSize = Interlocked.Add(ref BackupFileSize, growthSize);
                if (newSize < ActualBackupFileSize || writer == null)
                    return r;
                // File resize across threads is not safe (can corrupt/overstep), so lock before file resize
                // resize by growth file chunk size...
                int chunkCount = 1;
                if (newSize > GrowthSizeChunk)
                {
                    chunkCount = (int)(newSize / GrowthSizeChunk);
                    if (newSize % GrowthSizeChunk == 0 ||
                        newSize % GrowthSizeChunk >= (int)GrowthSizeChunk / 2)
                        chunkCount++;
                }
                ActualBackupFileSize += (chunkCount * GrowthSizeChunk);
                if (writer != null)
                    writer.SetLength(ActualBackupFileSize, true);
            }
            return r;
        }
        #endregion

        private void BackupDataWithIntersection(
            IEnumerable<KeyValuePair<BackupDataLogKey, BackupDataLogValue>> intersectingLogs,
            BackupDataLogKey logKey, KeyValuePair<long, int> area, OnDisk.File.IFile f, string fFilename,
            ConcurrentIOPoolManager readPool, ConcurrentIOPoolManager writePool,
            RecordKey key
            )
        {
            if (intersectingLogs == null)
            {
                // process conflicts with other trans...
                //ProcessTransactionConflicts(logKey, area.Value);
                // area is within an already backed up area (intersectingLogs == null), do nothing...
                return;
            }
            LogTracer.Verbose("BackupDataWithIntersection: Start for Thread {0}.", Thread.CurrentThread.ManagedThreadId);

            // get area(s) outside each intersecting segment and back it up...
            var newRegion = new Region(area.Key, area.Value);
            #region for future implements... ?
            //bool wasIntersected = false;
            //foreach (KeyValuePair<BackupDataLogKey, BackupDataLogValue> entry in intersectingLogs)
            //{
            //    // process conflicts with other trans...
            //    ProcessTransactionConflicts(entry.Key, entry.Value.DataSize);
            //    if (newRegion.Subtract(entry.Key.SourceDataAddress, entry.Value.DataSize))
            //        wasIntersected = true;
            //}
            //if (!wasIntersected) return;
            #endregion

            // copy modified blocks to the transaction backup file.
            foreach (KeyValuePair<long, int> newArea in newRegion)
            {
                if (readPool.AsyncThreadException != null)
                    throw readPool.AsyncThreadException;
                if (writePool.AsyncThreadException != null)
                    throw writePool.AsyncThreadException;

                var logKey2 = new BackupDataLogKey();
                logKey2.SourceFilename = logKey.SourceFilename;
                logKey2.SourceDataAddress = newArea.Key;

                var logValue = new BackupDataLogValue();
                logValue.DataSize = newArea.Value;
                logValue.TransactionId = Id;

                int newSize = newArea.Value;
                key.Address = newArea.Key;
                //if (RegisterAdd(_addBlocksStore, null, null, key, newArea.Value, false))
                //    return;

                logValue.BackupFileHandle = GetLogBackupFileHandle(DataBackupFilename);
                ConcurrentIOData reader = f != null
                                              ? readPool.GetInstance(f, newArea.Value)
                                              : readPool.GetInstance(fFilename, null, newArea.Value);
                if (reader == null)
                    throw new InvalidOperationException("Can't get ConcurrentIOData from ReadPool");
                string systemBackupFilename = Server.Path + DataBackupFilename;
                ConcurrentIOData writer = writePool.GetInstance(systemBackupFilename, ((TransactionRoot)Root));
                if (writer == null)
                    throw new InvalidOperationException("Can't get ConcurrentIOData from WritePool");

                // return the current backup file size and grow it to make room for data to be backed up...
                logValue.BackupDataAddress = GrowBackupFile(newSize, writer.FileStream);

                // save a record of the backed up data..
                LogCollection.Add(logKey2, logValue);

                // prepare lambda expression to log after data was backed up!!
                Sop.VoidFunc logBackedupData = () =>
                {
                    UpdateLogger.LogLine(
                    "{0}{1}:{2} to {3}:{4} Size={5}", BackupFromToken, logKey2.SourceFilename,
                    logKey2.SourceDataAddress, DataBackupFilename, logValue.BackupDataAddress, newSize);
                };

                writer.FileStream.Seek(logValue.BackupDataAddress, SeekOrigin.Begin, true);
                reader.FileStream.Seek(newArea.Key, SeekOrigin.Begin, true);
                reader.FileStream.BeginRead(
                    reader.Buffer, 0, newSize, ReadCallback,
                    new object[] { new[] { reader, writer }, true, logKey2, logBackedupData });
            }
        }


        private void BackupDataWithNoIntersection(
            IEnumerable<KeyValuePair<BackupDataLogKey, BackupDataLogValue>> intersectingLogs,
            BackupDataLogKey logKey, KeyValuePair<long, int> area, OnDisk.File.IFile f, string fFilename,
            ConcurrentIOPoolManager readPool, ConcurrentIOPoolManager writePool,
            RecordKey key)
        {
            string systemBackupFilename = Server.Path + DataBackupFilename;
            int size = area.Value;

            key.Address = area.Key;

            // no intersection nor mergeable logs, add new log! backup and log the data area
            ConcurrentIOData reader = f != null
                                          ? readPool.GetInstance(f, size)
                                          : readPool.GetInstance(fFilename, null, size);
            ConcurrentIOData writer = writePool.GetInstance(systemBackupFilename, (TransactionRoot)Root);

            if (reader == null || writer == null)
            {
                throw new SopException("This program has a bug! 'didn't get reader or writer from Async IO Pool.");
            }

            LogTracer.Verbose("BackupDataWithNoIntersection: Start for Thread {0}.", Thread.CurrentThread.ManagedThreadId);


            var logValue = new BackupDataLogValue();
            logValue.DataSize = size;
            logValue.TransactionId = Id;

            logValue.BackupFileHandle = GetLogBackupFileHandle(DataBackupFilename);

            // return the current backup file size and grow it to make room for data to be backed up...
            logValue.BackupDataAddress = GrowBackupFile(size, writer.FileStream);

            // save a record of the backed up data..
            LogCollection.Add(logKey, logValue);

            // log after data was backed up!!
            Sop.VoidFunc logBackedupData = () =>
            {
                UpdateLogger.LogLine("{0}{1}:{2} to {3}:{4} Size={5}",
                                     BackupFromToken, f != null ? f.Filename : fFilename, area.Key,
                                     DataBackupFilename, logValue.BackupDataAddress, size);
            };

            writer.FileStream.Seek(logValue.BackupDataAddress, SeekOrigin.Begin, true);
            reader.FileStream.Seek(area.Key, SeekOrigin.Begin, true);
            reader.FileStream.BeginRead(
                reader.Buffer, 0, size, ReadCallback,
                new object[] { new[] { reader, writer }, true, logKey, logBackedupData });
        }

        #endregion

        /// <summary>
        /// true means there is one and only one transaction and all changes
        /// are seen globally. The only Transaction is either committed or rolled back.
        /// </summary>
        public static bool IsGlobal
        {
            get
            {
                return _isGlobal == null || _isGlobal.Value;
            }
            protected internal set
            {
                if (_isGlobal != null && _isGlobal.Value != value)
                    throw new InvalidOperationException("Can't set Transaction.IsGlobal as is already set.");
                _isGlobal = value;
            }
        }

        /// <summary>
        /// Returns true if the data segment intersects or totally within 
        /// any of the log (already backed up!) entries. If the latter, target
        /// will be null to denote there is no need to backup this segment.
        /// </summary>
        /// <param name="logKey"></param>
        /// <param name="logKeySize"></param>
        /// <param name="target"></param>
        /// <param name="startMergedBlockAddress"></param>
        /// <param name="mergedBlockSize"></param>
        /// <returns></returns>
        internal static bool GetIntersectingLogs(BackupDataLogKey logKey, int logKeySize,
                                                 out IEnumerable<KeyValuePair<BackupDataLogKey, BackupDataLogValue>>
                                                     target,
                                                 out long startMergedBlockAddress, out long mergedBlockSize)
        {
            target = null;
            startMergedBlockAddress = mergedBlockSize = 0;
            var l = new List<KeyValuePair<BackupDataLogKey, BackupDataLogValue>>();
            LogCollection.Locker.Lock();
            try
            {
                if (!LogCollection.Search(logKey))
                {
                    if (!LogCollection.MovePrevious())
                    {
                        if (!LogCollection.MoveFirst())
                            return false;
                    }
                }
                long address1 = logKey.SourceDataAddress;
                int size1 = logKeySize;
                startMergedBlockAddress = address1;
                mergedBlockSize = size1;
                bool intersected = false;
                for (int i = 0; i < 3; i++)
                {
                    var key = LogCollection.CurrentKey;
                    var value = LogCollection.CurrentValue;
                    if (logKey.SourceFilename == key.SourceFilename)
                    {
                        long address2 = key.SourceDataAddress;
                        int size2 = value.DataSize;
                        if (RegionLogic.FirstWithinSecond(address1, size1, address2, size2))
                            return true;
                        if (RegionLogic.Intersect(address1, size1, address2, size2))
                        {
                            l.Add(new KeyValuePair<BackupDataLogKey, BackupDataLogValue>(key, value));
                            i = 0;
                            intersected = true;
                            if (address2 < startMergedBlockAddress)
                                startMergedBlockAddress = address2;
                            if (startMergedBlockAddress + mergedBlockSize < address2 + size2)
                            {
                                long l2 = address2 + size2 - startMergedBlockAddress;
                                if (l2 >= int.MaxValue)
                                    break;
                                mergedBlockSize = l2;
                            }
                        }
                        else if (intersected)
                            break;
                    }
                    else
                        break;
                    if (!LogCollection.MoveNext())
                        break;
                }
                if (l.Count > 0)
                {
                    target = l;
                    return true;
                }
                return false;
            }
            finally
            {
                LogCollection.Locker.Unlock();
            }
        }

        private static bool? _isGlobal;
        private static IMruManager<BackupDataLogKey, byte[]> _backupCache;
        internal static Collections.Generic.ISortedDictionary<BackupDataLogKey, BackupDataLogValue> LogCollection;

        //private static readonly Logger Logger = Logger.Instance;
        internal static RegionLogic RegionLogic = new RegionLogic();
    }
}
