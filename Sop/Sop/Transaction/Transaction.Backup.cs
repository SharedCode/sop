// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
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

                //** NOTE: study(!)- optimize BackupCache use to be 1 instance per Transaction   90;

                byte[] blockBuffer = GetBackupData(LogKey, (int) collection.DataBlockSize);
                if (blockBuffer == null)
                {
                    var lv = LogCollection[LogKey];
                    if (lv != null)
                    {
                        //** read from disk the block and encache it..
                        int BlockSize;
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
            foreach(var kvp in BackupStreams)
            {
                kvp.Value.Dispose();
            }
            BackupStreams.Clear();
        }
        private static readonly Collections.Generic.ISortedDictionary<string, FileStream> BackupStreams = new ConcurrentSortedDictionary<string, FileStream>();

        protected virtual void ProcessTransactionConflicts(BackupDataLogKey logKey, int logKeySize)
        {
            //** process conflicts with other trans and register
        }
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
            foreach (KeyValuePair<RecordKey, Region> dataRegion in dataRegions)
            {
                RecordKey key = dataRegion.Key;
                Region region = dataRegion.Value;

                var f = (OnDisk.File.IFile)Server.GetFile(key.Filename);
                string fFilename = key.Filename;

                //** foreach disk area in region, copy it to transaction file
                foreach (KeyValuePair<long, int> area in region)
                {
                    var logKey = new BackupDataLogKey();
                    logKey.SourceFilename = f == null ? fFilename : f.Filename;
                    logKey.SourceDataAddress = area.Key;

                    IEnumerable<KeyValuePair<BackupDataLogKey, BackupDataLogValue>> intersectingLogs;
                    long mergedBlockStartAddress, mergedBlockSize;
                    if (GetIntersectingLogs(logKey, area.Value, out intersectingLogs, out mergedBlockStartAddress,
                                            out mergedBlockSize))
                    {
                        BackupDataWithIntersection(intersectingLogs, logKey, area, f, fFilename, readPool, writePool,
                                                   key);
                    }
                    else
                    {
                        BackupDataWithNoIntersection(intersectingLogs, logKey, area, f, fFilename, readPool, writePool,
                                                     key);
                    }
                }

                //** Detect and Merge backed up blocks to minimize growth of Items stored in LogCollection,
                //** impact of not merging is a slower rollback of an unfinished pending transaction in previous run.
                //DetectAndMergeBlocks();
            }
        }
        private void BackupDataWithIntersection(
            IEnumerable<KeyValuePair<BackupDataLogKey, BackupDataLogValue>> intersectingLogs,
            BackupDataLogKey logKey, KeyValuePair<long, int> area, OnDisk.File.IFile f, string fFilename,
            ConcurrentIOPoolManager readPool, ConcurrentIOPoolManager writePool,
            RecordKey key
            )
        {
            if (intersectingLogs == null)
            {
                //** process conflicts with other trans...
                ProcessTransactionConflicts(logKey, area.Value);
                //** area is within an already backed up area (intersectingLogs == null), do nothing...
                return;
            }
            //** get area(s) outside each intersecting segment and back it up...
            var newRegion = new Region(area.Key, area.Value);
            bool wasIntersected = false;
            foreach (KeyValuePair<BackupDataLogKey, BackupDataLogValue> entry in intersectingLogs)
            {
                //** process conflicts with other trans...
                ProcessTransactionConflicts(entry.Key, entry.Value.DataSize);
                if (newRegion.Subtract(entry.Key.SourceDataAddress, entry.Value.DataSize))
                    wasIntersected = true;
            }
            //** copy
            if (!wasIntersected) return;
            foreach (KeyValuePair<long, int> newArea in newRegion)
            {
                var logKey2 = new BackupDataLogKey();
                logKey2.SourceFilename = logKey.SourceFilename;
                logKey2.SourceDataAddress = newArea.Key;

                var logValue = new BackupDataLogValue();
                logValue.DataSize = newArea.Value;
                logValue.TransactionId = Id;

                int newSize = newArea.Value;
                key.Address = newArea.Key;
                //if (RegisterAdd(_addStore, null, null, key, newArea.Value, false))
                //    return;

                logValue.BackupFileHandle = GetLogBackupFileHandle(DataBackupFilename);
                ConcurrentIOData reader = f != null
                                              ? readPool.GetInstance(f, newArea.Value)
                                              : readPool.GetInstance(fFilename, null, newArea.Value);
                if (reader == null)
                    throw new InvalidOperationException("Can't get ConcurrentIOData from ReadPool");
                string systemBackupFilename = Server.Path + DataBackupFilename;
                ConcurrentIOData writer = writePool.GetInstance(systemBackupFilename, ((TransactionRoot) Root),
                                                                newArea.Value);
                if (writer == null)
                    throw new InvalidOperationException("Can't get ConcurrentIOData from WritePool");

                logValue.BackupDataAddress = writer.FileStream.Seek(0, SeekOrigin.End);


                //** todo: can we remove this block:
                //long readerFileSize = reader.FileStream.Length;
                //if (newArea.Key + newArea.Value > readerFileSize)
                //{
                //    int appendSize = (int)(newArea.Key + newArea.Value - readerFileSize);
                //    key.Address = readerFileSize;
                //    RegisterAdd(_addStore, null, null, key, appendSize, false);
                //    newSize = (int)(readerFileSize - newArea.Key);
                //    logValue.DataSize = newSize;
                //    reader.Buffer = new byte[newSize];
                //}
                //**


                reader.FileStream.Seek(newArea.Key, SeekOrigin.Begin);
                UpdateLogger.LogLine(
                    "{0}{1}:{2} to {3}:{4} Size={5}", BackupFromToken, logKey2.SourceFilename,
                    logKey2.SourceDataAddress,
                    DataBackupFilename, logValue.BackupDataAddress, newSize);

                // resize target file to accomodate data to be copied.
                writer.FileStream.Seek(newSize, SeekOrigin.End);
                writer.FileStream.Seek(logValue.BackupDataAddress, SeekOrigin.Begin);

                reader.FileStream.BeginRead(
                    reader.Buffer, 0, newSize, ReadCallback,
                    new object[] {new[] {reader, writer}, true, logKey2}
                    );

                //** save a record of the backed up data..
                LogCollection.Add(logKey2, logValue);
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
            //if (RegisterAdd(_addStore, null, null, key, size, false))
            //{
            //    Logger.LogLine("Extending, skipping Backup...");
            //    return;
            //}

            //** no intersection nor mergeable logs, add new log!
            //** backup and log the data area
            ConcurrentIOData reader = f != null
                                          ? readPool.GetInstance(f, size)
                                          : readPool.GetInstance(fFilename, null, size);
            ConcurrentIOData writer = writePool.GetInstance(systemBackupFilename, (TransactionRoot) Root, size);

            if (reader == null || writer == null)
                return;

            var logValue = new BackupDataLogValue();
            logValue.DataSize = size;
            logValue.TransactionId = Id;

            //** todo: can we remove this block:
            //long readerFileSize = reader.FileStream.Length;
            //if (area.Key + size > readerFileSize)
            //{
            //    int appendSize = (int)(area.Key + size - readerFileSize);
            //    key.Address = readerFileSize;
            //    RegisterAdd(_addStore, null, null, key, appendSize, false);
            //    size = (int)(readerFileSize - area.Key);
            //    logValue.DataSize = size;
            //    reader.Buffer = new byte[size];
            //}
            //**

            reader.FileStream.Seek(area.Key, SeekOrigin.Begin);

            logValue.BackupFileHandle = GetLogBackupFileHandle(DataBackupFilename);
            logValue.BackupDataAddress = writer.FileStream.Seek(0, SeekOrigin.End);

            UpdateLogger.LogLine("{0}{1}:{2} to {3}:{4} Size={5}",
                                 BackupFromToken, f != null ? f.Filename : fFilename, area.Key,
                                 DataBackupFilename, logValue.BackupDataAddress, size);

            // resize target file to accomodate data to be copied.
            writer.FileStream.Seek(size, SeekOrigin.End);
            writer.FileStream.Seek(logValue.BackupDataAddress, SeekOrigin.Begin);

            reader.FileStream.BeginRead(
                reader.Buffer, 0, size, ReadCallback,
                new object[] {new[] {reader, writer}, true, logKey}
                );

            //** save a record of the backed up data..
            LogCollection.Add(logKey, logValue);
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

        internal static bool GetIntersectingLogs(BackupDataLogKey logKey, int logKeySize,
                                                 out IEnumerable<KeyValuePair<BackupDataLogKey, BackupDataLogValue>>
                                                     target,
                                                 out long startMergedBlockAddress, out long mergedBlockSize)
        {
            target = null;
            startMergedBlockAddress = mergedBlockSize = 0;
            var l = new List<KeyValuePair<BackupDataLogKey, BackupDataLogValue>>();
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

        private static bool? _isGlobal;
        private static IMruManager<BackupDataLogKey, byte[]> _backupCache;
        internal static Collections.Generic.ISortedDictionary<BackupDataLogKey, BackupDataLogValue> LogCollection;

        //private static readonly Logger Logger = Logger.Instance;
        internal static RegionLogic RegionLogic = new RegionLogic();
    }
}
