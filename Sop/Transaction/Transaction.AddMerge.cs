// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: MIT
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

    internal partial class Transaction
    {

        private bool IsInUpdatedBlocks(CollectionOnDisk collection, long blockAddress, int blockSize)
        {
            BackupDataLogKey logKey = new BackupDataLogKey();
            logKey.SourceFilename = collection.File.Filename;
            logKey.SourceDataAddress = blockAddress;
            IEnumerable<KeyValuePair<BackupDataLogKey, BackupDataLogValue>> intersectingLogs;
            long mergedBlockStartAddress, mergedBlockSize;
            return GetIntersectingLogs(logKey, blockSize, out intersectingLogs, out mergedBlockStartAddress,
                                            out mergedBlockSize) &&
                                            intersectingLogs == null;
        }

        internal static void AddMerge(
            Collections.Generic.ISortedDictionary<RecordKey, long> addStore, RecordKey key, int blockSize)
        {
            addStore.Locker.Invoke(() =>
            {
                // Add Block to AddStore for use on Rollback...
                if (!addStore.ContainsKey(key))
                {
                    short passCount = 0;
                    // Detect and merge contiguous blocks
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
                                addStore[de.Value.Key] = i + blockSize;
                                //addStore.CurrentValue = i + blockSize;
                                return;
                            }
                            if (key.Address >= k2.Address && key.Address + blockSize <= k2.Address + i)
                                // if block is inclusive, don't do anything...
                                return;
                        }
                        else if (++passCount >= 2)
                            break;
                        addStore.MoveNext();
                    }
                    addStore.Add(key, blockSize);
                }
            });
        }
   }
}