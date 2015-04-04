// Scalable Object Persistence (SOP) Framework, by Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections;
using System.Collections.Generic;

namespace Sop.OnDisk.Geometry
{
    /// <summary>
    /// Disk Region Logic.
    /// </summary>
    internal class RegionLogic
    {
        public bool Equals(long dataAddress1, long size1, long dataAddress2, long size2)
        {
            return dataAddress1 == dataAddress2 && size1 == size2;
        }

        public bool FirstWithinSecond(long dataAddress1, long size1, long dataAddress2, long size2)
        {
            if (dataAddress2 >= 0 && size2 > 0)
            {
                if (dataAddress1 >= dataAddress2)
                {
                    if (dataAddress1 + size1 <= dataAddress2 + size2)
                        return true;
                }
            }
            return false;
        }

        public bool Intersect(long dataAddress1, long size1, long dataAddress2, long size2)
        {
            if (FirstIntersectWithSecond(dataAddress1, size1, dataAddress2, size2))
                return true;
            if (FirstIntersectWithSecond(dataAddress2, size2, dataAddress1, size1))
                return true;
            return false;
        }

        public bool FirstIntersectWithSecond(long dataAddress1, long size1, long dataAddress2, long size2)
        {
            if (dataAddress1 >= dataAddress2)
            {
                if (dataAddress1 + size1 <= dataAddress2 + size2)
                    return true;
                if (dataAddress1 < dataAddress2 + size2)
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Checks whether a certain area on disk (segment)
        /// is inclusive on any of segment entries of the store.
        /// </summary>
        /// <param name="fileGrowthStore"></param>
        /// <param name="key"></param>
        /// <param name="blockSize"></param>
        /// <returns></returns>
        public bool IsSegmentInStore(
            Collections.Generic.ISortedDictionary<Transaction.Transaction.RecordKey, long> fileGrowthStore, Transaction.Transaction.RecordKey key, int blockSize)
        {
            if (fileGrowthStore.ContainsKey(key))
                return true;
            KeyValuePair<Transaction.Transaction.RecordKey, long>? de;
            Transaction.Transaction.RecordKey k2;
            short passCount = 0;
            if (!fileGrowthStore.MovePrevious())
                fileGrowthStore.MoveFirst();
            while (!fileGrowthStore.EndOfTree())
            {
                de = fileGrowthStore.CurrentEntry;
                k2 = de.Value.Key;
                if (k2.ServerSystemFilename == key.ServerSystemFilename &&
                    k2.Filename == key.Filename)
                {
                    long i = (long)de.Value.Value;
                    if (key.Address >= k2.Address && key.Address + blockSize <= k2.Address + i)
                        return true;
                }
                else if (++passCount >= 2)
                    break;
                fileGrowthStore.MoveNext();
            }
            return false;
        }

        /// <summary>
        /// RemoveIntersections will check whether an area on disk was already
        /// inclusive in any of the segment areas stored as entries in a store (addStore).
        /// If input area is fully inclusive, this function returns null, otherwise
        /// it will return a region equivalent to the input area minus any intersecting
        /// area(s) of segment(s) in the store. 
        /// </summary>
        /// <param name="addStore"></param>
        /// <param name="key"></param>
        /// <param name="blockAddress"></param>
        /// <param name="segmentSize"></param>
        /// <returns></returns>
        public Region RemoveIntersections(
            Collections.Generic.ISortedDictionary<Transaction.Transaction.RecordKey, long> addStore,
            Transaction.Transaction.RecordKey key,
            long blockAddress, int segmentSize)
        {
            if (addStore.Search(key))
            {
                long size = (long)addStore.CurrentValue;
                if (size >= segmentSize)
                    return null;
            }
            else if (!addStore.MovePrevious())
                addStore.MoveFirst();

            //** Step 1
            //** Starting from current block until block whose address is > BlockAddress + SegmentSize...
            //** long[0] = Address
            //** long[1] = Size
            var region = new Region(blockAddress, segmentSize);
            Transaction.Transaction.RecordKey k2;
            short passCount = 0;
            while (!addStore.EndOfTree())
            {
                var de = addStore.CurrentEntry;
                k2 = de.Value.Key;
                if (k2.ServerSystemFilename == key.ServerSystemFilename &&
                    k2.Filename == key.Filename)
                // && k2.CollectionName == key.CollectionName)
                {
                    long size = de.Value.Value;
                    if (k2.Address >= blockAddress + segmentSize)
                        break;
                    if (size > int.MaxValue)
                        throw new InvalidOperationException(
                            string.Format(
                                "Updated segment Size({0} reached > int.MaxValue which isn't supported in a transaction. Keep your transaction smaller by Committing more often",
                                size));
                    if (Intersect(k2.Address, (int)size, blockAddress, segmentSize))
                    {
                        region.Subtract(k2.Address, (int)size);
                        if (region.Count == 0)
                            return null;
                    }
                    else if (++passCount >= 2)
                        break;
                }
                else if (++passCount >= 2)
                    break;
                if (!addStore.MoveNext())
                    break;
            }
            return region;
        }
    }
}
