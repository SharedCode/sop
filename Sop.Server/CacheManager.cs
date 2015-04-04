using System;
using System.Collections.Generic;
using System.Linq;

using Sop;

namespace EFCachingProvider.ScalableCache
{
    /* 
     * Optimization Design:
     * - Use ConcurrentSortedDictionaryOnDisk to provide built-in multi-reader 
     *      on cached MRU Items and Dependent Entity Set
     *      NOTE: implement ConcurrentSortedDictionaryOnDisk with multi-reader 
     *      allowed on MRU items and exclusive lock otherwise
     *      UPDATE: this is not possible as SDOD MRU contains Nodes not Items.
     *
     * 
     * - Batch add for faster inserts
     *      NOTE: Add item batch 1st then add each batch of dependent entity sets
     * - Batch delete of invalidated items
     * - Batch update for reduced client induced OnDisk store writer locks
     * 
     * - Batch delete of expired items occur on the background periodically
     * - Commit occurs in background periodically
     */

    internal class CacheManager
    {
        internal readonly BatchContainer CurrentBatches = new BatchContainer();
        private readonly BatchContainer _previousBatches = new BatchContainer();

        /// <summary>
        /// Allows external code to set its method to get date time.
        /// NOTE: this allows unit test code to set date time to some
        /// test driven values.
        /// </summary>
        public Func<DateTime> GetCurrentDate
        {
            get { return _getCurrentDate; }
            set { _getCurrentDate = value; }
        }
        private Func<DateTime> _getCurrentDate = () => DateTime.Now;

        public void CopyClearCache(out BatchContainer mappingBatches)
        {
            CurrentBatches.LockAll(OperationType.Write);
            CurrentBatches.Copy(_previousBatches);
            CurrentBatches.Clear();
            mappingBatches = _previousBatches;
            CurrentBatches.UnlockAll(OperationType.Write);
        }

        /// <summary>
        /// Store in cache the Item read from disk.
        /// NOTE: background processor should process Updated time stamp items as
        /// to synchronize equivalent entries on disk.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="entry"></param>
        public bool CacheItem(string key, CacheEntry entry)
        {
            if (CurrentBatches.IsInvalidItem(entry) || _previousBatches.IsInvalidItem(entry))
                return false;
            CurrentBatches.Lock(OperationType.Write, BatchContainer.BatchType.AddItems);
            if (entry.SlidingExpiration > TimeSpan.Zero)
                entry.ExpirationTime = GetCurrentDate().Add(entry.SlidingExpiration);
            CurrentBatches.AddItems[key] = entry;
            CurrentBatches.Unlock(OperationType.Write, BatchContainer.BatchType.AddItems);
            return true;
        }

        /// <summary>
        /// Tries to the get cached entry by key.
        /// </summary>
        /// <param name="key">The cache key.</param>
        /// <param name="value">The retrieved value.</param>
        /// <param name="foundButExpired"> </param>
        /// <returns>A value of <c>true</c> if entry was found in the cache, <c>false</c> otherwise.</returns>
        public bool GetItem(string key, out object value, out bool foundButExpired)
        {
            CacheEntry entry;
            var now = GetCurrentDate();
            value = null;
            foundButExpired = false;
            // try to get from new items in cache
            if (CurrentBatches.GetItem(key, out entry))
            {
                DateTime entryExpirationTime = entry.ExpirationTime;
                if (now < entryExpirationTime)
                {
                    if (entry.SlidingExpiration > TimeSpan.Zero)
                        entry.ExpirationTime = now.Add(entry.SlidingExpiration);
                    value = entry.Value;
                    return true;
                }
                foundButExpired = true;
                //CurrentBatches.ExpireItem(key, entry);
                return false;
            }
            // try to get from items being mapped to disk...
            if (_previousBatches.GetItem(key, out entry))
            {
                DateTime entryExpirationTime = entry.ExpirationTime;
                if (now < entryExpirationTime)
                {
                    // allow only a single reader thread to update the entry...
                    // encache the item being mapped to disk as it was accessed...
                    var reCacheEntry = new CacheEntry(entry);
                    if (entry.SlidingExpiration > TimeSpan.Zero)
                        reCacheEntry.ExpirationTime = now.Add(reCacheEntry.SlidingExpiration);
                    if (!CacheItem(key, reCacheEntry)) return false;
                    value = entry.Value;
                    return true;
                }
                // just allow return false if expired item, background processor will evict it.
                foundButExpired = true;
            }
            return false;
        }

        #region PutItem
        /// <summary>
        /// Adds the specified entry to the cache.
        /// </summary>
        /// <param name="key">The entry key.</param>
        /// <param name="value">The entry value.</param>
        /// <param name="dependentEntitySets">The list of dependent entity sets.</param>
        /// <param name="slidingExpiration">The sliding expiration.</param>
        /// <param name="absoluteExpiration">The absolute expiration.</param>
        public void PutItem(string key, object value, IEnumerable<string> dependentEntitySets,
                            TimeSpan slidingExpiration, DateTime absoluteExpiration)
        {
            CacheEntry currentEntry;
            var newEntry = new CacheEntry(GetCurrentDate())
                {
                    Value = value,
                    ExpirationTime = absoluteExpiration,
                    DependentEntitySets = dependentEntitySets.ToList(),
                    SlidingExpiration = slidingExpiration,
                };

            if (slidingExpiration > TimeSpan.Zero)
            {
                newEntry.ExpirationTime = GetCurrentDate().Add(slidingExpiration);
            }
            var resToLock = new[]
                {
                    BatchContainer.BatchType.AddItems,
                    //Batches.BatchType.AddSets
                };
            CurrentBatches.Lock(resToLock);
            // updated time stamp no longer valid
            //CurrentBatches.UpdatedTimeStamps.Remove(key);
            if (!CurrentBatches.AddItems.TryGetValue(key, out currentEntry))
            {
                // if not found, add item and dependent entity sets
                CurrentBatches.AddItems.Add(key, newEntry);
                AddDependentEntitySet(key, dependentEntitySets);
            }
            else
            {
                ProcessDeltasDependentEntitySets(key, newEntry, currentEntry);
                // update entity store & entities' timestamp store...
                CurrentBatches.AddItems[key] = newEntry;
            }
            CurrentBatches.Unlock(resToLock);
        }

        private void AddDependentEntitySet(string key, IEnumerable<string> dependentEntitySets)
        {
            if (dependentEntitySets == null) return;
            foreach (string dependentEntitySet in dependentEntitySets)
            {
                Dictionary<string, byte> setKeys;
                if (!CurrentBatches.AddSets.TryGetValue(dependentEntitySet, out setKeys))
                {
                    setKeys = new Dictionary<string, byte>(BatchContainer.BatchSize);
                    CurrentBatches.AddSets.Add(dependentEntitySet, setKeys);
                }
                setKeys[key] = 0;
            }
        }

        // Process deltas on Dependent Entity Sets...
        private void ProcessDeltasDependentEntitySets(string key,
                                                      CacheEntry newEntry, CacheEntry currentEntry)
        {
            var removedDependentEntitySets = new List<string>();
            var addedDependentEntitySets = new List<string>();
            if (currentEntry.DependentEntitySets != null)
            {
                foreach (string dependentEntitySet in currentEntry.DependentEntitySets)
                {
                    if (!newEntry.DependentEntitySets.Contains(dependentEntitySet))
                        removedDependentEntitySets.Add(dependentEntitySet);
                }
            }
            if (newEntry.DependentEntitySets != null)
            {
                foreach (string dependentEntitySet in newEntry.DependentEntitySets)
                {
                    if (!currentEntry.DependentEntitySets.Contains(dependentEntitySet))
                        addedDependentEntitySets.Add(dependentEntitySet);
                }
            }
            AddDependentEntitySet(key, addedDependentEntitySets);
            // process removed dependent entity set(s)
            foreach (string dependentEntitySet in removedDependentEntitySets)
            {
                Dictionary<string, byte> setKeys;
                if (!CurrentBatches.AddSets.TryGetValue(dependentEntitySet, out setKeys)) continue;
                CurrentBatches.AddItems.Remove(key);
                if (setKeys.Count == 0)
                    CurrentBatches.AddSets.Remove(dependentEntitySet);
            }
        }
        #endregion

        /// <summary>
        /// Invalidates all cache entries which are dependent on any of the specified entity sets.
        /// </summary>
        /// <param name="entitySets">The entity sets.</param>
        public void InvalidateSets(IEnumerable<string> entitySets)
        {
            var resToLock = new[]
                {
                    BatchContainer.BatchType.AddItems,

                    //Batches.BatchType.AddSets
                    //Batches.BatchType.InvalidSets
                };
            CurrentBatches.Lock(resToLock);
            // process removed dependent entity set(s)
            foreach (string dependentEntitySet in entitySets)
            {
                Dictionary<string, byte> setKeys;
                if (CurrentBatches.AddSets.TryGetValue(dependentEntitySet, out setKeys))
                {
                    foreach (string key in setKeys.Keys)
                    {
                        CurrentBatches.AddItems.Remove(key);
                    }
                    CurrentBatches.AddSets.Remove(dependentEntitySet);
                }
                CurrentBatches.InvalidSets[dependentEntitySet] = 0;
            }
            CurrentBatches.Unlock(resToLock);
        }

        /// <summary>
        /// Invalidates cache entry with a given key.
        /// </summary>
        /// <param name="key">The cache key.</param>
        public void InvalidateItem(string key)
        {
            CacheEntry entry;
            var resToLock = new[]
                {
                    //Batches.BatchType.AddSets,
                    BatchContainer.BatchType.AddItems,
                    //Batches.BatchType.InvalidItems
                };
            CurrentBatches.Lock(resToLock);
            if (CurrentBatches.AddItems.TryGetValue(key, out entry))
            {
                if (entry.DependentEntitySets != null)
                {
                    foreach (string dependentEntitySet in entry.DependentEntitySets)
                    {
                        Dictionary<string, byte> setKeys;
                        if (!CurrentBatches.AddSets.TryGetValue(dependentEntitySet, out setKeys)) continue;
                        setKeys.Remove(key);
                        if (setKeys.Count == 0)
                            CurrentBatches.AddSets.Remove(dependentEntitySet);
                    }
                }
                CurrentBatches.AddItems.Remove(key);
            }
            CurrentBatches.InvalidItems[key] = 0;
            CurrentBatches.Unlock(resToLock);
        }
    }
}
