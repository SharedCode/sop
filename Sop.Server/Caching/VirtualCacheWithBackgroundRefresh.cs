using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Caching;
using System.Threading;

namespace Sop.Caching
{
    /// <summary>
    /// VirtualCacheBackgroundRefresh is a VirtualCache implementation that does
    /// item refresh on the background.
    /// </summary>
    public partial class VirtualCacheWithBackgroundRefresh : VirtualCacheBase
    {
        protected static BackgroundProcessor<VirtualCacheWithBackgroundRefresh> Processor = new BackgroundProcessor<VirtualCacheWithBackgroundRefresh>();
        protected bool isMultiThreaded = true;
        // Key is long int value of a DateTime Offset.
        private Sop.ISortedDictionary<long, CacheEntryReference> _storeKeysByDate;

        /// <summary>
        /// Virtual Cache Constructor. Virtual Cache now defaults to being a Memory Extender.
        /// I.e. - Persisted static property defaults to false. Set VirtualCache.Persisted to true
        /// if wanting to persist the cached data across application runs.
        /// </summary>
        /// <param name="storePath">Data Store URI path or the Store name.</param>
        /// <param name="clusteredData">true (default) will configure the Store to save
        /// data together with the Keys in the Key Segment (a.k.a. - clustered), 
        /// false will configure Store to save data in its own Data Segment. For small to medium sized
        /// data, Clustered is recommended, otherwise set this to false.</param>
        /// <param name="storeFileName">Valid complete file path where to create the File to contain the data Store. 
        /// It will be created if it does not exist yet. Leaving this blank will create the Store within the default 
        /// SOP SystemFile, or in the referenced File portion of the storePath, if storePath has a directory path.</param>
        /// <param name="fileConfig">File Config should contain description how to manage data allocations on disk and B-Tree 
        /// settings for all the Data Stores of the File. If not set, SOP will use the default configuration.</param>
        public VirtualCacheWithBackgroundRefresh(string storePath, bool clusteredData = true, string storeFileName = null, Sop.Profile fileConfig = null)
            : base(storePath, clusteredData, storeFileName, fileConfig)
        {
            Server.SystemFile.Store.Locker.Invoke(() =>
            {
                var storeTimeStampPath = string.Format("{0}ByDate", storePath);
                Logger.Verbose("Creating/retrieving store {0}.", storeTimeStampPath);
                _storeKeysByDate = Server.StoreNavigator.GetStore<long, CacheEntryReference>(
                    storeTimeStampPath,
                    new StoreParameters<long>
                    {
                        IsDataInKeySegment = clusteredData,
                        AutoFlush = !clusteredData,
                        MruManaged = false
                    });
            });
            if (isMultiThreaded)
                Processor.Register(this);
        }

        /// <summary>
        /// Dispose this Virtual Cache instance. Update and On Idle tasks are waited for completion if
        /// ongoing during time of Dispose. Transaction is committed if pending and Virtual Cache is
        /// set to persist cached items to disk (parameter persisted was set to true on ctor).
        /// 
        /// The cache entries' data Stores are closed then disposed off as well.
        /// </summary>
        override public void Dispose()
        {
            bool needsCommit = false;
            if (Processor != null)
                needsCommit = Processor.Unregister(this);
            #region wait for ongoing tasks to finish & dispose them after.
            if (!Persisted)
                ExitSignal = true;
            if (_updateTimeStampTask != null)
            {
                // this is a quick task, no need to set a timeout.
                if (_updateTimeStampTask.Status == TaskStatus.Running)
                    _updateTimeStampTask.Wait();
                _updateTimeStampTask.Dispose();
                _updateTimeStampTask = null;
            }
            if (_onIdleTask != null)
            {
                // this is a quick task, no need to set a timeout.
                if (_onIdleTask.Status == TaskStatus.Running || 
                    _onIdleTask.Status == TaskStatus.WaitingToRun)
                    _onIdleTask.Wait();
                _onIdleTask.Dispose();
                _onIdleTask = null;
            }

            // flush all in-flight changes if persisted...
            if (!_store.Locker.TransactionRollback && Persisted)
            {
                ProcessBatch(true);
                ProcessExpiredEntries();
            }
            #endregion
            if (_storeKeysByDate != null)
            {
                Logger.Verbose("Disposing Store {0}.", _storeKeysByDate.Name);
                if (!Persisted)
                    ((OnDisk.Algorithm.SortedDictionary.ISortedDictionaryOnDisk)_storeKeysByDate.RealObject).IsUnloading = true;
                _storeKeysByDate.Dispose();
                _storeKeysByDate = null;
            }
            if (_store != null)
            {
                Logger.Verbose("Disposing Store {0}.", _store.Name);
                if (!Persisted)
                    ((OnDisk.Algorithm.SortedDictionary.ISortedDictionaryOnDisk)_store.RealObject).IsUnloading = true;
                _store.Dispose();
                _store = null;
            }
            if (needsCommit)
            {
                if (Persisted)
                    // commit if Persisted is true.
                    Commit(true);
                else
                {
                    // if in memextender, destroy Server to cause removal of data file.
                    lock (_serverLocker)
                    {
                        // unload any inflight changes in memory, no need to save on dispose.
                        ((OnDisk.ObjectServer)((Sop.ObjectServer)_server).RealObjectServer).Unload();
                        _server.Dispose();
                        _server = null;
                    }
                }
            }
            if (Logger != null)
            {
                Logger.Dispose();
                Logger = null;
            }
        }

        /// <summary>
        /// Returns the name of this cache.
        /// </summary>
        public override string Name
        {
            get { return _store != null ? _store.Name : ""; }
        }

        #region Add or Get Existing methods
        /// <summary>
        /// Add or Get Existing cache entry.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="policy"></param>
        /// <param name="regionName"></param>
        /// <returns></returns>
        public override object AddOrGetExisting(string key, object value,
                                                CacheItemPolicy policy, string regionName = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (value == null)
                throw new ArgumentNullException("value");
            if (policy == null)
                policy = DefaultPolicy;

            var v = new CacheEntry(policy, GetCurrentDate(0))
            {
                Value = value
            };
            bool add = true;

            var now = GetCurrentDate(0);
            var cacheKey = new CacheKey(key) { TimeStamp = now };
            _store.Locker.Invoke(() =>
            {
                if (!_store.AddIfNotExist(cacheKey, v))
                {
                    cacheKey = _store.CurrentKey;
                    v = _store.CurrentValue;
                    add = false;
                }
            });

            if (add)
                AddTimeStamp(cacheKey, v);
            else
                UpdateTimeStamp(cacheKey, v, now);

            return v.Value;
        }
        /// <summary>
        /// Add or Get Existing cache entry.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="policy"></param>
        /// <returns></returns>
        public override CacheItem AddOrGetExisting(CacheItem value, CacheItemPolicy policy)
        {
            if (value == null)
                throw new ArgumentNullException("value");
            if (value.Key == null)
                throw new ArgumentNullException("value.key");
            if (value.Value == null)
                throw new ArgumentNullException("value.value");

            if (policy == null)
                policy = DefaultPolicy;

            var v = new CacheEntry(policy, GetCurrentDate(0))
            {
                Value = value.Value
            };
            bool add = true;

            var now = GetCurrentDate(0);
            var cacheKey = new CacheKey(value.Key) { TimeStamp = now };
            _store.Locker.Invoke(() =>
            {
                if (!_store.AddIfNotExist(cacheKey, v))
                {
                    cacheKey = _store.CurrentKey;
                    v = _store.CurrentValue;
                    value.Value = v.Value;
                    add = false;
                }
            });
            if (add)
                AddTimeStamp(cacheKey, v);
            else
                UpdateTimeStamp(cacheKey, v, now);
            return value;
        }

        /// <summary>
        /// Add or Get Existing cache entry.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="absoluteExpiration"></param>
        /// <param name="regionName"></param>
        /// <returns></returns>
        public override object AddOrGetExisting(string key, object value,
            DateTimeOffset absoluteExpiration, string regionName = null)
        {
            if (value == null)
                throw new ArgumentNullException("value");
            if (key == null)
                throw new ArgumentNullException("key");

            var v = new CacheEntry
            {
                Value = value,
                ExpirationTime = absoluteExpiration.UtcTicks,
            };
            bool add = true;
            var now = GetCurrentDate(0);
            var cacheKey = new CacheKey(key) { TimeStamp = now };
            _store.Locker.Invoke(() =>
                {
                    if (!_store.AddIfNotExist(cacheKey, v))
                    {
                        cacheKey = _store.CurrentKey;
                        v = _store.CurrentValue;
                        value = v.Value;
                        add = false;
                    }
                });
            if (add)
                AddTimeStamp(cacheKey, v);
            else
                UpdateTimeStamp(cacheKey, v, now);
            return value;
        }
        #endregion

        /// <summary>
        /// true if cache entry with given key is found, false otherwise.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="regionName">regionName is ignored as is not supported in VirtualCache.</param>
        /// <returns></returns>
        public override bool Contains(string key, string regionName = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            CacheEntry entry;
            var cacheKey = new CacheKey(key);
            _store.Locker.Lock();
            var isLocked = true;
            try
            {
                // try to get from the store
                if (_store.TryGetValue(cacheKey, out entry))
                {
                    cacheKey = _store.CurrentKey;
                    var b = IsNotExpired(cacheKey, entry, false);
                    _store.Locker.Unlock();
                    isLocked = false;
                    if (!b)
                    {
                        // try to update the Cache entry if it is expired by calling the Update callback.
                        if (CacheEntrySetUpdateCallback != null)
                        {
                            CacheEntrySetUpdateCallback(new CacheEntryUpdateArguments[] { new CacheEntryUpdateArguments(this, CacheEntryRemovedReason.Expired, key, null) });
                            _store.Locker.Invoke(() =>
                                {
                                    // try to get from the store a 2nd time and see if item got updated & no longer expired...
                                    if (_store.TryGetValue(cacheKey, out entry))
                                    {
                                        cacheKey = _store.CurrentKey;
                                        b = IsNotExpired(cacheKey, entry, false);
                                    }
                                });
                        }
                        if (!b)
                            // insert the expired entry time stamp to expired cache store so the cache can get serviced for deletion on next scheduled pruning...
                            AddTimeStamp(cacheKey, entry, false);
                    }
                    MethodOnIdle();
                    return b;
                }
            }
            finally
            {
                if (isLocked)
                    _store.Locker.Unlock();
            }

            MethodOnIdle();
            return false;
        }
        /// <summary>
        /// This is not implemented and will error if called.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="regionName"></param>
        /// <returns></returns>
        public override CacheEntryChangeMonitor CreateCacheEntryChangeMonitor(
            IEnumerable<string> keys, string regionName = null)
        {
            throw new NotImplementedException("CreateCacheEntryChangeMonitor failed: Cache Entry Change Monitor is not supported.");
        }

        /// <summary>
        /// Default Cache capabilities of VirtualCache.
        /// These are the capabilities:
        /// Absolute Expiration, Sliding Expiration, 
        /// Set of CacheEntry(ies) update callback,
        /// Set of CacheEntry(ies) removed callback.
        /// 
        /// NOTE: pls. assign your delegates to the CacheEntrySetUpdateCallback and
        /// CacheEntrySetRemovedCallback for subscribing to events when
        /// a set of cache items are about to expire or when they
        /// expired and removed from cache respectively. 
        /// VirtualCache is a high volume, storage virtualized caching facility 
        /// and thus, this appropriate bulk entries' expiration and refresh facilities
        /// were provided.
        /// </summary>
        public override DefaultCacheCapabilities DefaultCacheCapabilities
        {
            get
            {
                return DefaultCacheCapabilities.AbsoluteExpirations | 
                       DefaultCacheCapabilities.SlidingExpirations |
                       DefaultCacheCapabilities.CacheEntryRemovedCallback |
                       DefaultCacheCapabilities.CacheEntryUpdateCallback |
                       DefaultCacheCapabilities.InMemoryProvider;
            }
        }

        #region Get methods
        /// <summary>
        /// Returns a cache entry with a given key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="regionName"></param>
        /// <returns></returns>
        public override object Get(string key, string regionName = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            CacheEntry entry = null;
            var cacheKey = new CacheKey(key);

            // code block to get item from store and if found, return it if not expired.
            Func<bool> block = (() =>
                {
                    // try to get from the store
                    if (_store.TryGetValue(cacheKey, out entry))
                    {
                        cacheKey = _store.CurrentKey;
                        if (IsNotExpired(cacheKey, entry, false))
                            return true;
                    }
                    return false;
                });

            if (_store.Locker.Invoke(block))
            {
                MethodOnIdle();
                return entry.Value;
            }
            #region try to update the Cache entry if it is expired by calling the Update callback.
            if (CacheEntrySetUpdateCallback != null)
            {
                CacheEntrySetUpdateCallback(new CacheEntryUpdateArguments[] { new CacheEntryUpdateArguments(this, CacheEntryRemovedReason.Expired, key, null) });
                // try to get from the store a 2nd time and see if item got updated & no longer expired...
                if (_store.Locker.Invoke(block))
                {
                    MethodOnIdle();
                    return entry.Value;
                }
            }
            #endregion

            // insert the expired entry time stamp to expired cache store so the cache can get serviced for deletion on next scheduled pruning...
            AddTimeStamp(cacheKey, entry);
            return null;
        }
        /// <summary>
        /// Returns the cache item with a given key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="regionName"></param>
        /// <returns></returns>
        public override CacheItem GetCacheItem(string key, string regionName = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            CacheEntry entry = null;
            var cacheKey = new CacheKey(key);

            Func<bool> block = (() =>
                {
                    // try to get from the store
                    if (_store.TryGetValue(cacheKey, out entry))
                    {
                        cacheKey = _store.CurrentKey;
                        if (IsNotExpired(cacheKey, entry, false))
                            return true;
                    }
                    return false;
                });

            if (_store.Locker.Invoke(block))
            {
                MethodOnIdle();
                return new CacheItem(key, entry.Value);
            }
            #region try to update the Cache entry if it is expired by calling the Update callback.
            if (CacheEntrySetUpdateCallback != null)
            {
                CacheEntrySetUpdateCallback(new CacheEntryUpdateArguments[] { new CacheEntryUpdateArguments(this, CacheEntryRemovedReason.Expired, key, null) });
                // try to get from the store a 2nd time and see if item got updated & no longer expired...
                if (_store.Locker.Invoke(block))
                {
                    MethodOnIdle();
                    return new CacheItem(key, entry.Value);
                }
            }
            #endregion

            // insert the expired entry time stamp to expired cache store so the cache can get serviced for deletion on next scheduled pruning...
            AddTimeStamp(cacheKey, entry);
            return null;
        }
        /// <summary>
        /// Returns the total number of cache entries expired and not in the Virtual Cache store.
        /// NOTE: returned value will change in time as expired items eventually get deleted 
        /// from the cache store by the eviction processor.
        /// </summary>
        /// <param name="regionName"></param>
        /// <returns></returns>
        public override long GetCount(string regionName = null)
        {
            return _store.Locker.Invoke(() => { return _store.Count; });
        }
        /// <summary>
        /// Retrievs cached entries given their Keys. If key is not found or expired,
        /// null will be returned to this item's Value portion of the KeyValuePair.
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="regionName"></param>
        /// <returns></returns>
        public override IDictionary<string, object> GetValues(IEnumerable<string> keys, string regionName = null)
        {
            if (keys == null)
                throw new ArgumentNullException("keys");
            var keysList = new List<CacheKey>();
            foreach (var k in keys)
                keysList.Add(new CacheKey(k));
            Dictionary<string, object> r = new Dictionary<string, object>();
            if (keysList.Count == 0)
                return r;

            QueryResult<CacheKey>[] result = null;
            if (!_store.Locker.Invoke<bool>(() =>
                {
                    if (!_store.Query(Sop.QueryExpression<CacheKey>.Package(keysList.ToArray()), out result))
                    {
                        // just unlock and return, no need to update the TimeStamps as no match was found...
                        _store.Locker.Unlock();
                        MethodOnIdle();
                        return false;
                    }
                    return true;
                }))
                return r;

            // package data Values onto the dictionary for return...
            var l = new List<QueryResult<CacheKey>>(result.Length);
            var now = GetCurrentDate(0);
            foreach (var res in result)
            {
                r[res.Key.Key] = res.Value;
                if (res.Found)
                {
                    CacheEntry ce = res.Value as CacheEntry;
                    if (ce.IsExpired(now))
                    {
                        // try to update the expired entry...
                        if (CacheEntrySetUpdateCallback != null)
                        {
                            CacheEntrySetUpdateCallback(new CacheEntryUpdateArguments[] { new CacheEntryUpdateArguments(this, CacheEntryRemovedReason.Expired, res.Key.Key, null) });
                            CacheEntry entry;
                            var resKey = res.Key;
                            _store.Locker.Invoke(() =>
                                {
                                    if (_store.TryGetValue(resKey, out entry))
                                    {
                                        if (IsNotExpired(resKey, entry, false))
                                            r[resKey.Key] = _store.CurrentValue.Value;
                                    }
                                });
                        }
                    }
                    else if (!ce.NonExpiring)
                        l.Add(res);
                }
            }
            UpdateTimeStamp(l.ToArray(), now);
            return r;
        }
        #endregion
        /// <summary>
        /// Remove a cache entry with a given key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="regionName"></param>
        /// <returns></returns>
        public override object Remove(string key, string regionName = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            var cacheKey = new CacheKey(key);
            CacheEntry r = null;
            CacheEntry cacheValue = null;
            // remove the item, return if not found.
            if (!_store.Locker.Invoke(() =>
                {
                    r = _store[cacheKey];
                    if (r == null)
                        return false;
                    // get the Store's Current Key containing the TimeStamp!
                    cacheKey = _store.CurrentKey;
                    cacheValue = _store.CurrentValue;
                    _store.Remove();
                    return true;
                }))
            {
                MethodOnIdle();
                return r;
            }

            // Notify the subscriber of the removed item.
            if (CacheEntrySetRemovedCallback != null)
                CacheEntrySetRemovedCallback(new CacheEntryRemovedArguments[] { new CacheEntryRemovedArguments(this, CacheEntryRemovedReason.Removed, cacheValue.Convert(cacheKey)) });

            if (!r.NonExpiring)
                RemoveTimeStamp(cacheKey);
            return r;
        }

        #region Set methods
        /// <summary>
        /// Set inserts or updates an existing cache entry with the given key.
        /// If eviction policy is null, a Sliding Expiration policy with five minute 
        /// timespan will be used. VirtualCache doesn't support regions thus, regionName
        /// will be ignored and defaults to null.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="policy"></param>
        /// <param name="regionName"></param>
        public override void Set(string key, object value, CacheItemPolicy policy, string regionName = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (value == null)
                throw new ArgumentNullException("value");
            if (policy == null)
                policy = DefaultPolicy;

            var now = GetCurrentDate(0);
            CacheEntry ce = new CacheEntry(policy, now);
            ce.Value = value;
            var cacheKey = new CacheKey(key) { TimeStamp = now };
            CacheEntry ce2 = null;
            if (_store.Locker.Invoke(() =>
                {
                    if (_store.AddIfNotExist(cacheKey, ce))
                        return true;
                    else
                    {
                        // update the store...
                        cacheKey.TimeStamp = _store.CurrentKey.TimeStamp;
                        _store.CurrentKey.TimeStamp = now;
                        ce2 = new CacheEntry(policy, GetCurrentDate(0));
                        ce2.Value = value;
                        _store.CurrentValue = ce2;
                        return false;
                    }
                }))
                // add to the TimeStamp store...
                AddTimeStamp(cacheKey, ce);
            else
                // udpate the TimeStamp store...
                UpdateTimeStamp(cacheKey, ce2, now);
        }
        /// <summary>
        /// Insert or update a given cache entry.
        /// </summary>
        /// <param name="item"></param>
        /// <param name="policy"></param>
        public override void Set(CacheItem item, CacheItemPolicy policy)
        {
            if (item == null)
                throw new ArgumentNullException("item");
            if (policy == null)
                throw new ArgumentNullException("policy");

            Set(item.Key, item.Value, policy, item.RegionName);
        }

        /// <summary>
        /// Insert or update a given list of cache entries.
        /// </summary>
        /// <param name="values"></param>
        public void SetValues(IEnumerable<CacheKeyValue> values)
        {
            if (values == null)
                throw new ArgumentNullException("values");
            foreach (var item in values)
                Set(item.Key, item.Value, item.Policy, item.RegionName);
        }

        /// <summary>
        /// Insert or update a given cache entry.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="absoluteExpiration"></param>
        /// <param name="regionName"></param>
        public override void Set(string key, object value, DateTimeOffset absoluteExpiration, string regionName = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (value == null)
                throw new ArgumentNullException("value");

            Set(key, value, new CacheItemPolicy { AbsoluteExpiration = absoluteExpiration }, regionName);
        }
        #endregion
        /// <summary>
        /// get will return the cache entry with a given key.
        /// set will insert/update the cache entry with a given key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public override object this[string key]
        {
            get
            {
                var r = Get(key);
                MethodOnIdle();
                return r;
            }
            set
            {
                var policy = new CacheItemPolicy { SlidingExpiration = new TimeSpan(0, 5, 0) };
                var now = GetCurrentDate(0);
                var entry = new CacheEntry(policy, GetCurrentDate(0));
                var cacheKey = new CacheKey(key) { TimeStamp = now };

                var r = _store.Locker.Invoke(() =>
                    {
                        if (_store.AddIfNotExist(cacheKey, entry))
                            return 1;

                        // if priotiry is not removable, just update the store w/ value
                        if (entry.NonExpiring)
                        {
                            _store.CurrentValue.Value = value;
                            return 2;
                        }

                        entry = _store.CurrentValue;
                        entry.Value = value;
                        if (!entry.IsExpired(now))
                        {
                            // slide the expire time...
                            if (entry.SlidingExpiration > 0)
                            {
                                entry.ExpirationTime = GetCurrentDate(entry.SlidingExpiration);
                                if (!_store.IsDataInKeySegment)
                                    _store.CurrentValue = entry;
                                return 0;
                            }
                            return 2;
                        }
                        // adjust policy (set to slide every 5 mins) of the expired item to accomodate this update...
                        if (entry.SlidingExpiration == 0)
                            entry.SlidingExpiration = TimeSpan.TicksPerMinute * 5;
                        entry.ExpirationTime = GetCurrentDate(entry.SlidingExpiration);
                        if (!_store.IsDataInKeySegment)
                            _store.CurrentValue = entry;
                        return 0;
                    }
                );
                if (r == 0)
                    UpdateTimeStamp(cacheKey, entry, now);
                else if (r == 1)
                    AddTimeStamp(cacheKey, entry);
                else if (r == 2)
                    MethodOnIdle();
            }
        }

        /// <summary>
        /// Get an enumerator.
        /// 
        /// Retrieving an enumerator for a VirtualCache instance is a resource-intensive and blocking operation. 
        /// Therefore, the enumerator should not be used in production applications.
        /// </summary>
        /// <returns></returns>
        protected override IEnumerator<KeyValuePair<string, object>> GetEnumerator()
        {
            return new ThreadSafeEnumerator(_store);
        }

        private CacheItemPolicy DefaultPolicy
        {
            get
            {
                return new CacheItemPolicy();
            }
        }
    }
}
