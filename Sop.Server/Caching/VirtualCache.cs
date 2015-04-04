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
    /// SOP VirtualCache is a virtualized (RAM & Disk) caching entity.
    /// It efficiently uses disk to extend memory capacity and to provide
    /// (optional) persisted Caching functionality.
    /// </summary>
    public partial class VirtualCache : VirtualCacheBase
    {
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
        public VirtualCache(string storePath, bool clusteredData = true, 
              string storeFileName = null, Sop.Profile fileConfig = null)
            : base(storePath, clusteredData, storeFileName, fileConfig)
        {
            Register();
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
            bool needsCommit = Unregister();
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

        #region Registration
        /// <summary>
        /// Register the VirtualCache for background processing.
        /// </summary>
        /// <param name="cache"></param>
        public void Register()
        {
            var cache = this;
            _cacheManager.Locker.Invoke(() =>
            {
                _cacheManager[cache.Name] = cache;
            });
        }
        /// <summary>
        /// Unregister VirtualCache for removal from backgound processing.
        /// </summary>
        /// <param name="cache"></param>
        public bool Unregister()
        {
            var cache = this;
            bool endBackgroundProcess = false;
            _cacheManager.Locker.Invoke(() =>
            {
                var currStore = _cacheManager.CurrentKey;
                _cacheManager.Remove(cache.Name);
                // repositions the cursor back to where it was before removal..
                if (!string.IsNullOrWhiteSpace(currStore) && currStore != cache.Name)
                    _cacheManager.Search(currStore);
                if (_cacheManager.Count == 0)
                    endBackgroundProcess = true;
            });
            return endBackgroundProcess;
        }
        private static Sop.Collections.Generic.SortedDictionary<string, VirtualCache> _cacheManager =
            new Sop.Collections.Generic.SortedDictionary<string, VirtualCache>();
        #endregion

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
            var now = GetCurrentDate(0);
            var cacheKey = new CacheKey(key) { TimeStamp = now };
            _store.Locker.Invoke(() =>
            {
                if (!_store.AddIfNotExist(cacheKey, v))
                {
                    cacheKey = _store.CurrentKey;
                    v = _store.CurrentValue;
                }
            });
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
            var now = GetCurrentDate(0);
            var cacheKey = new CacheKey(value.Key) { TimeStamp = now };
            _store.Locker.Invoke(() =>
            {
                if (!_store.AddIfNotExist(cacheKey, v))
                {
                    cacheKey = _store.CurrentKey;
                    v = _store.CurrentValue;
                    value.Value = v.Value;
                }
            });
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
            var now = GetCurrentDate(0);
            var cacheKey = new CacheKey(key) { TimeStamp = now };
            _store.Locker.Invoke(() =>
                {
                    if (!_store.AddIfNotExist(cacheKey, v))
                    {
                        cacheKey = _store.CurrentKey;
                        v = _store.CurrentValue;
                        value = v.Value;
                    }
                });
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
                    if (b || CacheEntrySetUpdateCallback == null)
                        return b;
                    // try to update the Cache entry if it is expired by calling the Update callback.
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
                    return b;
                }
            }
            finally
            {
                if (isLocked)
                    _store.Locker.Unlock();
            }
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
                return entry.Value;

            #region try to update the Cache entry if it is expired by calling the Update callback.
            if (CacheEntrySetUpdateCallback != null)
            {
                CacheEntrySetUpdateCallback(new CacheEntryUpdateArguments[] { new CacheEntryUpdateArguments(this, CacheEntryRemovedReason.Expired, key, null) });
                // try to get from the store a 2nd time and see if item got updated & no longer expired...
                if (_store.Locker.Invoke(block))
                    return entry.Value;
            }
            #endregion

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
                return new CacheItem(key, entry.Value);

            #region try to update the Cache entry if it is expired by calling the Update callback.
            if (CacheEntrySetUpdateCallback != null)
            {
                CacheEntrySetUpdateCallback(new CacheEntryUpdateArguments[] { new CacheEntryUpdateArguments(this, CacheEntryRemovedReason.Expired, key, null) });
                // try to get from the store a 2nd time and see if item got updated & no longer expired...
                if (_store.Locker.Invoke(block))
                    return new CacheItem(key, entry.Value);
            }
            #endregion

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
            if (keysList.Count == 0)
                return new Dictionary<string, object>();
            Dictionary<string, object> r = new Dictionary<string, object>(keysList.Count);

            QueryResult<CacheKey>[] result = null;
            if (!_store.Locker.Invoke<bool>(() =>
                {
                    return _store.Query(Sop.QueryExpression<CacheKey>.Package(keysList.ToArray()), out result);
                }))
                // just return, no need to update the TimeStamps as no match was found...
                return r;

            // package data Values onto the dictionary for return...
            //var l = new List<QueryResult<CacheKey>>(result.Length);
            //var now = GetCurrentDate(0);
            foreach (var res in result)
            {
                r[res.Key.Key] = res.Value;
                if (res.Found)
                {
                    CacheEntry ce = res.Value as CacheEntry;
                    if (!IsNotExpired(res.Key, ce, false))
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
                    //else if (!ce.NonExpiring)
                    //    l.Add(res);
                }
            }
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
                return r;

            // Notify the subscriber of the removed item.
            if (CacheEntrySetRemovedCallback != null)
                CacheEntrySetRemovedCallback(new CacheEntryRemovedArguments[] { new CacheEntryRemovedArguments(this, CacheEntryRemovedReason.Removed, cacheValue.Convert(cacheKey)) });

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
            var entry = new CacheEntry(policy, now) { Value = value };
            var cacheKey = new CacheKey(key) { TimeStamp = now };
            _store.Locker.Invoke(() =>
                {
                    #region update block
                    if (_store.AddIfNotExist(cacheKey, entry))
                        return;

                    // if priotiry is not removable, just update the store w/ value
                    if (entry.NonExpiring)
                    {
                        _store.CurrentValue.Value = value;
                        return;
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
                        }
                        return;
                    }
                    // adjust policy (set to slide every 5 mins) of the expired item to accomodate this update...
                    if (entry.SlidingExpiration == 0)
                        entry.SlidingExpiration = TimeSpan.TicksPerMinute * 5;
                    entry.ExpirationTime = GetCurrentDate(entry.SlidingExpiration);
                    if (!_store.IsDataInKeySegment)
                        _store.CurrentValue = entry;
                    return;
                    #region for removal
                    //if (_store.AddIfNotExist(cacheKey, ce))
                    //    return;
                    //// update the store...
                    //cacheKey.TimeStamp = _store.CurrentKey.TimeStamp;
                    //_store.CurrentKey.TimeStamp = now;
                    //var ce2 = new CacheEntry(policy, GetCurrentDate(0));
                    //ce2.Value = value;
                    //_store.CurrentValue = ce2;
                    #endregion
                    #endregion
                });
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
            _store.Locker.Invoke(() =>
                {
                    foreach (var item in values)
                        Set(item.Key, item.Value, item.Policy, item.RegionName);
                });
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
                return r;
            }
            set
            {
                var policy = new CacheItemPolicy { SlidingExpiration = new TimeSpan(0, 5, 0) };
                Set(key, value, policy);
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
            // todo: prevent Dispose of _store, NOTE: not critical as this method is a debugging tool only.
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
