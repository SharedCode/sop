using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Sop.Mru;
using System.Runtime.Caching;
using Sop.Caching;

namespace Sop.Server.Caching
{
    /// <summary>
    /// Virtual Store is a general purpose caching entity for any backend data Storage.
    /// </summary>
    public class VirtualStore : ObjectCache, IMruClient
    {
        #region MRU
        class CacheKeyComparer : IComparer<object>
        {
            public int Compare(object x, object y)
            {
                return string.Compare(((CacheKey)x).Key, ((CacheKey)y).Key);
            }
        }
        public VirtualStore(string name, int mruMinCapacity = 4500, int mruMaxCapacity = 6000)
        {
            if (mruMinCapacity < 7) mruMinCapacity = 7;
            if (mruMaxCapacity < 10) mruMaxCapacity = 10;
            if (mruMinCapacity >= mruMaxCapacity)
                mruMinCapacity = (int)(mruMaxCapacity * .75);
            MruManager = new MruManager(mruMinCapacity, mruMaxCapacity, new CacheKeyComparer());
            MruManager.SetDataStores(this, null);
            _name = name;
        }
        public int MruMinCapacity
        {
            get
            {
                return MruManager.MinCapacity;
            }
        }
        public int MruMaxCapacity
        {
            get
            {
                return MruManager.MaxCapacity;
            }
        }

        /// <summary>
        /// Objects MRU cache manager
        /// </summary>
        internal MruManager MruManager { get; set; }
        #endregion
        #region not used, just needed for the IMruClient interface implement.
        int IMruClient.OnMaxCapacity(System.Collections.IEnumerable nodes)
        {
            return 0;
        }
        void IMruClient.OnMaxCapacity()
        {
            // do nothing.
        }
        #endregion

        #region Cache Entries' Update and Remove callbacks
        /// <summary>
        /// Cache Entry Set update callback. Set this to your application callback to be called
        /// when one or more Cache Entries are about to expire.
        /// </summary>
        public event CacheEntrySetUpdateCallback OnCacheEntrySetUpdate
        {
            add { CacheEntrySetUpdateCallback += value; }
            remove { CacheEntrySetUpdateCallback -= value; }
        }
        protected CacheEntrySetUpdateCallback CacheEntrySetUpdateCallback;

        /// <summary>
        /// Cache Entry Set removed callback. Set this to your application callback to be called
        /// when one or more Cache Entries were expired and removed from the cache.
        /// </summary>
        public event CacheEntrySetRemovedCallback OnCacheEntrySetRemoved
        {
            add { CacheEntrySetRemovedCallback += value; }
            remove { CacheEntrySetRemovedCallback -= value; }
        }
        protected CacheEntrySetRemovedCallback CacheEntrySetRemovedCallback;
        #endregion

        /// <summary>
        /// Allows external code to set its method to get date time.
        /// NOTE: this allows unit test code to set date time to some
        /// test driven values.
        /// </summary>
        public Func<long, long> GetCurrentDate
        {
            get { return _getCurrentDate; }
            set { _getCurrentDate = value; }
        }
        private Func<long, long> _getCurrentDate = (timeOffsetInTicks) => DateTimeOffset.UtcNow.Ticks + timeOffsetInTicks;

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
            lock(_locker)
            {
                // check if cacheKey is in MRU.
                var o = MruManager[cacheKey];
                if (o == null)
                    // add if not found.
                    MruManager[cacheKey] = v;
                else
                    // return it if found.
                    v = (CacheEntry)o;
            };
            return v.Value;
        }

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
            lock(_locker)
            {
                // check if cacheKey is in MRU.
                CacheItem o = (CacheItem)MruManager[cacheKey];
                if (o == null)
                    // add if not found.
                    MruManager[cacheKey] = v;
                else
                    // return it if found.
                    value.Value = o.Value;
            };
            return value;
        }

        public override object AddOrGetExisting(string key, object value, DateTimeOffset absoluteExpiration, string regionName = null)
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
            lock(_locker)
            {
                // check if cacheKey is in MRU.
                CacheItem o = (CacheItem)MruManager[cacheKey];
                if (o == null)
                    // add if not found.
                    MruManager[cacheKey] = v;
                else
                    // return it if found.
                    value = o.Value;
            };
            return value;
        }

        public override bool Contains(string key, string regionName = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            var cacheKey = new CacheKey(key);
            bool b;
            lock(_locker)
            {
                // try to get from the store
                var mruEntry = MruManager.GetItem(cacheKey);
                if (mruEntry == null)
                    return false;

                cacheKey = (CacheKey)mruEntry.Key;
                b = IsNotExpired(cacheKey, (CacheEntry)mruEntry.Value, false);
                if (b || CacheEntrySetUpdateCallback == null)
                    return b;
            }
            // try to update the Cache entry if it is expired by calling the Update callback.
            CacheEntrySetUpdateCallback(new CacheEntryUpdateArguments[] 
                { new CacheEntryUpdateArguments(this, CacheEntryRemovedReason.Expired, key, null) });
            lock(_locker)
            {
                // try to get from the store a 2nd time and see if item got updated & no longer expired...
                var mruEntry = MruManager.GetItem(cacheKey);
                if (mruEntry != null)
                {
                    cacheKey = (CacheKey)mruEntry.Key;
                    b = IsNotExpired(cacheKey, (CacheEntry)mruEntry.Value, false);
                }
                return b;
            }
        }

        public override CacheEntryChangeMonitor CreateCacheEntryChangeMonitor(
            IEnumerable<string> keys, string regionName = null)
        {
            throw new NotImplementedException("CreateCacheEntryChangeMonitor failed: Cache Entry Change Monitor is not supported.");
        }


        public override object Get(string key, string regionName = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            MruItem mruEntry = null;
            var cacheKey = new CacheKey(key);

            // code block to get item from store and if found, return it if not expired.
            Func<bool> block = (() =>
                {
                    lock (_locker)
                    {
                        // try to get from the store
                        mruEntry = MruManager.GetItem(cacheKey);
                        if (mruEntry != null)
                        {
                            cacheKey = (CacheKey)mruEntry.Key;
                            if (IsNotExpired(cacheKey, (CacheEntry)mruEntry.Value, false))
                                return true;
                        }
                        return false;
                    }
                });
            if (block())
                return mruEntry.Value;

            #region try to update the Cache entry if it is expired by calling the Update callback.
            if (CacheEntrySetUpdateCallback == null) return null;

            CacheEntrySetUpdateCallback(new CacheEntryUpdateArguments[] { new CacheEntryUpdateArguments(this, CacheEntryRemovedReason.Expired, key, null) });
            // try to get from the store a 2nd time and see if item got updated & no longer expired...
            if (block())
                return mruEntry.Value;
            #endregion
            return null;
        }

        public override CacheItem GetCacheItem(string key, string regionName = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            MruItem mruEntry = null;
            var cacheKey = new CacheKey(key);

            // code block to get item from store and if found, return it if not expired.
            Func<bool> block = (() =>
            {
                lock (_locker)
                {
                    // try to get from the store
                    mruEntry = MruManager.GetItem(cacheKey);
                    if (mruEntry != null)
                    {
                        cacheKey = (CacheKey)mruEntry.Key;
                        if (IsNotExpired(cacheKey, (CacheEntry)mruEntry.Value, false))
                            return true;
                    }
                    return false;
                }
            });
            if (block())
                return new CacheItem(key, mruEntry.Value);

            #region try to update the Cache entry if it is expired by calling the Update callback.
            if (CacheEntrySetUpdateCallback == null) return null;

            CacheEntrySetUpdateCallback(new CacheEntryUpdateArguments[] { new CacheEntryUpdateArguments(this, CacheEntryRemovedReason.Expired, key, null) });
            // try to get from the store a 2nd time and see if item got updated & no longer expired...
            if (block())
                return new CacheItem(key, mruEntry.Value);
            #endregion
            return null;
        }

        public override long GetCount(string regionName = null)
        {
            lock(_locker)
            {
                return MruManager.Count;
            }
        }

        /// <summary>
        /// Returns the Items of the MRU Cache.
        /// </summary>
        /// <returns></returns>
        protected override IEnumerator<KeyValuePair<string, object>> GetEnumerator()
        {
            lock(_locker)
            {
                var e = MruManager.GetEnumerator();
                List<KeyValuePair<string, object>> items = new List<KeyValuePair<string,object>>();
                do
                {
                    items.Add(new KeyValuePair<string, object>(
                        ((CacheKey)e.Current.Key).Key, ((CacheEntry)e.Current.Value).Value));
                } while (e.MoveNext());
                return items.GetEnumerator();
            }
        }
        /// <summary>
        /// Retrieves Values of a given set of entry keys.
        /// This also updates expiry time of those entries set for SlidingExpiration.
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
            foreach(var k in keysList)
            {
                CacheEntry ce;
                bool found;
                lock (_locker)
                {
                    object o;
                    found = MruManager.CacheCollection.TryGetValue(k, out o);
                    ce = (CacheEntry)o;
                }
                if (found)
                {
                    if (IsNotExpired(k, ce, false))
                    {
                        r[k.Key] = ce.Value;
                        continue;
                    }
                    #region try to update the expired entry...
                    if (CacheEntrySetUpdateCallback != null)
                    {
                        CacheEntrySetUpdateCallback(new[] 
                        {
                            new CacheEntryUpdateArguments(this, CacheEntryRemovedReason.Expired, k.Key, null) 
                        });
                        CacheEntry entry;
                        lock (_locker)
                        {
                            object o;
                            found = MruManager.CacheCollection.TryGetValue(k, out o);
                            entry = (CacheEntry)o;
                        }
                        if (found)
                        {
                            if (IsNotExpired(k, entry, false))
                            {
                                r[k.Key] = entry.Value;
                                continue;
                            }
                        }
                    }
                    #endregion
                }
                r[k.Key] = null;
            }
            return r;
        }

        #region Returns Name of this virtual Store.
        private string _name;
        public override string Name
        {
            get { return _name; }
        }
        #endregion

        /// <summary>
        /// Remove an entry with a given key. Notifies subscriber, if there is any, 
        /// of the removed item.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="regionName"></param>
        /// <returns></returns>
        public override object Remove(string key, string regionName = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            var cacheKey = new CacheKey(key);
            CacheEntry cacheValue = null;
            // remove the item, return if not found.
            lock(_locker)
            {
                var mruEntry = MruManager.GetItem(cacheKey);
                if (mruEntry == null)
                    return null;
                // get the Store's Current Key containing the TimeStamp!
                cacheKey = (CacheKey)mruEntry.Key;
                cacheValue = mruEntry.Value as CacheEntry;
                MruManager.Remove(cacheKey);
            }
            // Notify the subscriber of the removed item.
            if (CacheEntrySetRemovedCallback != null)
                CacheEntrySetRemovedCallback(new[] 
                {
                    new CacheEntryRemovedArguments(this, CacheEntryRemovedReason.Removed, cacheValue.Convert(cacheKey)) 
                });
            return cacheValue;
        }

        public override void Set(string key, object value,
            CacheItemPolicy policy, string regionName = null)
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

            lock (_locker)
            {
                var e = MruManager.GetItem(cacheKey);
                if (e == null)
                {
                    MruManager.Add(cacheKey, entry);
                    return;
                }
                entry = e.Value as CacheEntry;
                entry.Value = value;
                // if priotiry is not removable, just update the store w/ value
                if (entry.NonExpiring)
                    return;

                if (!entry.IsExpired(now))
                {
                    // slide the expire time...
                    if (entry.SlidingExpiration > 0)
                        entry.ExpirationTime = GetCurrentDate(entry.SlidingExpiration);
                    return;
                }
                // adjust policy (set to slide every 5 mins) of the expired item to accomodate this update...
                if (entry.SlidingExpiration == 0)
                    entry.SlidingExpiration = TimeSpan.TicksPerMinute * 5;
                entry.ExpirationTime = GetCurrentDate(entry.SlidingExpiration);
            }
        }

        public override void Set(CacheItem item, CacheItemPolicy policy)
        {
            if (item == null)
                throw new ArgumentNullException("item");
            if (policy == null)
                throw new ArgumentNullException("policy");

            Set(item.Key, item.Value, policy, item.RegionName);
        }

        public override void Set(string key, object value, 
            DateTimeOffset absoluteExpiration, string regionName = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            if (value == null)
                throw new ArgumentNullException("value");

            Set(key, value, new CacheItemPolicy { AbsoluteExpiration = absoluteExpiration }, regionName);
        }

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

        private CacheItemPolicy DefaultPolicy
        {
            get
            {
                return new CacheItemPolicy();
            }
        }

        /// <summary>
        /// Check if cache entry is not expired or expired.
        /// This slides the expire time if item is marked for SlidingExpiration.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="entry"></param>
        /// <param name="callOnIdle"></param>
        /// <returns></returns>
        private bool IsNotExpired(CacheKey key, CacheEntry entry, bool callOnIdle = true)
        {
            if (entry.NonExpiring)
                return true;
            var now = GetCurrentDate(0);
            if (entry.IsExpired(now))
                return false;
            if (entry.SlidingExpiration > 0)
            {
                // entry is in MRU cache, just update its expiration time.
                entry.ExpirationTime = GetCurrentDate(entry.SlidingExpiration);
            }
            return true;
        }

        private readonly object _locker = new object();
    }
}
