// Scalable Object Persistence (SOP) Framework, main contact - Gerardo Recinto (email: gerardorecinto@Yahoo.com for questions/comments)
// Open Source License: LGPL v2.1
// Have fun Coding! ;)

using System;
using System.Collections.Generic;
using System.Text;
using Sop.Mru;
using Sop.OnDisk.Algorithm.Collection;

namespace Sop.OnDisk
{
    /// <summary>
    /// Cache Pool Manager.
    /// </summary>
    internal class CachePoolManager
    {
        /// <summary>
        /// Initialize Cache Pool Manager.
        /// </summary>
        /// <param name="mruMinCapacity"></param>
        /// <param name="mruMaxCapacity"></param>
        public void Initialize(int mruMinCapacity, int mruMaxCapacity)
        {
            //if (_collectionCachePool != null) return;
            //lock (_locker)
            //{
            //    if (_collectionCachePool == null && mruMinCapacity > 0)
            //        _collectionCachePool =
            //            new MruManager(mruMinCapacity, mruMaxCapacity);
            //}
        }
        /// <summary>
        /// Get collection cache given an Id.
        /// </summary>
        /// <param name="cacheId"></param>
        /// <returns></returns>
        public ICollectionCache GetCache(string cacheId)
        {
            return null;
            //lock (_locker)
            //{
            //    // get/add this instance's cache from/to the pool.
            //    if (_collectionCachePool.Contains(cacheId))
            //    {
            //        var r = (ICollectionCache) _collectionCachePool[cacheId];
            //        if (r.MruManager != null)
            //            return r;
            //        _collectionCachePool.Remove(cacheId);
            //    }
            //    return null;
            //}
        }
        /// <summary>
        /// Updates a collection cache with a given cache Id.
        /// </summary>
        /// <param name="cacheId"></param>
        /// <param name="cache"></param>
        public void SetCache(string cacheId, ICollectionCache cache)
        {
            //lock (_locker)
            //{
            //    _collectionCachePool.Add(cacheId, cache);
            //}
        }

        //private readonly object _locker = new object();
        //private IMruManager _collectionCachePool;
    }
}
