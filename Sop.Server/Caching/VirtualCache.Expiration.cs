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
    /// Defines a reference to a method that is called when cache entry(ies)
    /// are about to be removed from the cache.
    /// </summary>
    /// <param name="arguments">Set of information about the entry(ies) that is 
    /// about to be removed from the cache.</param>
    public delegate void CacheEntrySetUpdateCallback(CacheEntryUpdateArguments[] arguments);
    /// <summary>
    /// Defines a reference to a method that is called after cache entry(ies)
    /// were removed from the cache.
    /// </summary>
    /// <param name="arguments">Set of information about the cache entry(ies)
    /// that were removed from the cache.</param>
    public delegate void CacheEntrySetRemovedCallback(CacheEntryRemovedArguments[] arguments);

    /// <summary>
    /// SOP VirtualCache Expiration related.
    /// </summary>
    public partial class VirtualCache
    {
        private bool commitToggler = true;
        public void Commit(bool onDispose = false)
        {
            // only commit if Server/Store is marked to persist cached entries...
            if (!Persisted)
                return;
            if (_lastCommitDateTime == 0)
            {
                _lastCommitDateTime = GetCurrentDate(0);
            }
            if (!onDispose)
            {
                if (CommitInterval == null || 
                    GetCurrentDate(0) - _lastCommitDateTime < CommitInterval.Value.Ticks)
                    return;
                if (_store == null || _store.Locker.TransactionRollback)
                    return;
            }
            if (_inCommit)
                return;
            lock (_serverLocker)
            {
                if (_inCommit)
                    return;
                _inCommit = true;

                //commitToggler = !commitToggler;
                Logger.Verbose("Transaction Cycling: {0}.", commitToggler);
                Server.CycleTransaction(commitToggler);
                _inCommit = false;
            }
            _lastCommitDateTime = GetCurrentDate(0);
        }
        static private volatile bool _inCommit;

        private bool IsNotExpired(CacheKey key, CacheEntry entry, bool callOnIdle = true)
        {
            if (entry.NonExpiring)
                return true;
            var now = GetCurrentDate(0);
            if (entry.IsExpired(now))
                return false;
            if (entry.SlidingExpiration > 0)
            {
                // if Value is saved in Store's key segment, updating this
                // field updated the current Value in the Store as they refer
                // to the same object at this point.
                entry.ExpirationTime = GetCurrentDate(entry.SlidingExpiration);
                if (!_store.IsDataInKeySegment)
                    _store.CurrentValue = entry;
            }
            return true;
        }

        private static long _lastCommitDateTime;
    }
}
