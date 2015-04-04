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
    /// SOP VirtualCache Expiration related.
    /// </summary>
    public partial class VirtualCacheWithBackgroundRefresh
    {
        struct UpdateEntry
        {
            public UpdateEntry(long currentTimeStamp, long newTimeStamp, CacheEntryReference cacheEntryReference)
            {
                CurrentTimeStamp = currentTimeStamp;
                NewTimeStamp = newTimeStamp;
                CacheEntryReference = cacheEntryReference;
            }
            public long CurrentTimeStamp;
            public long NewTimeStamp;
            public CacheEntryReference CacheEntryReference;
        }

        #region Add, Update, Remove Cache entries' timestamp records in the timestamp Store
        // set timestamp and do other background processing such as expiration & removal of old Entries...
        private void AddTimeStamp(CacheKey key, CacheEntryBase cacheEntry, bool callOnIdle = true)
        {
            if (cacheEntry == null || cacheEntry.NonExpiring)
            {
                if (callOnIdle)
                    MethodOnIdle();
                return;
            }
            lock (_addBatch)
            {
                _addBatch.Add(new KeyValuePair<CacheKey, CacheEntryBase>(key, cacheEntry));
            }
            if (callOnIdle)
                MethodOnIdle();
        }
        private void UpdateTimeStamp(CacheKey key, CacheEntryBase cacheEntry, long newTimeStamp, bool callOnIdle = true)
        {
            if (cacheEntry.NonExpiring)
            {
                if (callOnIdle)
                    MethodOnIdle();
                return;
            }
            lock (_updateBatch)
            {
                _updateBatch.Add(new UpdateEntry(key.TimeStamp, newTimeStamp, new CacheEntryReference(cacheEntry) { CacheEntryKey = key.Key }));
            }
            if (callOnIdle)
                MethodOnIdle();
        }
        // this gets called when a Cache Entry got removed from the Cache Store.
        private void RemoveTimeStamp(CacheKey key)
        {
            lock(_removeBatch)
            {
                _removeBatch.Add(key);
            }
            MethodOnIdle();
        }
        private void UpdateTimeStamp(QueryResult<CacheKey>[] itemsFound, long newTimeStamp)
        {
            if (itemsFound == null || itemsFound.Length == 0)
                return;
            if (itemsFound.Length < minimumBatchProcess)
            {
                // add to the update batch if less than 20 for optimization... (no need to spawn a thread).
                lock (_updateBatch)
                {
                    _updateBatch.AddRange(itemsFound.Select((a) => new UpdateEntry
                    {
                        CurrentTimeStamp = a.Key.TimeStamp,
                        NewTimeStamp = newTimeStamp,
                        CacheEntryReference = new CacheEntryReference((CacheEntry)a.Value)
                    }).ToArray());
                }
                MethodOnIdle();
                return;
            }
            // allow last update task to complete...
            if (_updateTimeStampTask != null)
            {
                if (_updateTimeStampTask.Status == TaskStatus.Running)
                    _updateTimeStampTask.Wait();
                _updateTimeStampTask.Dispose();
                _updateTimeStampTask = null;
            }
            // spawn a new thread task to do the batch update...
            _updateTimeStampTask = TaskRun(() =>
            {
                _storeKeysByDate.Locker.Invoke(() =>
                    {
                        // remove the entries in prep for adding new ones with newer timestamp as keys...
                        _storeKeysByDate.Remove(
                            itemsFound.Select((a) => new QueryExpression<long>()
                            {
                                Key = a.Key.TimeStamp,
                                ValueFilterFunc = (v) =>
                                    {
                                        return ((CacheEntryReference)v).CacheEntryKey == a.Key.Key;
                                    }
                            }).ToArray()
                            );
                        _storeKeysByDate.Add(itemsFound.Select(s => new KeyValuePair<long, CacheEntryReference>(newTimeStamp,
                            new CacheEntryReference(((CacheEntry)s.Value)) { CacheEntryKey = s.Key.Key })).ToArray());
                    });
                MethodOnIdle();
            });
        }
        private Task _updateTimeStampTask;
        #endregion

        #region On Idle event: background processing of Batched updates, Expired entries' detection and removal, periodic transcation Commit

        #region OnIdle method related
        // method OnIdle
        private void MethodOnIdle()
        {
            OnIdle(isMultiThreaded);
        }
        public void OnIdle(bool? isAsync = null)
        {
            try
            {
                if (Interlocked.Increment(ref _onIdleCallCtr) == 1)
                {
                    if (_onIdleTask != null)
                    {
                        if (!_onIdleTask.IsCompleted)
                            return;
                        _onIdleTask.Dispose();
                        _onIdleTask = null;
                    }
                    if (!IsProcessTime || ExitSignal)
                        return;
                    _onIdleTask = TaskRun(() =>
                        {
                            if (ProcessBatch())
                                ProcessExpiredEntries();
                            Commit();
                        }, isAsync);
                }
            }
            finally
            {
                Interlocked.Decrement(ref _onIdleCallCtr);
            }
        }
        internal volatile bool ExitSignal;
        private long _onIdleCallCtr;
        private Task _onIdleTask;
        #endregion

        private bool IsProcessTime
        {
            get
            {
                // if there are any batch changes reaching minimum count, return true so they can get processed
                if (IsBatchReadyForProcess)
                    return true;

                var now = GetCurrentDate(0);
                if (_lastProcessExpirationDateTime == 0)
                    _lastProcessExpirationDateTime = now;
                if (_lastCommitDateTime == 0)
                    _lastCommitDateTime = now;
                if (_lastProcessBatchDateTime == 0)
                    _lastProcessBatchDateTime = now;

                // check if it's time to process batch
                if (_lastProcessExpirationDateTime != 0 && now - _lastProcessExpirationDateTime >= ProcessExpiredEntriesIntervalInSec)
                    return true;

                return !(CommitInterval == null || _lastCommitDateTime == 0 || 
                         now - _lastCommitDateTime < CommitInterval.Value.Ticks);
            }
        }
        private bool IsBatchReadyForProcess
        {
            get
            {
                return _updateBatch.Count + _removeBatch.Count + _addBatch.Count > minimumBatchProcess;
            }
        }

        // Detect and remove expired Entries...
        private bool ProcessExpiredEntries()
        {
            if (_lastProcessExpirationDateTime == 0)
            {
                _lastProcessExpirationDateTime = GetCurrentDate(0);
            }

            var now = GetCurrentDate(0);
            // commented as invoking this method or not is driven by the caller code... i.e. - when Process Batch returns true, this function gets called.
            //if (now - _lastProcessExpirationDateTime < ProcessExpiredEntriesIntervalInSec)
            //    return false;
            _lastProcessExpirationDateTime = now;

            Logger.Verbose("ProcessExpiredEntries start.");

            #region get a batch of expired cache entries...
            int expireBatchCount = 200;
            var expiredKeys = new List<KeyValuePair<string, long>>(expireBatchCount);
            var expiringEntries = new List<CacheEntryUpdateArguments>(expireBatchCount);

            _storeKeysByDate.Locker.Invoke(() =>
                {
                    if (_storeKeysByDate.MoveFirst())
                    {
                        int ctr = 0;
                        do
                        {
                            if (ExitSignal) break;

                            // check if there are items that expired...
                            if (!_storeKeysByDate.CurrentValue.IsExpired(now)) break;

                            // add item to the expired items lists.
                            expiredKeys.Add(new KeyValuePair<string, long>(
                                _storeKeysByDate.CurrentValue.CacheEntryKey, _storeKeysByDate.CurrentKey));
                            // add item to the list for Update notification.
                            expiringEntries.Add(new CacheEntryUpdateArguments(this,
                                CacheEntryRemovedReason.Expired, _storeKeysByDate.CurrentValue.CacheEntryKey, null));

                        } while (_storeKeysByDate.MoveNext() && ctr++ < expireBatchCount);
                    }
                });

            if (expiredKeys.Count <= 0)
            {
                Logger.Verbose("No expired items found from Cache Timestamp Store with {0} items, returning.", _storeKeysByDate.Count);
                return false;
            }
            #endregion

            // Notify subscriber of the expiring entries to give it chance to update these expiring entries...
            if (CacheEntrySetUpdateCallback != null)
            {
                Logger.Verbose("Notifying subscriber(s) of {0} expiring items.", expiringEntries.Count);
                if (ExitSignal) return false;
                CacheEntrySetUpdateCallback(expiringEntries.ToArray());
            }
            expiringEntries = null;

            #region confirm expired entities are indeed expired per their Entity Store...
            var confirmedExpiredEntries = new List<KeyValuePair<CacheKey, CacheEntry>>(expiredKeys.Count);
            foreach (var item in expiredKeys)
            {
                if (ExitSignal) return false;
                CacheEntry entry;
                var entryKey = new CacheKey(item.Key);
                _store.Locker.Invoke(() =>
                    {
                        if (!_store.TryGetValue(entryKey, out entry))
                            return;
                        if (entry.IsExpired(now))
                        {
                            confirmedExpiredEntries.Add(new KeyValuePair<CacheKey, CacheEntry>(_store.CurrentKey, entry));
                            _store.Remove();
                        }
                    });
            }
            #endregion

            if (confirmedExpiredEntries.Count <= 0)
            {
                Logger.Verbose("All expiring entries were refreshed.");
                return false;
            }
            if (CacheEntrySetUpdateCallback != null)
                Logger.Verbose("After notification, expiring entries count is {0}.", confirmedExpiredEntries.Count);

            #region remove confirmed expired entities' timestamps from the timestamp store...
            var exprs = new QueryExpression<long>[confirmedExpiredEntries.Count];
            var cacheEntryRemovedItems = new CacheEntryRemovedArguments[confirmedExpiredEntries.Count];
            for(int i = 0; i < confirmedExpiredEntries.Count; i++)
            {
                if (ExitSignal) return false;
                cacheEntryRemovedItems[i] = new CacheEntryRemovedArguments(this, CacheEntryRemovedReason.Expired, 
                    confirmedExpiredEntries[i].Value.Convert(confirmedExpiredEntries[i].Key));

                var cek = confirmedExpiredEntries[i].Key.Key;
                exprs[i] = new QueryExpression<long>()
                {
                    Key = confirmedExpiredEntries[i].Key.TimeStamp,
                    ValueFilterFunc = (v) =>
                        {
                            return ((CacheEntryReference)v).CacheEntryKey == cek;
                        }
                };
            }
            QueryResult<long>[] r;
            Logger.Verbose("Removing {0} expired entries.", confirmedExpiredEntries.Count);
            _storeKeysByDate.Locker.Invoke(() =>
                {
                    _storeKeysByDate.Remove(exprs, out r);
                });
            #endregion

            // Generate the OnEntrySetRemoved event so event subscribers can get notified 
            // of items removed due to expiration...
            if (CacheEntrySetRemovedCallback != null)
                CacheEntrySetRemovedCallback(cacheEntryRemovedItems);

            _lastProcessExpirationDateTime = GetCurrentDate(0);
            return true;
        }

        private bool ProcessBatch(bool onDispose = false)
        {
            if (ExitSignal) return false;
            var now = GetCurrentDate(0);
            if (_lastProcessBatchDateTime == 0)
            {
                _lastProcessBatchDateTime = now;
            }
            if (!onDispose)
            {
                if (now - _lastProcessBatchDateTime < ProcessEntriesIntervalInSec && !IsBatchReadyForProcess)
                    return false;
            }
            _lastProcessBatchDateTime = now;

            #region Copy lock the batches locally and release the lock so other threads will not block using them.
            var addBatch = new List<KeyValuePair<CacheKey, CacheEntryBase>>();
            var updateBatch = new List<UpdateEntry>();
            var removeBatch = new List<CacheKey>();
            lock (_addBatch)
            {
                addBatch.AddRange(_addBatch);
                _addBatch.Clear();
            }
            lock (_updateBatch)
            {
                updateBatch.AddRange(_updateBatch);
                _updateBatch.Clear();
            }
            lock (_removeBatch)
            {
                removeBatch.AddRange(_removeBatch);
                _removeBatch.Clear();
            }
            #endregion

            #region Process the remove batch
            if (removeBatch.Count > 0)
            {
                Logger.Verbose("Removing a batch of {0} items.", removeBatch.Count);
                if (ExitSignal) return false;
                _storeKeysByDate.Locker.Invoke(() =>
                    {
                        _storeKeysByDate.Remove(removeBatch.Select((a) => new QueryExpression<long>()
                            {
                                Key = a.TimeStamp,
                                ValueFilterFunc = (o) => { return a.Key == ((CacheEntryReference)o).CacheEntryKey; }
                            }).ToArray());
                    });
            }
            #endregion

            #region Process the insert batch of newly added Entity timestamps...
            if (addBatch.Count > 0 && _storeKeysByDate.Count < MaxExpiredEntriesCount)
            {
                Logger.Verbose("Adding a batch of {0} items.", addBatch.Count);
                if (ExitSignal) return false;
                _storeKeysByDate.Locker.Invoke(() =>
                    {
                        _storeKeysByDate.Add(
                            addBatch.Select(
                            (s) => new KeyValuePair<long, CacheEntryReference>(s.Key.TimeStamp,
                                new CacheEntryReference(s.Value) { CacheEntryKey = s.Key.Key })
                            ).ToArray());
                    });
            }
            #endregion

            _lastProcessBatchDateTime = GetCurrentDate(0);
            if (Logger != null)
                Logger.Verbose("ProcessBatch returning true.");
            return true;
        }

        /// <summary>
        /// Maximum count of expired entries to keep.
        /// NOTE: expired entries are limited so as not to impact performance.
        /// No functionality was sacrificed by limiting this, as Expired Entries'
        /// Store serve as a persisted MRU store and is used primarily to help
        /// the System auto-remove/recycle expired cache.
        /// </summary>
        public static int MaxExpiredEntriesCount = 10000;

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
        #endregion

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
                UpdateTimeStamp(key, entry, now, callOnIdle);
            }
            return true;
        }

        #region Helper TaskRun method
        private Task TaskRun(Action action, bool? isMultiThreaded = null)
        {
            if ((isMultiThreaded == null && this.isMultiThreaded) ||
                (isMultiThreaded != null && isMultiThreaded.Value))
                return Task.Run(action);
            action();
            return null;
        }
        #endregion

        private const int minimumBatchProcess = 100;

        private static long _lastProcessBatchDateTime;
        private static long _lastCommitDateTime;
        private long _lastProcessExpirationDateTime;

        /// <summary>
        /// Process Entries Interval In Seconds.
        /// </summary>
        private const int ProcessEntriesIntervalInSec = 30;
        private const int ProcessExpiredEntriesIntervalInSec = 30;

        private List<KeyValuePair<CacheKey, CacheEntryBase>> _addBatch = 
            new List<KeyValuePair<CacheKey, CacheEntryBase>>();
        private List<CacheKey> _removeBatch = new List<CacheKey>();
    }
}
