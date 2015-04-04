using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Caching;
using System.Threading;
using Sop.Collections.Generic;

namespace Sop.Caching
{
    /// <summary>
    /// SOP VirtualCache Background processing.
    /// Background processor takes care of periodic execution of each virtual cache
    /// OnIdle method so each can manage cached items' expiration, batch processing, 
    /// Server wide transaction commit, etc...
    /// </summary>
    public class BackgroundProcessor<T> where T : VirtualCacheWithBackgroundRefresh
    {
        public BackgroundProcessor() { }
        /// <summary>
        /// Register the VirtualCache for background processing.
        /// </summary>
        /// <param name="cache"></param>
        public void Register(T cache)
        {
            _cacheManager.Locker.Invoke(() =>
            {
                _cacheManager[cache.Name] = cache;
            });
            StartBackgroundProcessing();
        }
        /// <summary>
        /// Unregister VirtualCache for removal from backgound processing.
        /// </summary>
        /// <param name="cache"></param>
        public bool Unregister(T cache)
        {
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
            if (endBackgroundProcess)
                EndBackgroundProcessing();
            return endBackgroundProcess;
        }

        /// <summary>
        /// Initiate background processing if it is not yet set.
        /// </summary>
        private void StartBackgroundProcessing(int runIntervalInMilli = 15000)
        {
            if (runIntervalInMilli < 500)
                throw new ArgumentOutOfRangeException("runIntervalInMilli", "Minimum runIntervalInMilli is 500.");

            if (_runTask != null) return;
            lock (this)
            {
                if (_runTask != null) return;
                _runIntervalInMilli = runIntervalInMilli;
                _exitEvent = new AutoResetEvent(false);
                _runTask = new Task(RunLoop);
                _runTask.Start();
            }
        }
        // background run loop.
        private void RunLoop()
        {
            // wait for exit event
            Func<int, bool> waitForExitEvent = (int runIntervalInMilli) =>
                {
                    return _exitEvent != null && !_exitEvent.WaitOne(runIntervalInMilli);
                };
            // run each Virtual Cache's "OnIdle" method for house keeping.
            while (waitForExitEvent(_runIntervalInMilli))
            {
                bool stopInnerLoop = false;
                while (waitForExitEvent(5))
                {
                    T cache = null;
                    _cacheManager.Locker.Invoke(() =>
                    {
                        if (_cacheManager.EndOfTree())
                        {
                            if (!_cacheManager.MoveFirst())
                            {
                                stopInnerLoop = true;
                                return;
                            }
                        }
                        cache = _cacheManager.CurrentValue;
                        if (!_cacheManager.MoveNext())
                            stopInnerLoop = true;
                    });
                    if (cache != null)
                    {
                        currentRunningCache = cache;
                        cache.OnIdle(false);
                        currentRunningCache = null;
                    }
                    if (stopInnerLoop) break;
                }
                if (stopInnerLoop)
                    continue;
                // exit event signal was detected, break the outer loop to stop execution.
                break;
            }
        }
        private volatile T currentRunningCache;

        /// <summary>
        /// Signal background processing to end.
        /// </summary>
        public void EndBackgroundProcessing()
        {
            if (_exitEvent == null)
                return;
            lock (this)
            {
                if (_exitEvent == null)
                    return;
                var r = _exitEvent;
                _exitEvent = null;
                r.Set();
                r.Dispose();
                if (!_runTask.IsCompleted && !_runTask.IsCanceled && !_runTask.IsFaulted)
                {
                    try
                    {
                        if (currentRunningCache != null && !VirtualCacheWithBackgroundRefresh.Persisted)
                            currentRunningCache.ExitSignal = true;
                    }
                    catch { }   // suppress any exit signaling exception, it's ok to throw.

                    _runTask.Wait();
                }
                _runTask.Dispose();
                _runTask = null;
            }
        }

        public bool IsBackgroundProcessing
        {
            get { return _runTask != null; }
        }

        private SortedDictionary<string, T> _cacheManager = new SortedDictionary<string, T>();
        private Task _runTask;
        private int _runIntervalInMilli = 5000;
        private AutoResetEvent _exitEvent;
    }
}
