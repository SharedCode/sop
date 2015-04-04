using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Caching;
using Sop.Caching;

namespace Sop.Samples
{
    /// <summary>
    /// Virtual Cache(VC) in Memory Extender mode getting disposed and re-Created demo module.
    /// </summary>
    public class VirtualCacheWithBackgroundRefreshDemo
    {
        private Log.Logger _logger;
        private List<VirtualCacheWithBackgroundRefresh> vcs;
        const int ThreadCount = 2;

        public void Run()
        {
            List<Action> actions = new List<Action>();

            vcs = CreateVCs(ThreadCount);
            // create threads that will populate Virtual Cache and retrieve the items.
            for (int i = 0; i < ThreadCount; i++)
            {
                var vcIndex = i;
                // Set "isPersisted" flag true if wanting to persist cached data across runs.
                // Non-persisted run (default) causes VirtualCache to be used as memory extender 
                // utilizing disk for extending cached data capacity beyond what can fit in memory to
                // what Disk can accomodate.
                // function to execute by the thread.
                actions.Add(() =>
                {
                    Populate(vcIndex, vcIndex % 2 == 0);
                    RetrieveAll(vcIndex);
                    Console.WriteLine("Before VirtualCache {0} dispose.", vcIndex);
                    DisposeVCs(vcs, vcIndex);
                    Console.WriteLine("VirtualCache {0} was disposed.", vcIndex);
                });
            }

            List<Task> tasks = new List<Task>();
            // launch or start the threads all at once.
            foreach (var a in actions)
            {
                var t = TaskRun(a);
                if (t == null)
                    continue;
                tasks.Add(t);
            }
            // wait until all threads are finished.
            if (_threaded)
                Task.WaitAll(tasks.ToArray());
            Console.WriteLine("End of VirtualCache demo.");
        }
        private bool _threaded = true;
        private Task TaskRun(Action action)
        {
            if (!_threaded)
            {
                action();
                return null;
            }
            return Task.Run(action);
        }

        private List<VirtualCacheWithBackgroundRefresh> CreateVCs(int ThreadCount)
        {
            //VirtualCache.Persisted = true;
            List<VirtualCacheWithBackgroundRefresh> vcs = new List<VirtualCacheWithBackgroundRefresh>();
            // Use a profile that uses more resources to accomodate more data in-memory (faster!).
            var profile = new Profile
            {
                MemoryLimitInPercent = 98,
                MaxStoreCount = ThreadCount,
                BTreeSlotLength = 250
            };
            for (int i = 0; i < ThreadCount; i++)
            {
                var vc = new VirtualCacheWithBackgroundRefresh(string.Format("MyCacheStore{0}", i), true, null, profile);
                if (_logger == null)
                {
                    _logger = vc.Logger;
                    _logger.LogLevel = Log.LogLevels.Verbose;
                    _logger.Information("Start of VirtualCache demo.");
                }
                vcs.Add(vc);
            }
            return vcs;
        }
        private void DisposeVCs(List<VirtualCacheWithBackgroundRefresh> vcs, int targetId = -1)
        {
            if (targetId == -1)
            {
                foreach (var vc in vcs)
                    vc.Dispose();
                return;
            }
            vcs[targetId].Dispose();
            vcs[targetId] = null;
        }

        const int MaxCacheEntries = 500000;
        private void Populate(int targetId, bool slidingExpiration = false)
        {
            here:
            var target = vcs[targetId];
            _logger.Information("{0}: Start of Populating target cache {1}.", DateTime.Now, target.Name);
            try
            {
                for (int i = 0; i < MaxCacheEntries; i++)
                {
                    target.Set(string.Format("Hello{0}", i), string.Format("Value{0}", i), new CacheItemPolicy() { SlidingExpiration = new TimeSpan(0, 15, 0) });
                }
            }
            catch (Exception exc)
            {
                Console.WriteLine("Exception encountered: " + exc.Message);
                DisposeVCs(vcs);
                vcs = CreateVCs(ThreadCount);
                // this is a test/demo code, "goto" should be fine (in a hurry :).
                goto here;
            }
            _logger.Information("{0}: End Populating target cache {1}.", DateTime.Now, target.Name);
        }
        private void RetrieveAll(int sourceId)
        {
            var source = vcs[sourceId];
            _logger.Information("{0}: Start of RetrieveAll cache entries from {1}.", DateTime.Now, source.Name);
            try
            {
                for (int i = 0; i < MaxCacheEntries; i++)
                {
                    if ((string)source[string.Format("Hello{0}", i)] == null)
                        Console.WriteLine("Entry {0} was not found in cache {1}, 'it could have expired and got evicted.", string.Format("Hello{0}", i), source.Name);
                }
            }
            catch(Exception exc)
            {
                // inspect exception here.
            }
            _logger.Information("{0}: End of RetrieveAll cache entries from {1}.", DateTime.Now, source.Name);
        }
    }
}
