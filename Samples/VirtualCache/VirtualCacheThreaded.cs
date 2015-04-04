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
    /// Heavily threaded Virtual Cache(VC) demo module.
    /// </summary>
    public class VirtualCacheThreaded
    {
        private Log.Logger _logger;
        const int ThreadCount = 20;
        public void Run()
        {
            List<Action> actions = new List<Action>();
            List<VirtualCacheBase> vcs = new List<VirtualCacheBase>();
            // create threads that will populate Virtual Cache and retrieve the items.
            for (int i = 0; i < ThreadCount; i++)
            {
                // Set "isPersisted" flag true if wanting to persist cached data across runs.
                // Non-persisted run (default) causes VirtualCache to be used as memory extender 
                // utilizing disk for extending cached data capacity beyond what can fit in memory to
                // what Disk can accomodate.
                var vc = new VirtualCache(string.Format("MyCacheStore{0}", i), true);
                if (_logger == null)
                {
                    _logger = vc.Logger;
                    _logger.LogLevel = Log.LogLevels.Verbose;
                    _logger.Information("Start of VirtualCache demo.");
                }
                // function to execute by the thread.
                actions.Add(() =>
                {
                    Populate(vc, i % 2 == 0);
                    RetrieveAll(vc);
                    var name = vc.Name;
                    vc.Dispose();
                    Console.WriteLine("VirtualCache {0} was disposed.", name);
                });
                vcs.Add(vc);
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
        const int MaxCacheEntries = 50000;
        private void Populate(ObjectCache target, bool slidingExpiration = false)
        {
            _logger.Information("{0}: Start of Populating target cache {1}.", DateTime.Now, target.Name);
            for (int i = 0; i < MaxCacheEntries; i++)
            {
                target.Set(string.Format("Hello{0}", i), string.Format("Value{0}", i), new CacheItemPolicy() { SlidingExpiration = new TimeSpan(0, 15, 0) });
            }
            _logger.Information("{0}: End Populating target cache {1}.", DateTime.Now, target.Name);
        }
        private void RetrieveAll(ObjectCache source)
        {
            _logger.Information("{0}: Start of RetrieveAll cache entries from {1}.", DateTime.Now, source.Name);
            for (int i = 0; i < MaxCacheEntries; i++)
            {
                if ((string)source[string.Format("Hello{0}", i)] == null)
                    Console.WriteLine("Entry {0} was not found in cache {1}, 'it could have expired and got evicted.", string.Format("Hello{0}", i), source.Name);
            }
            _logger.Information("{0}: End of RetrieveAll cache entries from {1}.", DateTime.Now, source.Name);
        }
    }
}
