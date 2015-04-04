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
    /// TODO: implement to compare Virtual Cache with equivalent code using SqlExpress for persistence.
    /// </summary>
    public class VirtualCacheVsEFSqlExpress
    {
        private Log.Logger _logger;
        public void Run()
        {
            var ThreadCount = 5;
            VirtualCache cache;
            List<Action> actions = new List<Action>();

            // Use a profile that uses more resources to accomodate more data in-memory (faster!).
            var profile = new Profile
            {
                MemoryLimitInPercent = 99,
                MaxStoreCount = 1,
                BTreeSlotLength = 150
            };
            cache = new VirtualCache("MyCacheStore", true, null, profile);
            if (_logger == null)
            {
                _logger = cache.Logger;
                _logger.LogLevel = Log.LogLevels.Verbose;
                _logger.Information("Start of VirtualCache multi-clients simulation demo.");
            }
            // create threads that will populate Virtual Cache and retrieve the items.
            for (int i = 0; i < ThreadCount; i++)
            {
                var vcIndex = i;
                actions.Add(() =>
                {
                    if (Populate(cache, vcIndex % 2 == 0, vcIndex * MaxCacheEntries))
                        RetrieveAll(cache, vcIndex * MaxCacheEntries);
                });
            }

            Console.WriteLine("Starting client simulation threads.");
            List<Task> tasks = new List<Task>();
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

            Console.WriteLine("Before VirtualCache dispose.");
            cache.Dispose();
            Console.WriteLine("VirtualCache was disposed.");
                        Console.WriteLine("End of VirtualCache demo.");
            Console.WriteLine("'Cached' & 'Accessed all' {0} records across {1} simulated clients.", MaxCacheEntries * ThreadCount, ThreadCount);
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
        private bool Populate(VirtualCache cache, bool slidingExpiration, int start)
        {
            var target = cache;
            _logger.Information("{0}: Start of Populating target cache {1}.", DateTime.Now, target.Name);
            try
            {
                // using a batch allows more efficient use of SOP data store so it can do bulk insert.
                CacheKeyValue[] batch = new CacheKeyValue[5000];
                var policy = new CacheItemPolicy() { SlidingExpiration = new TimeSpan(0, 15, 0) };
                for (int i = start; i < start + MaxCacheEntries; i++)
                {
                    batch[i % batch.Length] = new CacheKeyValue()
                    {
                        Key = string.Format("Hello{0}", i),
                        Value = string.Format("Value{0}", i),
                        Policy = policy
                    };
                    if (i % batch.Length == batch.Length - 1)
                        target.SetValues(batch);
                }
            }
            catch (Exception exc)
            {
                _logger.Fatal(exc, "{0}: Failed Populating target cache {1}.", DateTime.Now, target.Name);
                return false;
            }
            _logger.Information("{0}: End Populating target cache {1}.", DateTime.Now, target.Name);
            return true;
        }
        private bool RetrieveAll(VirtualCache cache, int start)
        {
            var source = cache;
            _logger.Information("{0}: Start of RetrieveAll cache entries from {1}.", DateTime.Now, source.Name);
            try
            {
                for (int i = start; i < start + MaxCacheEntries; i++)
                {
                    if ((string)source[string.Format("Hello{0}", i)] == null)
                        Console.WriteLine("Entry {0} was not found in cache {1}, 'it could have expired and got evicted.", string.Format("Hello{0}", i), source.Name);
                }
            }
            catch (Exception exc)
            {
                _logger.Fatal(exc, "{0}: Failed Retrieving items from target cache {1}.", DateTime.Now, source.Name);
                return false;
            }
            _logger.Information("{0}: End of RetrieveAll cache entries from {1}.", DateTime.Now, source.Name);
            return true;
        }
    }
}
