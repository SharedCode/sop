using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Caching;

using Sop.Transaction;

namespace Sop.Samples
{
    /// <summary>
    /// Virtual Cache demo module. This will showcase usage of the new VirtualCache
    /// and will compare it with the .net built-in MemoryCache. VirtualCache is
    /// expected to lag behind as is not a pure in-memory caching facility
    /// but it should showcase more scalability. i.e. - ability to store
    /// cache entries limited only by the hardware (HDD).
    /// 
    /// VirtualCache also will not evict cache entries during out of memory
    /// conditions because it prevents out of memory conditions in the 1st place.
    /// Cached entries are swapped in/out of the disk to conserve memory.
    /// Most Recently Used (MRU) cached entries will stay in-memory thus, performance
    /// will still be excellent.
    /// </summary>
    public class VirtualCacheMemoryCacheCompare
    {
        private Log.Logger _logger;
        public void Run()
        {
            const int LoopCount = 3;
            Sop.Caching.VirtualCache vc = new Sop.Caching.VirtualCache("MyCacheStore", true);
            for (int i = 0; i < LoopCount; i++)
            {
                _logger = vc.Logger;
                _logger.LogLevel = Log.LogLevels.Verbose;
                _logger.Information("Start of VirtualCache MemoryCache Compare demo.");
                try
                {
                    ObjectCache oc = MemoryCache.Default;
                    if (i == 0)
                        Populate(vc);
                    else
                        Populate(vc);
                    //Populate(oc);
                    RetrieveAll(vc);
                    RetrieveAll(oc);
                    Compare(vc, oc);
                    _logger.Information("End of VirtualCache MemoryCache Compare demo.");
                    vc.Commit(true);
                }
                catch (TransactionRolledbackException)
                {
                    vc.Dispose();
                    vc = new Sop.Caching.VirtualCache("MyCacheStore");
                }
            }
            vc.Dispose();
        }
        const int MaxCacheEntries = 100000;       // 33778;         //10000000;
        private void Populate(ObjectCache target, int count = MaxCacheEntries)
        {
            _logger.Information("{0}: Start of Populating target cache {1}.", DateTime.Now, target.Name);
            for (int i = 0; i < count; i++)
            {
                if (i % 100 == 0)
                    _logger.Verbose("Cache Set on item# {0}.", i);
                target.Set(string.Format("Hello{0}", i), string.Format("Value{0}", i), null);
            }
            _logger.Information("{0}: End Populating target cache {1}.", DateTime.Now, target.Name);
        }
        private void RetrieveAll(ObjectCache source)
        {
            //_logger.Information("{0}: Start of RetrieveAll cache entries from {1}.", DateTime.Now, source.Name);
            //for (int i = 0; i < MaxCacheEntries; i++)
            //{
            //    if ((string)source[string.Format("Hello{0}", i)] == null)
            //        Console.WriteLine("Failed, entry {0} was not found in cache {1}!", string.Format("Hello{0}", source.Name));
            //}
            //_logger.Information("{0}: End of RetrieveAll cache entries from {1}.", DateTime.Now, source.Name);
        }

        private void Compare(ObjectCache a, ObjectCache b)
        {
            //Console.WriteLine("{0}: Start of Comparing cache {1} with cache {2}.", DateTime.Now, a.Name, b.Name);
            //for (int i = 0; i < MaxCacheEntries; i++)
            //{
            //    if ((string)a[string.Format("Hello{0}", i)] != (string)b[string.Format("Hello{0}", i)])
            //        Console.WriteLine("Failed, entry {0} was not found in either MemoryCache or VirtualCache!", string.Format("Hello{0}", i));
            //}
            //Console.WriteLine("{0}: End of Comparing cache {1} with cache {2}.", DateTime.Now, a.Name, b.Name);
        }
    }
}
