using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Caching;
using Sop.Server.Caching;

namespace Sop.Samples
{
    /// <summary>
    /// Virtual Cache(VC) in Memory Extender mode simulating multiple clients 
    /// running on different threads of execution storing cached data on a single VirtualCache.
    /// </summary>
    public class AzureVirtualStore
    {
        private Log.Logger _logger;
        private VirtualStore _cache = new VirtualStore("AzureStore");
        public void Run()
        {

            //cache.Dispose();
            //Console.WriteLine("VirtualCache was disposed.");
            //            Console.WriteLine("End of VirtualCache demo.");
            //Console.WriteLine("'Cached' & 'Accessed all' {0} records across {1} simulated clients.", MaxCacheEntries * ThreadCount, ThreadCount);
        }

        private void Add(string key, object value)
        {
            _cache[key] = value;

        }
    }
}
