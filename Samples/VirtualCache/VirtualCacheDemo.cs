using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Caching;

namespace Sop.Samples
{
    /// <summary>
    /// Virtual Cache demo module.
    /// </summary>
    public class VirtualCacheDemo
    {
        private Log.Logger _logger;
        public void Run()
        {
            // Use a profile that uses more resources to accomodate more data in-memory (faster!).
            var profile = new Profile
            {
                MemoryLimitInPercent = 98,
                MaxStoreCount = 1,
                BTreeSlotLength = 250
            };
            using (Sop.Caching.VirtualCache vc = new Sop.Caching.VirtualCache("MyCacheStore", true, null, profile))
            {
                _logger = vc.Logger;
                _logger.LogLevel = Log.LogLevels.Verbose;
                _logger.Information("Start of VirtualCache demo.");
                Populate(vc);
                RetrieveAll(vc);
                _logger.Information("End of VirtualCache demo.");
                _logger.Information("You just 'cached' & 'accessed all' {0} records.", MaxCacheEntries);
            }
        }
        const int MaxCacheEntries = 500000;
        private void Populate(ObjectCache target)
        {
            _logger.Information("{0}: Start of Populating target cache {1}.", DateTime.Now, target.Name);
            for (int i = 0; i < MaxCacheEntries; i++)
            {
                target.Set(string.Format("Hello{0}", i), string.Format("Value{0}", i), null);
            }
            _logger.Information("{0}: End Populating target cache {1}.", DateTime.Now, target.Name);
        }
        private void RetrieveAll(ObjectCache source)
        {
            _logger.Information("{0}: Start of RetrieveAll cache entries from {1}.", DateTime.Now, source.Name);
            for (int i = 0; i < MaxCacheEntries; i++)
            {
                if ((string)source[string.Format("Hello{0}", i)] == null)
                    Console.WriteLine("Failed, entry {0} was not found in cache {1}!", string.Format("Hello{0}", source.Name));
            }
            _logger.Information("{0}: End of RetrieveAll cache entries from {1}.", DateTime.Now, source.Name);
        }
    }
}
