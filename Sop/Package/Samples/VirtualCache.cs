using System;

namespace Sop.Samples
{
    /// <summary>
    /// Virtual Cache is a sample use case of Sop for creating a virtualized cache.
    /// A virtualized cache in this case is an IDictionary instance that efficiently
    /// uses the memory + disk to store/manage/retrieve in high speed large objects &/or 
    /// large number of objects. Since Objects are stored on disk, the Application
    /// utilizing this VirtualCache will actually carve a small memory footprint 
    /// (for SOP internal caching!) and objects storable limited only by the size of the Disk.
    /// 
    /// Once the App exits, the cache file will be deleted from Disk.
    /// </summary>
    public class VirtualCache
    {

        /// <summary>
        /// Sample code that uses this VirtualCache.
        /// </summary>
        public static void Run()
        {
            Console.WriteLine("{0}: Virtual Cache demo started...", DateTime.Now);
            VirtualCache.ServerPath = "SopBin\\";
            VirtualCache vc = new VirtualCache();
            ISortedDictionaryOnDisk cache1 = vc.GetObjectCache(1);
            const int MaxCount = 40000;
            for (int i = 0; i < MaxCount; i++)
                cache1.Add(i, string.Format("{0} cached data", i));

            Console.WriteLine("{0}: Finished inserting {1} records, reading 'em starts now...", DateTime.Now, MaxCount);
            cache1.MoveFirst();
            cache1.HintSequentialRead = true;
            for (int i = 0; i < MaxCount; i++)
            {
                string s = cache1.CurrentValue as string;
                if (string.IsNullOrEmpty(s) ||
                    s != string.Format("{0} cached data", i))
                    Console.WriteLine("Error, data not found.");
                cache1.MoveNext();
            }
            Console.WriteLine("{0}: Virtual Cache demo ended... {1} records were read.", DateTime.Now, MaxCount);
        }

        /// <summary>
        /// Setups and returns the Virtual Cache 
        /// </summary>
        /// <param name="ObjectCacheID"></param>
        /// <returns></returns>
        public ISortedDictionaryOnDisk GetObjectCache(int ObjectCacheID)
        {
            Sop.IFile f = null;
            string fn = CacheFilename + ObjectCacheID.ToString();
            if (server == null)
            {
                server = Sop.ObjectServer.OpenWithTransaction(string.Format("{0}{1}", ServerPath, ServerFilename),
                    // memory extender mode will cause SOP data file get deleted upon restart
                    new Preferences { MemoryExtenderMode = true });
            }
            if (!server.FileSet.Contains(fn))
                f = server.FileSet.Add(fn);
            else
                f = server.FileSet[fn];
            if (f != null)
                return f.Store;
            return null;
        }
        public static string CacheFilename = "OFile";
        public static string ServerPath = "SopBin\\";
        public static string ServerFilename = "OServer.dta";
        static IObjectServer server;
    }
}
