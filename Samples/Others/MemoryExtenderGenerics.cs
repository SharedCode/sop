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
    public class MemoryExtenderGenerics
    {
        public class Person
        {
            public string FirstName;
            public string LastName;
            public string PhoneNumber;
            public string Address;
        }
        /// <summary>
        /// Sample code that uses this VirtualCache.
        /// </summary>
        public static void Run()
        {
            Console.WriteLine("{0}: Virtual Cache demo started...", DateTime.Now);
            MemoryExtenderGenerics vc = new MemoryExtenderGenerics();
            ISortedDictionary<int, Person> cache1 = vc.GetObjectCache(1);
            const int MaxCount = 40000;
            for (int i = 0; i < MaxCount; i++)
            {
                Person p = new Person();
                p.FirstName = string.Format("Joe{0}", i);
                p.LastName = "Castanas";
                p.PhoneNumber = "510-324-2222";
                p.Address = string.Format("5434 {0} Coventry Court, Fremont, Ca. 94888, U.S.A.", i);
                cache1.Add(i, p);
            }

            Console.WriteLine("{0}: Finished inserting {1} records, reading 'em starts now...", DateTime.Now, MaxCount);
            cache1.MoveFirst();
            cache1.HintSequentialRead = true;
            for (int i = 0; i < MaxCount; i++)
            {
                Person p = cache1.CurrentValue;
                if (p == null ||
                    p.FirstName != string.Format("Joe{0}", i))
                    Console.WriteLine("Error, data for iteration {0} not found.", i);
                cache1.MoveNext();
            }
            Console.WriteLine("{0}: Virtual Cache demo ended... {1} records were read.", DateTime.Now, MaxCount);
        }
        Sop.IObjectServer server;

        /// <summary>
        /// Setup and return the Virtual Cache 
        /// </summary>
        /// <param name="ObjectCacheID"></param>
        /// <returns></returns>
        public ISortedDictionary<int, Person> GetObjectCache(int ObjectCacheID)
        {
            string CacheFilename = "OFile" + ObjectCacheID.ToString();
            string ServerPath = "SopBin\\";
            string ServerFilename = "OServer.dta";
            Sop.IFile f = null;
            string fn = string.Format("{0}{1}.{2}", ServerPath, CacheFilename, ObjectServer.DefaultFileExtension);

            if (server == null)
                server = Sop.ObjectServer.OpenWithTransaction(string.Format("{0}{1}", ServerPath, ServerFilename),
                    new Preferences { MemoryExtenderMode = true });

            f = server.FileSet[CacheFilename];
            if (f == null)
                f = server.FileSet.Add(CacheFilename, fn);
            IStoreFactory sf = new StoreFactory();
            return sf.Get<int, Person>(f.Store, "VirtualCache");
        }
    }
}
