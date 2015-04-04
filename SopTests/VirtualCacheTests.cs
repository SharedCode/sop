using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Xml;
using System.Xml.Serialization;
using System.Runtime.Caching;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Sop.Caching;

namespace SopClientTests
{
    [TestClass]
    public class VirtualCacheTests
    {
        private VirtualCacheBase CreateVirtualCache(string name)
        {
            return new VirtualCache(name);
        }

        [TestMethod]
        public void ExpirationTest1()
        {
            var cache = CreateVirtualCache("MyCacheStore");

            // current time is 10:00
            cache.GetCurrentDate = (ticks) => new DateTime(2009, 1, 1, 10, 0, 0).Ticks + ticks;

            // set expiration time to 11:00
            cache.AddOrGetExisting("Item1", "someValue", new CacheItemPolicy()
            {
                AbsoluteExpiration = new DateTimeOffset(2009, 1, 1, 11, 0, 0, TimeSpan.Zero)
            });

            // make sure the item is still there
            cache.GetCurrentDate = (ticks) => new DateTime(2009, 1, 1, 10, 59, 59).Ticks + ticks;
            Assert.AreEqual(cache.Get("Item1"), "someValue");

            // make sure the item gets evicted at 11:00
            cache.GetCurrentDate = (ticks) => new DateTime(2009, 1, 1, 11, 00, 00).Ticks + ticks;
            Assert.AreEqual(cache.Get("Item1"), null);

            // don't forget to dispose virtual cache when done!
            cache.Dispose();
        }

        [TestMethod]
        public void ExpirationTest2()
        {
            var cache = CreateVirtualCache("MyCacheStore");

            // current time is 10:00
            cache.GetCurrentDate = (ticks) => new DateTime(2009, 1, 1, 10, 0, 0).Ticks + ticks;

            // set expiration time to 1 hour from the last access
            cache.AddOrGetExisting("Item1", "someValue", new CacheItemPolicy()
            {
                SlidingExpiration = TimeSpan.FromHours(1),
                AbsoluteExpiration = DateTimeOffset.MaxValue
            });

            // make sure the item is still there
            cache.GetCurrentDate = (ticks) => new DateTime(2009, 1, 1, 10, 59, 59).Ticks + ticks;
            Assert.AreEqual(cache.Get("Item1"), "someValue");

            // make sure the item does not get evicted at 11:00 because we have touched it a second ago
            cache.GetCurrentDate = (ticks) => new DateTime(2009, 1, 1, 11, 00, 00).Ticks + ticks;
            Assert.AreEqual(cache.Get("Item1"), "someValue");

            // make sure the item gets evicted at 12:00 because we have touched it an hour ago
            cache.GetCurrentDate = (ticks) => new DateTime(2009, 1, 1, 12, 00, 00).Ticks + ticks;
            Assert.AreEqual(cache.Get("Item1"), null);

            // don't forget to dispose virtual cache when done!
            cache.Dispose();
        }


        [TestMethod]
        public void ExpirationTest3()
        {
            var cache = CreateVirtualCache("MyCacheStore");

            // current time is 10:00
            cache.GetCurrentDate = (ticks) => new DateTime(2009, 1, 1, 10, 0, 0).Ticks + ticks;

            cache.Set("Item1", "someValue", new CacheItemPolicy()
            {
                AbsoluteExpiration = new DateTimeOffset(2009, 1, 1, 10, 0, 0, TimeSpan.Zero)
            });
            cache.Set("Item2", "someValue", new CacheItemPolicy()
            {
                AbsoluteExpiration = new DateTimeOffset(2009, 1, 1, 10, 1, 0, TimeSpan.Zero)
            });
            cache.Set("Item3", "someValue", new CacheItemPolicy()
            {
                AbsoluteExpiration = new DateTimeOffset(2009, 1, 1, 10, 2, 0, TimeSpan.Zero)
            });
            cache.Set("Item4", "someValue", new CacheItemPolicy()
            {
                AbsoluteExpiration = new DateTimeOffset(2009, 1, 1, 10, 3, 0, TimeSpan.Zero)
            });
            cache.GetCurrentDate = (ticks) => new DateTime(2009, 1, 1, 10, 2, 0).Ticks + ticks;
            Assert.AreEqual(cache.Get("Item1"), null);
            Assert.AreEqual(cache.Get("Item2"), null);
            Assert.AreEqual(cache.Get("Item3"), null);
            Assert.AreEqual(cache.Get("Item4"), "someValue");

            // don't forget to dispose virtual cache when done!
            cache.Dispose();
        }

        [TestMethod]
        public void ExpiredEntriesUpdateTest()
        {
            var cache = CreateVirtualCache("MyCacheStore");

            cache.OnCacheEntrySetUpdate += cache_OnCacheEntrySetUpdate;

            // current time is 10:00
            cache.GetCurrentDate = (ticks) => new DateTime(2009, 1, 1, 10, 0, 0).Ticks + ticks;

            cache.Set("Item1", "someValue", new CacheItemPolicy()
            {
                AbsoluteExpiration = new DateTimeOffset(2009, 1, 1, 10, 0, 0, TimeSpan.Zero)
            });
            cache.Set("Item2", "someValue", new CacheItemPolicy()
            {
                AbsoluteExpiration = new DateTimeOffset(2009, 1, 1, 10, 1, 0, TimeSpan.Zero)
            });
            cache.Set("Item3", "someValue", new CacheItemPolicy()
            {
                AbsoluteExpiration = new DateTimeOffset(2009, 1, 1, 10, 2, 0, TimeSpan.Zero)
            });
            cache.Set("Item4", "someValue", new CacheItemPolicy()
            {
                AbsoluteExpiration = new DateTimeOffset(2009, 1, 1, 10, 3, 0, TimeSpan.Zero)
            });

            now = new DateTimeOffset(2009, 1, 1, 10, 3, 0, TimeSpan.Zero);
            cache.GetCurrentDate = (ticks) => new DateTime(2009, 1, 1, 10, 2, 0).Ticks + ticks;
            Assert.AreEqual(cache.Get("Item1"), updatedValue);
            Assert.AreEqual(cache.Get("Item2"), updatedValue);
            Assert.AreEqual(cache.Get("Item3"), updatedValue);
            Assert.AreEqual(cache.Get("Item4"), "someValue"); // not updated as not expired yet...

            // expire all items...
            cache.GetCurrentDate = (ticks) => new DateTime(2009, 1, 1, 11, 2, 0).Ticks + ticks;

            // cause Expired items' update in the cache stores, set now to non-expiring datetime...
            updatedValue = "Goofer goofer ball";
            now = new DateTimeOffset(2009, 1, 1, 11, 3, 0, TimeSpan.Zero);
            Assert.AreEqual(cache.Get("Item1"), updatedValue);
            Assert.AreEqual(cache.Get("Item2"), updatedValue);
            Assert.AreEqual(cache.Get("Item3"), updatedValue);
            Assert.AreEqual(cache.Get("Item4"), updatedValue);

            // don't forget to dispose virtual cache when done!
            cache.Dispose();
        }
        DateTimeOffset now = new DateTimeOffset(2009, 1, 1, 10, 2, 0, TimeSpan.Zero);
        string updatedValue = "I'm Updated";
        void cache_OnCacheEntrySetUpdate(CacheEntryUpdateArguments[] arguments)
        {
            foreach (var itm in arguments)
            {
                itm.Source.Set(itm.Key, updatedValue, new CacheItemPolicy()
                    {
                        AbsoluteExpiration = now
                    });
            }
        }


        [TestMethod]
        public void ExpiredEntriesRefreshByBackgroundProcessorTest()
        {
            var cache = CreateVirtualCache("MyCacheStore");   //, persisted: false);

            cache.OnCacheEntrySetUpdate += cache_OnCacheEntrySetUpdate;

            // current time is 10:00
            cache.GetCurrentDate = (ticks) => new DateTime(2009, 1, 1, 10, 0, 0).Ticks + ticks;

            cache.Set("Item1", "someValue", new CacheItemPolicy()
            {
                AbsoluteExpiration = new DateTimeOffset(2009, 1, 1, 10, 0, 0, TimeSpan.Zero)
            });
            cache.Set("Item2", "someValue", new CacheItemPolicy()
            {
                AbsoluteExpiration = new DateTimeOffset(2009, 1, 1, 10, 1, 0, TimeSpan.Zero)
            });
            cache.Set("Item3", "someValue", new CacheItemPolicy()
            {
                AbsoluteExpiration = new DateTimeOffset(2009, 1, 1, 10, 2, 0, TimeSpan.Zero)
            });
            cache.Set("Item4", "someValue", new CacheItemPolicy()
            {
                AbsoluteExpiration = new DateTimeOffset(2009, 1, 1, 10, 3, 0, TimeSpan.Zero)
            });

            now = new DateTimeOffset(2009, 1, 1, 10, 3, 0, TimeSpan.Zero);
            cache.GetCurrentDate = (ticks) => new DateTime(2009, 1, 1, 10, 2, 0).Ticks + ticks;
            Assert.AreEqual(cache.Get("Item1"), updatedValue);
            Assert.AreEqual(cache.Get("Item2"), updatedValue);
            Assert.AreEqual(cache.Get("Item3"), updatedValue);
            Assert.AreEqual(cache.Get("Item4"), "someValue"); // not updated as not expired yet...

            // expire all items...
            cache.GetCurrentDate = (ticks) => new DateTime(2009, 1, 1, 11, 2, 0).Ticks + ticks;

            // cause Expired items' update in the cache stores, set now to non-expiring datetime...
            updatedValue = "Goofer goofer ball";
            now = new DateTimeOffset(2009, 1, 1, 11, 3, 0, TimeSpan.Zero);
            Assert.AreEqual(cache.Get("Item1"), updatedValue);
            Assert.AreEqual(cache.Get("Item2"), updatedValue);
            Assert.AreEqual(cache.Get("Item3"), updatedValue);
            Assert.AreEqual(cache.Get("Item4"), updatedValue);

            // don't forget to dispose virtual cache when done!
            cache.Dispose();
        }


        [TestMethod]
        public void MicroStressTest()
        {
            // run a bunch of concurrent reads and writes, make sure we get no exceptions

            // Set Store Pooling Count to 26 as this stress test uses 26 maxed Dependent Entity sets, they will all be stored opened in Store Pool by SOP.
            //Profile.MaxStoreInstancePoolCount = 26;

            //Sop.Log.Logger.DefaultLogDirectory = "c:\\SopBin";
            //var cache = new ScalableCache.ScalableCache(storeFilename: "c:\\SopBin\\CacheStore.dta");

            //int numberOfRequestBatches = 50; // will be multiplied by 5 (3 readers + 1 writer + 1 invalidator)
            //int numberOfIterationsPerThread = 50000;

            //ManualResetEvent startEvent = new ManualResetEvent(false);

            //Action writer = () =>
            //    {
            //        startEvent.WaitOne();
            //        Random random = new Random();

            //        for (int i = 0; i < numberOfIterationsPerThread; ++i)
            //        {
            //            string randomKey = Guid.NewGuid().ToString("N").Substring(0, 4);
            //            string randomValue = randomKey + "_V";
            //            List<string> dependentSets = new List<string>();
            //            int numberOfDependencies = random.Next(5);
            //            for (int j = 0; j < numberOfDependencies; ++j)
            //            {
            //                string randomSetName = new string((char)('A' + random.Next(26)), 1);
            //                dependentSets.Add(randomSetName);
            //            }
            //            cache.PutItem(randomKey, randomValue, dependentSets, TimeSpan.FromSeconds(35), DateTime.MaxValue);
            //        }
            //    };

            //Action invalidator = () =>
            //    {
            //        startEvent.WaitOne();
            //        Random random = new Random();

            //        for (int i = 0; i < numberOfIterationsPerThread; ++i)
            //        {
            //            List<string> dependentSets = new List<string>();
            //            int numberOfDependencies = random.Next(5);
            //            for (int j = 0; j < numberOfDependencies; ++j)
            //            {
            //                string randomSetName = new string((char)('A' + random.Next(26)), 1);
            //                dependentSets.Add(randomSetName);
            //            }

            //            cache.InvalidateSets(dependentSets);
            //        }
            //    };

            //Action reader = () =>
            //    {
            //        startEvent.WaitOne();
            //        Random random = new Random();

            //        for (int i = 0; i < numberOfIterationsPerThread; ++i)
            //        {
            //            string randomKey = Guid.NewGuid().ToString("N").Substring(0, 4);
            //            object value;

            //            if (cache.GetItem(randomKey, out value))
            //            {
            //                Assert.AreEqual(randomKey + "_V", value);
            //            }
            //        }
            //    };

            //List<Thread> threads = new List<Thread>();
            //for (int i = 0; i < numberOfRequestBatches; ++i)
            //{
            //    threads.Add(new Thread(() => writer()));
            //    threads.Add(new Thread(() => invalidator()));
            //    threads.Add(new Thread(() => reader()));
            //    threads.Add(new Thread(() => reader()));
            //    threads.Add(new Thread(() => reader()));
            //}

            //foreach (Thread t in threads)
            //{
            //    t.Start();
            //}

            //startEvent.Set();

            //foreach (Thread t in threads)
            //{
            //    t.Join();
            //}

            //// commit Scalable Cache transaction.
            //cache.Server.Commit();
        }
    }
}
