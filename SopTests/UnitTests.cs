using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Sop;
using Sop.Mru;
using Sop.Mru.Generic;
using Sop.SystemInterface;

namespace SopClientTests
{

    // todo: HintSize on BTreeAlgorithm should be consistently set.


    [TestClass]
    public class UnitTests
    {
        [TestMethod]
        public void LambdaCallperfTest()
        {
            const int _iterations = 1000000;
            Console.WriteLine("{0} iterations.", _iterations);
            Console.WriteLine("{0}: Lambda Start.", DateTime.Now);
            for(int i = 0; i < _iterations; i++)
            {
                Func<string> f = () => { return Foo(_iterations); };
                f();
            }
            Console.WriteLine("{0}: Lambda End.", DateTime.Now);
            Console.WriteLine("{0}: call Start.", DateTime.Now);
            for (int i = 0; i < _iterations; i++)
            {
                Foo(_iterations);
            }
            Console.WriteLine("{0}: call End.", DateTime.Now);
        }
        private string Foo(int i)
        {
            object o = 90 + i;
            return o.ToString();
        }

        [TestMethod]
        public void TestSimpleWrite()
        {
            Sop.Log.Logger.DefaultLogDirectory = "\\\\MyBookLive\\Public\\SopBin2/";
            Sop.Log.Logger.Instance.LogLevel = Sop.Log.LogLevels.Verbose;
            Console.WriteLine("Start");
            try
            {
                Sop.ObjectServer server = Sop.ObjectServer.OpenWithTransaction("\\\\MyBookLive\\Public\\SopBin2\\OServer.dta");
                var df = new Sop.StoreFactory();
                var sortDict = df.Get<int, int>(server.SystemFile.Store, "Collection");
                for (int i = 0; i < 10000; i++)
                    sortDict.Add(i, i);
                Sop.Log.Logger.Instance.Verbose("Record Count: {0}", sortDict.Count);
                server.Commit();
                Console.WriteLine("End");
            }
            catch (Exception exc)
            {
                Console.WriteLine("Error: {0}", exc.ToString());
                Sop.Log.Logger.Instance.Error(exc);
            }
        }
        [TestMethod]
        public void ProfileTest()
        {
            Sop.Preferences pref = new Preferences();
            Sop.Profile p = new Profile(pref);
        }
        [TestMethod]
        public void GetMemSizeTest()
        {
            var ramSize = SystemAdaptor.SystemInterface.GetMemorySize();
        }
        [TestMethod]
        public void LoggerDirTest()
        {
            Sop.Utility.GenericLogger l = new Sop.Utility.GenericLogger();
            l.LogLine("test");
        }

        class BTree<TKey, TValue> : Sop.Collections.Generic.SortedDictionary<TKey, TValue>, IMruClient
        {
            public int OnMaxCapacity(System.Collections.IEnumerable nodes)
            {
                int i = 0;
                foreach (var o in nodes)
                    i++;
                return i;
            }
            public void OnMaxCapacity()
            {
            }
        }
        [TestMethod]
        public void MruCollectionPruneItemsTest()
        {
            ConcurrentMruManager<int, int> Mru = new ConcurrentMruManager<int, int>(10, 15);
            BTree<int, int> btree = new BTree<int,int>();
            Mru.SetDataStores(btree);
            for (int i = 0; i < 15; i++)
                Mru.Add(i, i);
            Mru.Add(14, 14);
        }

        [TestMethod]
        public void MruPruneItemsTest()
        {
            ConcurrentMruManager<int, int> Mru = new ConcurrentMruManager<int, int>(10, 15);
            for (int i = 0; i < 15; i++)
                Mru.Add(i, i);

            Mru.Add(14, 14);

            //int fileMaxCount = Sop.Collections.OnDisk.Core.Win32._getmaxstdio();
        }

        [TestMethod]
        public void GetQueryExpressionSizeTest()
        {
            Sop.QueryExpression<string> qe = new QueryExpression<string>();
            qe.Key = "hello world";
            //qe.ValueFilterFunc = value => value == "foo Bar";
            int size;
            unsafe
            {
                size = System.Runtime.InteropServices.Marshal.SizeOf(qe);
            } 
        }

        [TestMethod]
        public void NullValueInStoreTest()
        {
            Sop.ObjectServer server = Sop.ObjectServer.OpenWithTransaction("c:\\SopBin\\OServer.dta");
            var df = new Sop.StoreFactory();
            var sortDict = df.Get<int, FooBar>(server.SystemFile.Store, "Collection");
            if (sortDict.Count > 0)
            {
                do
                {
                    int i = sortDict.CurrentKey;
                    FooBar bar = sortDict.CurrentValue;
                } while (sortDict.MoveNext());
            }
            sortDict.Add(1, new FooBar(){Foo = "Hello World."});
            sortDict.Add(1, null);
            server.Commit();
        }


        [TestMethod]
        public void GeneralPurposeStoreTest()
        {
            Sop.ObjectServer server = Sop.ObjectServer.OpenWithTransaction("c:\\SopBin\\OServer.dta");
            var df = new Sop.StoreFactory();
            var sortDict = df.Get<int, int>(server.SystemFile.Store, "Collection");
            sortDict.Add(1, 1);
            sortDict.Transaction.Commit();
        }
        [TestMethod]
        public void Test()
        {
            Sop.ObjectServer server = Sop.ObjectServer.OpenWithTransaction("c:\\SopBin\\OServer.dta");
            for(int i = 0; i < 301; i++)
            {
                string s = string.Format("Collection{0}", i);
                server.SystemFile.Store[s] = i;
            }
            for (int i = 0; i < 301; i++)
            {
                string s = string.Format("Collection{0}", i);
                var oo = server.SystemFile.Store[s];
                if ((int)oo != i)
                {
                    Assert.Fail("Failed.");
                }
            }
        }
        [TestMethod]
        public void IncorrectValueTypeTest()
        {
            Sop.ObjectServer server = Sop.ObjectServer.OpenWithTransaction("c:\\SopBin\\OServer.dta");
            var df = new Sop.StoreFactory();
            const int ItemCount = 50000;
            long StoreItemCount = 0;
            for (int i = 0; i < ItemCount; i++)
            {
                string s = string.Format("Collection{0}", i);
                using (var sortDict = df.Get<int, int>(server.SystemFile.Store, s))  //, null, false))
                {
                    if (StoreItemCount == 0)
                        StoreItemCount = sortDict.Count;
                    else if (sortDict.Count != StoreItemCount)
                        Assert.Fail(string.Format("sortDict.Count {0}, expected {1}", sortDict.Count, StoreItemCount));
                    sortDict.MoveFirst();
                    do
                    {
                        int i2 = sortDict.CurrentKey;
                        int sss = sortDict.CurrentValue;
                    } while (sortDict.MoveNext());
                }
            }
        }

        [TestMethod]
        public void IterationTest()
        {
            Sop.ObjectServer server = Sop.ObjectServer.OpenWithTransaction("c:\\SopBin\\OServer.dta");
            var df = new Sop.StoreFactory();
            const int ItemCount = 50000;
            for (int i = 0; i < ItemCount; i++)
            {
                string s = string.Format("Collection{0}", i);
                using (var sortDict = df.Get<int, int>(server.SystemFile.Store, s))  //, null, false))
                {
                    sortDict.MoveFirst();
                    do
                    {
                        Console.WriteLine(string.Format("key = {0}, value = {1}", sortDict.CurrentKey, sortDict.CurrentValue));
                    } while (sortDict.MoveNext());
                }
            }
        }

        [TestMethod]
        public void RollbackTest()
        {
            Sop.ObjectServer server = Sop.ObjectServer.OpenWithTransaction("c:\\SopBin\\OServer.dta");
            var df = new Sop.StoreFactory();
            const int ItemCount = 50000;
            long StoreItemCount = 0;
            for (int i = 0; i < ItemCount; i++)
            {
                string s = string.Format("Collection{0}", i);
                using (var sortDict = df.Get<int, int>(server.SystemFile.Store, s))  //, null, false))
                {
                    if (StoreItemCount == 0)
                        StoreItemCount = sortDict.Count;
                    else if (sortDict.Count != StoreItemCount)
                        Assert.Fail(string.Format("sortDict.Count {0}, expected {1}", sortDict.Count, StoreItemCount));
                    for (int i2 = 0; i2 < ItemCount / 10; i2++)
                        sortDict.Add(i2, i2);
                }
            }
            StoreItemCount = 0;
            for (int i = 0; i < ItemCount; i++)
            {
                string s = string.Format("Collection{0}", i);
                using (var sortDict = df.Get<int, int>(server.SystemFile.Store, s)) //, null, false))
                {
                    if (StoreItemCount == 0)
                        StoreItemCount = sortDict.Count;
                    else if (sortDict.Count != StoreItemCount)
                        Assert.Fail(string.Format("sortDict.Count {0}, expected {1}", sortDict.Count, StoreItemCount));
                    for (int i2 = 0; i2 < ItemCount/10; i2++)
                        sortDict.Add(i2, i2);
                }
            }

            server.Commit();
        }

        [TestMethod]
        public void OnLoadProfileValidationTest()
        {
            // todo: modify SOP to support profile validation on File/Store load...

        }

        [TestMethod]
        public void InMemoryTree_RemoveItemTest()
        {
            const int ItemCount = 1;
            var SDODs = new Sop.Collections.Generic.SortedDictionary<int, int>[ItemCount];
            for (int ii = 0; ii < 3; ii++)
            {
                var sortDict = SDODs[0];
                if (sortDict == null)
                {
                    sortDict = new Sop.Collections.Generic.SortedDictionary<int, int>();
                    SDODs[0] = sortDict;
                }
                for (int i2 = 0; i2 < 5000; i2++)
                {
                    sortDict.Add(i2, i2);
                }
                sortDict = SDODs[0];
                for (int i2 = 0; i2 < 500; i2++)
                    sortDict.Remove(i2);
            }
        }
        [TestMethod]
        public void RemoveItem_InMemory()
        {
            int ItemCount = 5000;
            string s = string.Format("Collection{0}", 0);
            var sortDict = new Sop.Collections.Generic.SortedDictionary<int, int>();
            int loopCount = 25;
            for (int i = 0; i < loopCount; i++)
            {
                for (int i2 = 0; i2 < ItemCount; i2++)
                    sortDict.Add(i2, i2);
                for (int i2 = 0; i2 < 500; i2++)
                    sortDict.Remove(i2);
            }

            //for (int i = 0; i < loopCount; i++)
            //{
            //    //for (int i2 = 0; i2 < ItemCount; i2++)
            //    //    Assert.AreEqual(i2, sortDict[i2]);
            //    for (int i2 = 0; i2 < 500; i2++)
            //        sortDict.Remove(i2);
            //}
            //sortDict.Dispose();
        }

        [TestMethod]
        public void RemoveItemNoCommitTest()
        {
            ObjectServer server = ObjectServer.OpenWithTransaction("c:\\SopBin\\OServer.dta");
            var df = new StoreFactory();
            int ItemCount = 5000;
            string s = string.Format("Collection{0}", 0);
            var sortDict = df.Get<int, int>(server.SystemFile.Store, s,
                                            isDataInKeySegment: false);

            int loopCount = 25;
            for (int i = 0; i < loopCount; i++)
            {
                for (int i2 = 0; i2 < ItemCount; i2++)
                    sortDict.Add(i2, i2);
                for (int i2 = 0; i2 < 500; i2++)
                    sortDict.Remove(i2);
            }

            //for (int i = 0; i < loopCount; i++)
            //{
            //    //for (int i2 = 0; i2 < ItemCount; i2++)
            //    //    Assert.AreEqual(i2, sortDict[i2]);
            //    for (int i2 = 0; i2 < 500; i2++)
            //        sortDict.Remove(i2);
            //}
            //sortDict.Dispose();
            server.Commit();
        }


        [TestMethod]
        public void RemoveUpdateItem_InMemoryTest()
        {
            const int ItemCount = 20;
            var SDODs = new Sop.Collections.Generic.SortedDictionary<int, int>[ItemCount];
            for (int ii = 0; ii < 3; ii++)
            {
                int StoreItemCount = 0;
                // add
                for (int i = 0; i < ItemCount; i++)
                {
                    var sortDict = SDODs[i];
                    if (sortDict == null)
                    {
                        sortDict = new Sop.Collections.Generic.SortedDictionary<int, int>();
                        SDODs[i] = sortDict;
                    }
                    if (StoreItemCount == 0)
                        StoreItemCount = sortDict.Count;
                    else if (sortDict.Count != StoreItemCount)
                        Assert.Fail(string.Format("sortDict.Count {0}, expected {1}", sortDict.Count, StoreItemCount));
                    for (int i2 = 0; i2 < 5000; i2++)
                    {
                        sortDict.Add(i2, i2 + i);
                    }
                }
                StoreItemCount = 0;
                // remove
                for (int i = 0; i < ItemCount; i++)
                {
                    var sortDict = SDODs[i];
                    if (StoreItemCount == 0)
                        StoreItemCount = sortDict.Count;
                    else if (sortDict.Count != StoreItemCount)
                        Assert.Fail(string.Format("sortDict.Count {0}, expected {1}", sortDict.Count, StoreItemCount));
                    for (int i2 = 0; i2 < 500; i2++)
                        sortDict.Remove(i2);
                }
                StoreItemCount = 0;
                // update
                for (int i = 0; i < ItemCount; i++)
                {
                    var sortDict = SDODs[i];
                    if (StoreItemCount == 0)
                        StoreItemCount = sortDict.Count;
                    else if (sortDict.Count != StoreItemCount)
                        Assert.Fail(string.Format("sortDict.Count {0}, expected {1}", sortDict.Count, StoreItemCount));
                    for (int i2 = 0; i2 < 500; i2++)
                        sortDict[i2] = i2 * 2 + i;
                }
            }
        }
        [TestMethod]
        public void RemoveItemTest()
        {
            for (int ii = 0; ii < 3; ii++)
            {
                using (ObjectServer server = ObjectServer.OpenWithTransaction("c:\\SopBin\\OServer.dta"))
                {
                    var df = new StoreFactory();
                    const int ItemCount = 20;
                    long StoreItemCount = 0;
                    // add
                    for (int i = 0; i < ItemCount; i++)
                    {
                        string s = string.Format("Collection{0}", i);
                        var sortDict = df.Get<int, int>(server.SystemFile.Store, s, isDataInKeySegment: false);
                        if (StoreItemCount == 0)
                            StoreItemCount = sortDict.Count;
                        else if (sortDict.Count != StoreItemCount)
                            Assert.Fail(string.Format("sortDict.Count {0}, expected {1}", sortDict.Count, StoreItemCount));
                        for (int i2 = 0; i2 < 5000; i2++)
                        {
                            sortDict.Add(i2, i2 + i);
                        }
                        sortDict.Dispose();
                    }
                    StoreItemCount = 0;
                    // remove
                    for (int i = 0; i < ItemCount; i++)
                    {
                        string s = string.Format("Collection{0}", i);
                        var sortDict = df.Get<int, int>(server.SystemFile.Store, s, createIfNotExist:false,
                                                        isDataInKeySegment: false);
                        if (StoreItemCount == 0)
                            StoreItemCount = sortDict.Count;
                        else if (sortDict.Count != StoreItemCount)
                            Assert.Fail(string.Format("sortDict.Count {0}, expected {1}", sortDict.Count, StoreItemCount));
                        for (int i2 = 0; i2 < 500; i2++)
                            sortDict.Remove(i2);
                        sortDict.Dispose();
                    }
                    StoreItemCount = 0;
                    // update
                    for (int i = 0; i < ItemCount; i++)
                    {
                        string s = string.Format("Collection{0}", i);
                        var sortDict = df.Get<int, int>(server.SystemFile.Store, s, createIfNotExist: false,
                                                        isDataInKeySegment: false);
                        if (StoreItemCount == 0)
                            StoreItemCount = sortDict.Count;
                        else if (sortDict.Count != StoreItemCount)
                            Assert.Fail(string.Format("sortDict.Count {0}, expected {1}", sortDict.Count, StoreItemCount));
                        for (int i2 = 0; i2 < 500; i2++)
                            sortDict[i2] = i2 * 2 + i;
                        sortDict.Dispose();
                    }
                    server.Commit();
                }
            }
        }

        [TestMethod]
        public void LargeNumberOfStoresStressTest()
        {
            using (ObjectServer server = ObjectServer.OpenWithTransaction("c:\\SopBin\\OServer.dta"))
            {
                var sf = new StoreFactory();
                // create/open 350! stores or tables...
                const int ItemCount = 350;
                long StoreItemCount = 0;
                for (int i = 0; i < ItemCount; i++)
                {
                    string s = string.Format("Collection{0}", i);
                    var sortDict = sf.Get<int, int>(server.SystemFile.Store, s);
                    if (StoreItemCount == 0)
                        StoreItemCount = sortDict.Count;
                    else if (sortDict.Count != StoreItemCount)
                        Assert.Fail(string.Format("sortDict.Count {0}, expected {1}", sortDict.Count, StoreItemCount));
                    for (int i2 = 0; i2 < 50; i2++)
                    {
                        sortDict.Add(i2, i2 + i);
                    }
                }
                StoreItemCount = 0;
                for (int i = 0; i < ItemCount; i++)
                {
                    string s = string.Format("Collection{0}", i);
                    var sortDict = sf.Get<int, int>(server.SystemFile.Store, s, createIfNotExist:false);
                    if (StoreItemCount == 0)
                        StoreItemCount = sortDict.Count;
                    else if (sortDict.Count != StoreItemCount)
                        Assert.Fail(string.Format("sortDict.Count {0}, expected {1}", sortDict.Count, StoreItemCount));
                    for (int i2 = 0; i2 < 10; i2++)
                        sortDict.Remove(i2);
                }
                server.Commit();
            }
        }
        [TestMethod]
        public void ListAllStoresAndEnumeratorTests()
        {
            using (ObjectServer server = ObjectServer.OpenWithTransaction("c:\\SopBin\\OServer.dta"))
            {
                // set TrackStoreTypes true because it is false by default for MinimalDevice profile scheme (default scheme).
                server.Profile.TrackStoreTypes = true;
                if (server.StoreTypes != null)
                {
                    foreach (var de in server.StoreTypes)
                    {
                        Console.WriteLine("Store UNC Name: {0}, info: {1}", de.Key, de.Value);
                    }
                }
                bool fooBarFound = false;
                server.StoreTypes.Add("foo", "bar");
                foreach (var de in server.StoreTypes)
                {
                    if (de.Key == "foo")
                        fooBarFound = true;
                    Console.WriteLine("Store UNC Name: {0}, info: {1}", de.Key, de.Value);
                }
                Assert.AreEqual(fooBarFound, true);
                server.StoreTypes.Add("foo2", "bar2");
                fooBarFound = false;
                bool foo2Found = false;
                foreach (var de in server.StoreTypes)
                {
                    if (de.Key == "foo2")
                        foo2Found = true;
                    if (de.Key == "foo")
                        fooBarFound = true;
                    Console.WriteLine("Store UNC Name: {0}, info: {1}", de.Key, de.Value);
                }
                Assert.AreEqual(foo2Found, true);
                Assert.AreEqual(fooBarFound, true);
            }
            // NOTE: intentionally left transaction uncommitted to see rollback to work...
        }

        [TestMethod]
        public void NestedStoresTest()
        {
            ObjectServer server = ObjectServer.OpenWithTransaction("c:\\SopBin\\OServer.dta");
            server.Profile.TrackStoreTypes = true;

            var sf = new StoreFactory();
            const int firstLevelCount = 10;
            const int itemsPerLevel = 100;
            long storeItemCount = 0;
            for (int i = 0; i < firstLevelCount; i++)
            {
                string s = string.Format("Collection{0}", i);
                // get a dictionary that will contain a nested dictionary, tell SOP not to manage it in MRU
                // and not to "load" it during b-tree's item Key read from disk.
                using (var sortDict = sf.Get<string, object>(server.SystemFile.Store, s, mruManaged: false, isDataInKeySegment: false))
                {
                    for (int i2 = 0; i2 < itemsPerLevel; i2++)
                    {
                        string s2 = string.Format("ChildSet{0}", i2);
                        // nested dictionaries are MRU managed and data (an int!) is stored in Key segment.
                        var sortChildDict = sf.Get<string, int>(sortDict, s2);
                        if (storeItemCount == 0)
                            storeItemCount = sortChildDict.Count;
                        else if (sortChildDict.Count != storeItemCount)
                            Assert.Fail(string.Format("sortChildDict.Count {0}, expected {1}", sortChildDict.Count,
                                                      storeItemCount));
                        for (int i22 = 0; i22 < 5; i22++)
                            sortChildDict.Add(i22.ToString(), i22 + i);
                    }
                }
                // getting out of scope will dispose from memory the container and all its member on disk dictionaries...
                // note: since sortDict is not managed by SOP, it needs to be manually disposed when done with it.
            }
            storeItemCount = 0;
            for (int i = 0; i < firstLevelCount; i++)
            {
                string s = string.Format("Collection{0}", i);

                using (var sortDict = sf.Get<string, object>(server.SystemFile.Store, s, 
                        createIfNotExist:false, mruManaged: false, isDataInKeySegment: false))
                {
                    for (int i2 = 0; i2 < itemsPerLevel; i2++)
                    {
                        string s2 = string.Format("ChildSet{0}", i2);
                        var sortChildDict = sf.Get<string, int>(sortDict, s2, createIfNotExist:false);
                        if (storeItemCount == 0)
                            storeItemCount = sortChildDict.Count;
                        else if (sortChildDict.Count != storeItemCount)
                            Assert.Fail(string.Format("sortChildDict.Count {0}, expected {1}", sortChildDict.Count,
                                                      storeItemCount));
                        for (int i22 = 0; i22 < 5; i22++)
                            sortChildDict.Add(i22.ToString(), i22 + i);
                    }
                }
            }

            server.Commit();
            server.Dispose();
        }

        public class FooBar : IDisposable
        {
            private bool isDisposed;
            public void Dispose()
            {
                isDisposed = true;
            }
            public string Foo { get; set; }
        }
        [TestMethod]
        public void UserManagedDisposeTest()
        {
            Sop.ObjectServer server = Sop.ObjectServer.OpenWithTransaction("c:\\SopBin\\OServer.dta");
            var df = new Sop.StoreFactory();    // {AutoDisposeItem = false};
            const int ItemCount = 50000;
            List<FooBar> fooBars = new List<FooBar>(ItemCount);
            string s = string.Format("Collection{0}", 1);
            var sortDict = df.Get<int, FooBar>(server.SystemFile.Store, s); //, null, false);    // FooBar is disposable, IsDataInKeySegment = false.
            //sortDict.AutoDisposeItem = true;
            for (int i = 0; i < ItemCount; i++)
            {
                var v = new FooBar() {Foo = string.Format("Hello World {0}", i)};
                sortDict.Add(i, v);
                fooBars.Add(v);
            }
            server.Commit();
            foreach(FooBar fb in fooBars)
                fb.Dispose();
        }

        [TestMethod]
        public void LicenseKeyTestCreate()
        {
            Sop.ObjectServer.LicenseKey = "TestKey";
            Sop.ObjectServer server = Sop.ObjectServer.OpenWithTransaction("c:\\SopBin\\OServer.dta");

            Sop.StoreFactory df = new Sop.StoreFactory();
            var sortDict = df.Get<int, int>(server.SystemFile.Store, "Collection");
            sortDict.Add(1, 1);
            sortDict.Transaction.Commit();
        }

        [TestMethod]
        public void LicenseKeyTestRead()
        {
            Sop.ObjectServer.LicenseKey = "TestKey";
            Sop.IObjectServer server = Sop.ObjectServer.OpenReadOnly("c:\\SopBin\\OServer.dta");
            Sop.StoreFactory df = new Sop.StoreFactory();
            var sortDict = df.Get<int, int>(server.SystemFile.Store, "Collection", createIfNotExist:false);
            var v = sortDict[1];
        }

        [TestMethod]
        public void TestCollection400()
        {
            Sop.Samples.Store400 c400 = new Sop.Samples.Store400();
            c400.Run();
        }
        [TestMethod]
        public void TestBayWind()
        {
            Sop.Samples.BayWind bw = new Sop.Samples.BayWind();
            bw.Run();
        }
        [TestMethod]
        public void TestBTree()
        {
            Sop.ObjectServer server = Sop.ObjectServer.OpenWithTransaction("OServer.dta");
            Sop.StoreFactory df = new Sop.StoreFactory();
            var sortedDict = df.Get<int, int>(server.SystemFile.Store, "Collection");
            sortedDict.Flush();
            sortedDict.Transaction.Commit();
        }
    }
}
