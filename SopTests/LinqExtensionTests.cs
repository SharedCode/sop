using System;
using System.Linq;
using Sop.Linq;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using Sop;

namespace SopClientTests
{
    [TestClass]
    public class LinqExtensionTests
    {
        [TestMethod]
        public void SimpleLinqTest()
        {
            using (var Server = new ObjectServer("SopBin\\OServer.dta"))
            {
                IStoreFactory sf = new StoreFactory();
                var store = sf.Get<int, string>(Server, "People2");
                store.Add(3, "331");
                store.Add(2, "221");
                store.Add(1, "11");
                store.Add(4, "44");

                var qry = from a in store select a;
                int i = 1;
                foreach (var itm in qry)
                {
                    Assert.IsTrue(itm.Key == i++);
                }
            }
        }

        [TestMethod]
        public void QueryUniqueRecordsTest()
        {
            using (var Server = new ObjectServer("SopBin\\OServer.dta"))
            {
                IStoreFactory sf = new StoreFactory();
                var storeB = sf.Get<int, string>(Server, "People2");
                storeB.Add(1, "11");
                storeB.Add(2, "221");
                storeB.Add(3, "331");
                storeB.Add(4, "44");

                var qry = from a in storeB.Query(new int[] { 1, 2, 3, 4, 5 })
                          select a;
                foreach (var itm in qry)
                {
                    Assert.IsTrue(itm.Key == 1 || itm.Key == 2 || itm.Key == 3 || itm.Key == 4);
                }
            }
        }

        [TestMethod]
        public void QueryNoRecordMatchTest()
        {
            using (var Server = new ObjectServer("SopBin\\OServer.dta"))
            {
                IStoreFactory sf = new StoreFactory();
                var storeB = sf.Get<int, string>(Server, "People2");
                storeB.Add(1, "11");
                storeB.Add(2, "221");
                storeB.Add(2, "222");
                storeB.Add(3, "331");
                storeB.Add(3, "332");
                storeB.Add(4, "44");

                var qry = from a in storeB.Query(new int[] { 5 })
                          select a;
                foreach (var itm in qry)
                {
                    Assert.Fail();
                }
            }
        }
        [TestMethod]
        public void QueryBasicTest()
        {
            using (var Server = new ObjectServer("SopBin\\OServer.dta"))
            {
                IStoreFactory sf = new StoreFactory();
                var storeB = sf.Get<int, string>(Server, "People2");
                storeB.Add(1, "11");
                storeB.Add(2, "221");
                storeB.Add(2, "222");
                storeB.Add(3, "331");
                storeB.Add(3, "332");
                storeB.Add(4, "44");
                var qry = from a in storeB.Query(new int[] { 2, 3, 4 })
                          select a;
                foreach (var itm in qry)
                {
                    Assert.IsTrue(itm.Key == 2 || itm.Key == 3 || itm.Key == 4);
                }
            }
        }
        [TestMethod]
        public void MultipleQueryOnSameLinqBlockTest()
        {
            using (var Server = new ObjectServer("SopBin\\OServer.dta"))
            {
                IStoreFactory sf = new StoreFactory();
                var storeB = sf.Get<int, string>(Server, "People2");
                storeB.Add(1, "11");
                storeB.Add(2, "221");
                storeB.Add(2, "222");
                storeB.Add(3, "331");
                storeB.Add(3, "332");
                storeB.Add(4, "44");

                var qry = from a in storeB.Query(new int[] { 2, 3, 4 })
                          from b in storeB.Query(new int[] { 1, 2 })
                          group a by new { a.Key } into g
                          select g.FirstOrDefault();

                foreach (var itm in qry)
                {
                    var o = itm;
                }
            }
        }
        [TestMethod]
        public void StressTest()
        {
            using (var Server = new ObjectServer("SopBin\\OServer.dta"))
            {
                IStoreFactory sf = new StoreFactory();
                var store = sf.Get<int, string>(Server, "People2");

                // insert 10,000 records, query all of it in an ascending order, 
                // then verify whether Query result contains each of the records in the set,
                // and is in ascending order.
                const int IterationCount = 10000;
                int[] array = new int[IterationCount];
                for (int i = 0; i < IterationCount; i++)
                {
                    store.Add(i, string.Format("Value {0}.", i));
                    array[i] = i;
                }
                // NOTE: in reality, "Query" is not needed because anyways the array specifies
                // all records, so, might as well just Ling it directly from the "store".
                // But this is a good stress test for the Query IEnumerator.
                var qry = from a in store.Query(array) select a;
                int index = 0;
                foreach (var itm in qry)
                {
                    Assert.IsTrue(itm.Key == index++);
                }
            }
        }
    }
}
