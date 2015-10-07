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
        public void ExploreLinqToObjectTest()
        {
            var ints = new int[2] { 1, 2 };
            var qry = from a in ints
                      where a == 1
                      select a;
            foreach(var item in qry)
            {
                var o = item;
            }
        }

        [TestMethod]
        public void TestMethod1()
        {
            using (var Server = new ObjectServer("SopBin\\OServer.dta"))
            {
                IStoreFactory sf = new StoreFactory();
                var store = sf.Get<int, string>(Server, "People");
                store.Add(1, "1");
                store.Add(2, "2");
                var storeB = sf.Get<int, string>(Server, "People2");
                storeB.Add(1, "11");
                storeB.Add(2, "22");
                storeB.Add(3, "33");

                //var q =
                //    from c in store
                //    join p in storeB on c.Key equals p.Key into ps
                //    from p in ps.DefaultIfEmpty()
                //    select new
                //    {
                //        f = c.Key,
                //        e = p.Key
                //    };

                //foreach(var itm in q)
                //{
                //    var ooo = itm;
                //}

                var qry = from a in store.Query(new int[] { 1 })
                        //where store.ContainsKey(new int[] { 1, 2 })
                          select a;

                foreach (var itm in qry)
                {
                    var o = itm;
                }


                //var qry2 = from a in store
                //          where ().ContainsKey(1)
                //          select a;

                //foreach (var itm in qry)
                //{
                //    var o = itm;
                //}

            }
        }
    }
}
