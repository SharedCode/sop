using System;

namespace Sop.Samples
{
    public class MultipleTransactions
    {
        public class Person
        {
            public string FirstName;
            public string LastName;
            public string PhoneNumber;
            public string Address;
        }
		/// <summary>
		/// Shows how to use two Object Servers with separate transaction for each.
		/// </summary>
        public static void Run()
        {
            string CacheFilename = "OFile.dta";
			//** ObjectServer1 is to be physically stored in ...\Sop1 folder
            string ServerPath1 = "SopBin\\Sop1\\";
			//** ObjectServer2 is to be physically stored in ...\Sop2 folder
			string ServerPath2 = "SopBin\\Sop2\\";
            string ServerFilename = "OServer.dta";

            //** create 1st ObjectServer & its transaction
            ObjectServer server = Sop.ObjectServer.OpenWithTransaction(string.Format("{0}{1}", ServerPath1, ServerFilename),
                new Preferences());

			//** create 2nd ObjectServer & its transaction
			string ServerFilename2 = "OServer2.dta";
			string sfullpath2 = string.Format("{0}{1}", ServerPath2, ServerFilename2);
			ObjectServer server2 = Sop.ObjectServer.OpenWithTransaction(sfullpath2);

			string fn = string.Format("{0}{1}{2}", ServerPath1, 1, CacheFilename);
			string fn2 = string.Format("{0}{1}{2}", ServerPath2, 2, CacheFilename);
			Sop.IFile f = server.FileSet[CacheFilename];
            if (f == null)
                f = server.FileSet.Add(CacheFilename, fn);

            IStoreFactory sf = new StoreFactory();
            ISortedDictionary<int, Person>
                store = sf.Get<int, Person>(f.Store, "VirtualCache");
            Sop.IFile f2 = server2.FileSet[CacheFilename];
            if (f2== null)
                f2 = server2.FileSet.Add(CacheFilename, fn2);
            ISortedDictionary<int, Person>
                store2 = sf.Get<int, Person>(f2.Store, "VirtualCache");

            //** insert records
            Console.WriteLine("Start Insertion then Validation of records & their sequence...");

			object o = store.Transaction;
			object o2 = store2.Transaction;

            InsertRecords(store, 20003, "Store1");
			ReadRecords(store, 20003, "Store1");
			store.Transaction.Rollback();

			InsertRecords(store2, 20501, "Store2");
			ReadRecords(store2, 20501, "Store2");
            store2.Transaction.Rollback();

			Console.WriteLine("Done...");
		}
		static void ReadRecords(ISortedDictionary<int, Person> store, int MaxCount, string Salt)
        {
            store.MoveFirst();
            store.HintBatchCount = 99;
            for (int i = 0; i < MaxCount; i++)
            {
                Person p = store.CurrentValue;
                if (p == null ||
					p.FirstName != string.Format("Joe{0}{1}", i, Salt))
                    Console.WriteLine("Error, data for iteration {0} not found.", i);
                store.MoveNext();
            }
            if (store.EndOfTree())
                Console.WriteLine("Store's End of tree reached.");
        }
        static void InsertRecords(ISortedDictionary<int, Person> store, int MaxCount, string Salt)
        {
            for (int i = 0; i < MaxCount; i++)
            {
                Person p = new Person();
				p.FirstName = string.Format("Joe{0}{1}", i, Salt);
                p.LastName = "Castanas";
                p.PhoneNumber = "510-324-2222";
                p.Address = string.Format("5434 {0} Coventry Court, Fremont, Ca. 94888, U.S.A.", i);
                store.Add(i, p);
            }
            Console.WriteLine("InsertRecords ({0}) end.", MaxCount);
        }
    }
}
