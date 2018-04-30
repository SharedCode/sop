using System;

namespace Sop.Samples
{
    public class MultipleFiles
    {
        public class Person
        {
            public string FirstName;
            public string LastName;
            public string PhoneNumber;
            public int AddressID;
        }
        public class Address
        {
            public int AddressID;
            public string Street;
            public string City;
            public string State;
            public string Country;
            public string ZipCode;
        }
        /// <summary>
		/// Shows how to store two different kinds of entities(People & Addresses)
		/// on two Object Stores residing on two different Files.
		/// 
		/// NOTE: target folder paths (c:\SopBin & c:\SopBin2) should exist before running this program or else, it will 
		/// throw when opening the Files due to path not found.
        /// </summary>
        public static void Run()
        {
            Console.WriteLine("{0}: MultipleFiles demo started...", DateTime.Now);
            MultipleFiles MultipleFiles = new MultipleFiles();
            ISortedDictionary<int, Person> PeopleStore = MultipleFiles.GetObjectStore<Person>("People");
            ISortedDictionary<int, Address> AddressStore = MultipleFiles.GetObjectStore<Address>("Address", "OFile2",
                "SopBin2\\oFile2.dta");
            const int MaxCount = 25000;
            if (PeopleStore.Count == 0)
                MultipleFiles.Populate(PeopleStore, AddressStore, MaxCount);
            else
            {
                Console.WriteLine("Processing {0} People", PeopleStore.Count);
                MultipleFiles.Stress(PeopleStore, AddressStore, MaxCount);
                MultipleFiles.ReadAll(PeopleStore, AddressStore, MaxCount);
            }
            PeopleStore.Transaction.Commit();
            Console.WriteLine("{0}: MultipleFiles demo ended...", DateTime.Now);
        }
        void ReadAll(ISortedDictionary<int, Person> PeopleStore,
            ISortedDictionary<int, Address> AddressStore, int MaxCount)
        {
            Console.WriteLine("{0}: Start reading {1} records.", DateTime.Now, PeopleStore.Count);
            PeopleStore.MoveFirst();
            PeopleStore.HintBatchCount = 200;
			int[] Aids = new int[1000];
			int AidsIndex = 0;
            int Ctr = 0;
            while(!PeopleStore.EndOfTree())
            {
                Ctr++;
                Person p = PeopleStore.CurrentValue;
                if (p == null)
                    throw new InvalidOperationException("Person record not found.");
				Aids[AidsIndex++] = p.AddressID;
				if (AidsIndex == 1000)
				{
					//** Do batch Query in set of 1000 Addresses... here we're fully utilizing SOP/disk buffers
					//** and minimizes file pointer jumps
					QueryResult<int>[] addressFound;
					if (AddressStore.Query(QueryExpression<int>.Package(Aids), out addressFound))
					{
						foreach(QueryResult<int> v in addressFound)
						{
							if (!v.Found)
								Console.WriteLine("Address '{0}' not found.", v.Key);
						}
					}
					AidsIndex = 0;
				}
                if (!PeopleStore.MoveNext())
                    break;
                if (Ctr > PeopleStore.Count + 4)
                {
                    Console.WriteLine("Error... Ctr > People Count.");
                    break;
                }
            }
			if (AidsIndex > 0)
			{
				int[] Aids2 = new int[AidsIndex];
				Array.Copy(Aids, 0, Aids2, 0, AidsIndex);
				QueryResult<int>[] AddressFound;
				if (AddressStore.Query(QueryExpression<int>.Package(Aids2), out AddressFound))
				{
					foreach(QueryResult<int> v in AddressFound)
					{
						if (!v.Found)
							Console.WriteLine("Address '{0}' not found.", v.Key);
					}
				}
			}
            if (Ctr != PeopleStore.Count)
                Console.WriteLine("Error, count of records doesn't match tree traversal count!");
            else
                Console.WriteLine("{0}: Finished reading {1} records.", DateTime.Now, PeopleStore.Count);
        }
        void Stress(ISortedDictionary<int, Person> PeopleStore,
            ISortedDictionary<int, Address> AddressStore, int MaxCount)
        {
            PeopleStore.HintBatchCount = 100;
            AddressStore.HintBatchCount = 100;
            PeopleStore.MoveLast();
            int mx = MaxCount;
            Random rdm = new Random(mx);

			//** NOTE: no batching implemented here, just basic operations...
			//** see Populate function how to do batch Add in set of 1000 which optimizes SOP/Disk buffer usage
			//** batch remove can also be done to optimize usage of such buffers & minimize file pointer jumps.
			for (int i = 0; i < MaxCount / 10; i++)
			{
				int no = rdm.Next(mx);
				Person p;

				if (i % 2 == 0)
				{
					Address addr = new Address();
					addr.AddressID = (int)AddressStore.GetNextSequence();
					addr.City = "Fremont";
					addr.Country = "USA";
					addr.State = "California";
					addr.Street = string.Format("143{0} LoveLane", i);
					addr.ZipCode = "99999";
					AddressStore.Add(addr.AddressID, addr);

					p = new Person();
					p.AddressID = addr.AddressID;
					int i2 = no;
					p.FirstName = string.Format("Joe{0}", i2);
					p.LastName = "Peter";
					p.PhoneNumber = "510-555-9999";
					PeopleStore.Add(i2, p);
				}
				else
				{
					if (PeopleStore.TryGetValue(no, out p))
					{
						AddressStore.Remove(p.AddressID);
						PeopleStore.Remove(no);
					}
				}
				if (i % 500 <= 1)
				{
					if (i % 2 == 0)
						PeopleStore.Transaction.Commit();
					else
					{
						PeopleStore.Flush();
						AddressStore.Flush();
						PeopleStore.Transaction.Rollback();
					}
					server.BeginTransaction();
				}
			}
            Console.WriteLine("Stress ended.");
        }
        void Populate(ISortedDictionary<int, Person> PeopleStore,
            ISortedDictionary<int, Address> AddressStore, int MaxCount)
        {
			Person[] NewPeople = new Person[1000];
			int NewPeopleIndex = 0;
			for (int i = 0; i < MaxCount; i++)
            {
                Address addr = new Address();
                addr.AddressID = (int)AddressStore.GetNextSequence();
                addr.City = "Fremont";
                addr.Country = "USA";
                addr.State = "California";
                addr.Street = string.Format("143{0} LoveLane", i);
                addr.ZipCode = "99999";
                AddressStore.Add(addr.AddressID, addr);

				Person p = new Person();
				p.AddressID = addr.AddressID;
				p.FirstName = string.Format("Joe{0}", i);
				p.LastName = "Peter";
				p.PhoneNumber = "510-555-9999";
				NewPeople[NewPeopleIndex++] = p;
				//** Batch add New People each set of 1000
				if (NewPeopleIndex == 1000)
				{
					foreach (Person np in NewPeople)
						PeopleStore.Add((int)PeopleStore.CurrentSequence, np);
					NewPeopleIndex = 0;
				}
            }
			//** add any left over new People haven't been added yet...
			if (NewPeopleIndex > 0)
			{
				for (int i2 = 0; i2 < NewPeopleIndex; i2++)
				{
					Person np = NewPeople[i2];
					PeopleStore.Add((int)PeopleStore.CurrentSequence, np);
				}
				NewPeopleIndex = 0;
			}
		}

        Sop.IObjectServer server;
        Sop.IObjectServer Server
        {
            get
            {
                string ServerFilename = "SopBin\\OServer.dta";
				if (server == null)
					server = Sop.ObjectServer.OpenWithTransaction(ServerFilename);
                return server;
            }
        }
        public ISortedDictionary<int, T> GetObjectStore<T>(string StoreName)
            where T : new()
        {
            IStoreFactory sf = new StoreFactory();
            return sf.Get<int, T>(Server.SystemFile.Store, StoreName);
        }
        public ISortedDictionary<int, T> GetObjectStore<T>(string StoreName, string NameOfFile, string Filename)
            where T : new()
        {
            Sop.IFile f = Server.GetFile(NameOfFile);
            if (f == null)
                f = Server.FileSet.Add(NameOfFile, Filename);   //, new Profile { BTreeSlotLength = 99 });
            IStoreFactory sf = new StoreFactory();
			return sf.Get<int, T>(f.Store, StoreName);
        }
    }
}
