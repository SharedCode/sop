using System;
using System.Collections.Generic;

namespace Sop.Samples
{
    /// <summary>
    /// Bay Wind sample program.
    /// </summary>
    public class BayWind : IDisposable
    {
        public static string ServerPath = "SopBin\\";
        public static string ServerFilename = ServerPath + "OServer.dta";

        #region Person related entities
        /// <summary>
        /// Compares Firstname then Lastname.
        /// </summary>
		public class PersonComparer : IComparer<PersonKey>
		{
			public int Compare(PersonKey x, PersonKey y)
			{
				int i = string.Compare(x.FirstName, y.FirstName);
				if (i == 0)
					i = string.Compare(x.LastName, y.LastName);
				return i;
			}
		}
        /// <summary>
        /// FirstName and LastName comprises the PersonKey.
        /// </summary>
		public class PersonKey
		{
			public string FirstName;
			public string LastName;
		}
        /// <summary>
        /// Person 'record'.
        /// </summary>
        public class Person
        {
			public string FirstName { get; set; }
            public string LastName { get; set; }
			public PersonKey GetKey()
			{
				return new PersonKey()
				{
					FirstName = this.FirstName,
					LastName = this.LastName
				};
			}
			public string PhoneNumber { get; set; }
            public int AddressID;
        }
        /// <summary>
        /// Address 'record'.
        /// </summary>
        public class Address
        {
            public int AddressID;
            public string Street;
            public string City;
            public string State;
            public string Country;
            public string ZipCode;
        }
        #endregion

        public Sop.IObjectServer Server
        {
            get
            {
                return _server ?? (_server = Sop.ObjectServer.OpenWithTransaction(ServerFilename));
            }
        }
        private Sop.IObjectServer _server;

        /// <summary>
		/// Sample code to test general application usage of SOP.
		/// This creates, populates and reads all records of 2 tables, People & Addresses.
		/// This also showcases technique to take advantage of SOP feature of 
		/// "optimized" insertion/reading by doing said operations in batch per table.
		/// 
		/// NOTE: this is purely for demo purposes only. Ideal implementation will not
		/// necessarily need two tables. Person and Address records may be merged
		/// to one table for real world use.
        /// </summary>
        public void Run()
        {
            Console.WriteLine("{0}: BayWind demo started...", DateTime.Now);

            //** Create a Store Factory
            var storeFactory = new StoreFactory();

			ISortedDictionary<PersonKey, Person> PeopleStore =
                storeFactory.Get<PersonKey, Person>(Server.SystemFile.Store, "People", new PersonComparer());
            ISortedDictionary<int, Address> AddressStore =
                storeFactory.Get<int, Address>(Server.SystemFile.Store, "Address");

			const int MaxCount = 3000;
            if (PeopleStore.Count == 0)
            {
                Populate(PeopleStore, AddressStore, MaxCount);
                Server.Commit();
            }
            else
                Read(PeopleStore, AddressStore, MaxCount);
            Console.WriteLine("{0}: BayWind demo ended...", DateTime.Now);
        }

        /// <summary>
        /// Dispose will commit the transaction if it isn't commited yet.
        /// </summary>
        public void Dispose()
        {
            if (_server != null)
            {
                if (_server.SystemFile.Store.Transaction != null)
                {
                    //server.SystemFile.ObjectStore.Transaction.Commit();
                    _server.Transaction.Commit();
                }
                _server.Dispose();
                _server = null;
            }
        }

        #region Populate/Read methods

        private void Populate(ISortedDictionary<PersonKey, Person> PeopleStore,
            ISortedDictionary<int, Address> AddressStore, int MaxCount)
        {
            int[] AddressIDs = new int[AddressBatchCount];
            int AddressBatchIndex = 0;
            // insert Person and Address records onto PeopleStore and AddressStore in batch (bulk insert!)
            for (int i = 1; i <= MaxCount; i++)
            {
                Address addr = new Address();
                addr.AddressID = (int)AddressStore.GetNextSequence();
                addr.City = "Fremont";
                addr.Country = "USA";
                addr.State = "California";
                addr.Street = string.Format("143{0} LoveLane", i);
                addr.ZipCode = "99999";
                AddressStore.Add(addr.AddressID, addr);

                AddressIDs[AddressBatchIndex++] = addr.AddressID;
                //** in this block we've caused People to be inserted in batch of 1000
                if (AddressBatchIndex == AddressBatchCount)
                {
                    //** insert People in batch of 1000
                    int PhoneCtr = 1000;
                    for (int AddressID = 0; AddressID < AddressIDs.Length; AddressID++)
                    {
                        Person p = new Person();
                        p.AddressID = AddressIDs[AddressID];
                        p.FirstName = string.Format("{0}Joe", p.AddressID);
                        p.LastName = "Peter";
                        p.PhoneNumber = string.Format("510-555-{0}", PhoneCtr++);
                        PeopleStore.Add(p.GetKey(), p);
                    }
                    AddressBatchIndex = 0;
                }
            }
            // if last batch wasn't inserted yet, then insert it
            if (AddressBatchIndex > 0)
            {
                for (int AddressID = 0; AddressID < AddressBatchIndex; AddressID++)
                {
                    Person p = new Person();
                    p.AddressID = AddressIDs[AddressID];
                    p.FirstName = string.Format("Joe{0}", p.AddressID);
                    p.LastName = "Peter";
                    p.PhoneNumber = "510-555-9999";
                    PeopleStore.Add(p.GetKey(), p);
                }
            }
        }

        // "Bulk" read all Person and Address records from respective Stores...
        private void Read(ISortedDictionary<PersonKey, Person> PeopleStore,
            ISortedDictionary<int, Address> AddressStore, int MaxCount)
        {
            PeopleStore.MoveFirst();
            // tell People Store it can do read ahead of 77 Persons.
            PeopleStore.HintBatchCount = 77;
            AddressStore.MoveFirst();
            // tell Address Store it can do read ahead of 78 Addresses.
            AddressStore.HintBatchCount = 78;
			KeyValuePair<int, int>[] AddressIDs = new KeyValuePair<int,int>[AddressBatchCount];
			int AddressBatchIndex = 0;
			for (int i = 1; i <= MaxCount; i++)
            {
                Person p = PeopleStore.CurrentValue;
                if (p.FirstName != string.Format("Joe{0}", i))
                    Console.WriteLine("Error detected, expected Joe{0} not found in this sequence from disk", i);
                else
                {
					AddressIDs[AddressBatchIndex++] = new KeyValuePair<int, int>(p.AddressID, i);
					if (AddressBatchIndex == AddressBatchCount)
					{
						int[] a2 = new int[AddressBatchCount];
						for (int i2 = 0; i2 < AddressBatchCount; i2++)
							a2[i2] = AddressIDs[i2].Key;
						// Query a batch of 1000 addresses. NOTE: doing batch query is optimal operation 
                        // as it minimizes segment jumps of the HDD "disk head".
						QueryResult<int>[] Addresses;
						if (AddressStore.Query(QueryExpression<int>.Package(a2), out Addresses))
						{
							for (int i2 = 0; i2 < AddressBatchCount; i2++)
							{
								Address addr = (Address)Addresses[i2].Value;
								if (addr == null ||
									addr.Street != string.Format("143{0} LoveLane", AddressIDs[i2].Value))
									Console.WriteLine("Error detected, expected Address 143{0} not found in this sequence from disk",
										AddressIDs[i2].Value);
							}
						}
						AddressBatchIndex = 0;
					}
                }
                PeopleStore.MoveNext();
            }
            if (!PeopleStore.EndOfTree())
                Console.WriteLine("Expected EOT but isn't.");
            Console.WriteLine("Reading all data({0}) ended.", MaxCount);
        }
        #endregion
        
        private const int AddressBatchCount = 1000;
	}
}
