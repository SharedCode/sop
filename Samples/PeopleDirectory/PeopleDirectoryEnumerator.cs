using System;
using System.Collections.Generic;
using Sop.Persistence;

namespace Sop.Samples
{
    public class PeopleDirectoryEnumerator
    {

        //todo: modify below to illustrate Store Enumerator usage!!


        #region Record definitions & key comparers
        public class PersonKey : IPersistent
        {
            public string FirstName;
            public string LastName;

            public bool IsDisposed { get; set; }
            public void Pack(System.IO.BinaryWriter writer)
            {
                writer.Write(FirstName);
                writer.Write(LastName);
            }
            public void Unpack(System.IO.BinaryReader reader)
            {
                FirstName = reader.ReadString();
                LastName = reader.ReadString();
            }
            public int HintSizeOnDisk
            {
                get
                {
                    return 200;
                }
            }
        }
        public class Person : IPersistent
        {
            public bool IsDisposed { get; set; }
            public void Pack(System.IO.BinaryWriter writer)
            {
                Key.Pack(writer);
                writer.Write(PersonID);
                writer.Write(PhoneNumber);
                writer.Write(AddressID);
            }
            public void Unpack(System.IO.BinaryReader reader)
            {
                Key.Unpack(reader);
                PersonID = reader.ReadInt32();
                PhoneNumber = reader.ReadString();
                AddressID = reader.ReadInt32();
            }
            public int HintSizeOnDisk
            {
                get
                {
                    return Key.HintSizeOnDisk + 108;
                }
            }
            public PersonKey Key = new PersonKey();
            public int PersonID;
            public string PhoneNumber;
            public int AddressID;
        }
        public class AddressKey : IPersistent
        {
            public bool IsDisposed { get; set; }
            public string Street;
            public string City;
            public string State;
            public string ZipCode;
            public string Country;

            public void Pack(System.IO.BinaryWriter writer)
            {
                writer.Write(Street);
                writer.Write(City);
                writer.Write(State);
                writer.Write(ZipCode);
                writer.Write(Country);
            }
            public void Unpack(System.IO.BinaryReader reader)
            {
                Street = reader.ReadString();
                City = reader.ReadString();
                State = reader.ReadString();
                ZipCode = reader.ReadString();
                Country = reader.ReadString();
            }
            public int HintSizeOnDisk
            {
                get
                {
                    return 365;
                }
            }
        }
        public class Address : IPersistent
        {
            public bool IsDisposed { get; set; }
            public void Pack(System.IO.BinaryWriter writer)
            {
                Key.Pack(writer);
                writer.Write(AddressID);
            }
            public void Unpack(System.IO.BinaryReader reader)
            {
                Key.Unpack(reader);
                AddressID = reader.ReadInt32();
            }
            public int HintSizeOnDisk
            {
                get
                {
                    return Key.HintSizeOnDisk + sizeof(int);
                }
            }
            public AddressKey Key = new AddressKey();
            public int AddressID;
        }
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
        public class AddressComparer : IComparer<AddressKey>
        {
            public int Compare(AddressKey x, AddressKey y)
            {
                int i = string.Compare(x.Street, y.Street);
                if (i == 0)
                    i = string.Compare(x.City, y.City);
                if (i == 0)
                    i = string.Compare(x.State, y.State);
                if (i == 0)
                    i = string.Compare(x.ZipCode, y.ZipCode);
                if (i == 0)
                    i = string.Compare(x.Country, y.Country);
                return i;
            }
        }
        #endregion

		class CacheRecord
		{
			public Person p;
			public PersonKey pKey;
			public Address addr;
			public AddressKey addrKey;
		}

		//** change MaxCount to your desired count of items to save to see for yourself how fast SOP performs.
		const int MaxCount = 250000;   //250000;

		/// <summary>
		/// Sample code for managing People and their Addresses using the "IPersistent" 
		/// interface as method to save said objects to SOP DB.
		/// NOTE: this yields optimal disk usage as you control up to byte level what
		/// info to save. BUT schema (for Person and Address)
		/// versioning(not shown) needs to be managed by the application code.
		/// Xml Serialized objects has advantage on schema versioning as .Net's 
		/// Xml Serialization supports versioning built-in. BUT it isn't hard to
		/// implement IPersistent schema versioning.
		/// 
		/// Also, this demo shows SOP managing about 1 million records (250K records on 4 tables)
		/// </summary>
		public void Run()
        {
            Console.WriteLine("{0}: PeopleDirectoryUsingIPersistent demo started...", DateTime.Now);

            using (var Server = new ObjectServer("SopBin\\OServer.dta",
                                        true, new Profile() { BTreeSlotLength = 200 }))
            {
                IStoreFactory sf = new StoreFactory();
                PeopleStore = sf.GetPersistentValue<long, Person>(Server.SystemFile.Store, "People");
                PeopleStoreByName = sf.GetPersistentKey<PersonKey, long>(Server.SystemFile.Store, "PeopleByName", new PersonComparer());

                string AddressFilename = "oFile2";
                Sop.IFile f = Server.GetFile(AddressFilename);
                if (f == null)
                    f = Server.FileSet.Add(AddressFilename);
                AddressStore = sf.GetPersistentValue<long, Address>(f.Store, "Addresses");
                AddressStoreByAddress = sf.GetPersistentKey<AddressKey, long>(f.Store, "AddressesByAddress", new AddressComparer());

                if (PeopleStore.Count == 0)
                    Populate();
                else
                {
                    Console.WriteLine("Processing {0} records", PeopleStore.Count * 4);
                    ReadAll();
                }
                Server.Commit();
            }
            Console.WriteLine("{0}: PeopleDirectoryUsingIPersistent demo ended...", DateTime.Now);
        }
        void Populate()
        {
            int ZipCodeCtr = 5000;
			CacheRecord[] BatchedRecords = new CacheRecord[BatchCount];
			int BatchedIndex = 0;
            for (int i = 0; i < MaxCount; i++)
            {
                int aid = (int)AddressStore.GetNextSequence();
                Address addr = new Address()
                {
                    AddressID = aid,
                    Key = new AddressKey()
                    {
                        Street = string.Format("143{0} LoveLane", aid),
                        City = "Fremont",
                        Country = "USA",
                        State = "California",
                        ZipCode = ZipCodeCtr.ToString()
                    }
                };
                int pid = (int)PeopleStore.GetNextSequence();
                Person p = new Person()
                {
                    PersonID = pid,
                    AddressID = addr.AddressID,
                    Key = new PersonKey()
                    {
                        FirstName = string.Format("Joe{0}", pid),
                        LastName = string.Format("Peter{0}", pid)
                    },
                    PhoneNumber = "510-555-9999"
                };
				BatchedRecords[BatchedIndex++] = new CacheRecord()
				{
					p = p,
					pKey = p.Key,
					addr = addr,
					addrKey = addr.Key
				};
				if (BatchedIndex == BatchCount)
				{
					for (int i2 = 0; i2 < BatchedIndex; i2++)
					{
						AddressStore.Add(BatchedRecords[i2].addr.AddressID,
							BatchedRecords[i2].addr);
					}
					AddressStore.Flush();
					for (int i2 = 0; i2 < BatchedIndex; i2++)
					{
						PeopleStore.Add(BatchedRecords[i2].p.PersonID,
							BatchedRecords[i2].p);
					}
					PeopleStore.Flush();
					for (int i2 = 0; i2 < BatchedIndex; i2++)
					{
						PeopleStoreByName.Add(BatchedRecords[i2].p.Key,
							BatchedRecords[i2].p.PersonID);
					}
					PeopleStoreByName.Flush();
					for (int i2 = 0; i2 < BatchedIndex; i2++)
					{
						AddressStoreByAddress.Add(BatchedRecords[i2].addr.Key,
							BatchedRecords[i2].addr.AddressID);
					}
					AddressStoreByAddress.Flush();
					if (i % 500 == 0)
						ZipCodeCtr++;
					BatchedIndex = 0;
				}
            }
			if (BatchedIndex > 0)
			{
				for (int i2 = 0; i2 < BatchedIndex; i2++)
				{
					AddressStore.Add(BatchedRecords[i2].addr.AddressID,
						BatchedRecords[i2].addr);
				}
				AddressStore.Flush();
				for (int i2 = 0; i2 < BatchedIndex; i2++)
				{
					PeopleStore.Add(BatchedRecords[i2].p.PersonID,
						BatchedRecords[i2].p);
				}
				PeopleStore.Flush();
				for (int i2 = 0; i2 < BatchedIndex; i2++)
				{
					PeopleStoreByName.Add(BatchedRecords[i2].p.Key,
						BatchedRecords[i2].p.PersonID);
				}
				PeopleStoreByName.Flush();
				for (int i2 = 0; i2 < BatchedIndex; i2++)
				{
					AddressStoreByAddress.Add(BatchedRecords[i2].addr.Key,
						BatchedRecords[i2].addr.AddressID);
				}
				AddressStoreByAddress.Flush();
			}
        }
		const int BatchCount = 1000;
		void ReadAll()
        {
            PeopleStoreByName.MoveFirst();
            PeopleStoreByName.HintBatchCount = 103;
			long[] Pids = new long[BatchCount];
			int PidsIndex = 0;
			PersonKey[] pk = new PersonKey[BatchCount];
            int Ctr = 0;

            do
            {
                Ctr++;
				pk[PidsIndex++] = PeopleStoreByName.CurrentKey;
				if (PidsIndex == BatchCount)
				{
					QueryResult<PersonKey>[] PeopleIDs;
					if (PeopleStoreByName.Query(QueryExpression<PersonKey>.Package(pk), out PeopleIDs))
					{
						for (int i = 0; i < PeopleIDs.Length; i++)
							Pids[i] = (long)PeopleIDs[i].Value;

						QueryResult<long>[] PeopleFound;
						if (PeopleStore.Query(QueryExpression<long>.Package(Pids), out PeopleFound))
						{
							long[] Aids = new long[PidsIndex];
							int i = 0;
							foreach (QueryResult<long> pf in PeopleFound)
							{
								if (pf.Found)
									Aids[i++] = ((Person)pf.Value).AddressID;
							}
							QueryResult<long>[] AddressesFound;
							if (AddressStore.Query(QueryExpression<long>.Package(Aids), out AddressesFound))
							{
								//** process found Address records here...
                                int ctr2 = 0;
                                foreach (var a in AddressesFound)
                                {
                                    ctr2++;
                                    if (!a.Found)
                                        Console.WriteLine("Failed to read {0}.", a.Key);
                                }
                                if (ctr2 != 1000)
                                    Console.WriteLine("Failed to read 1000 records, 'only read {0}.", ctr2);
							}
						}
					}
					PidsIndex = 0;
				}
            } while (PeopleStoreByName.MoveNext());

            if (Ctr != PeopleStore.Count)
                Console.WriteLine("Failed! Read {0}, expected {1}", Ctr * 4, PeopleStore.Count * 4);
            else
                Console.WriteLine("Read {0} items.", Ctr * 4);
        }

        ISortedDictionary<long, Person> PeopleStore;
        ISortedDictionary<PersonKey, long> PeopleStoreByName;
        ISortedDictionary<long, Address> AddressStore;
        ISortedDictionary<AddressKey, long> AddressStoreByAddress;
    }
}
