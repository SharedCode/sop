using System;
using System.Collections.Generic;

namespace Sop.Samples
{
    public class PeopleDirectory
    {
        #region Record definitions & key comparers
        public class PersonKey
        {
            public string FirstName;
            public string LastName;
        }
        public class Person
        {
            public PersonKey Key;
            public int PersonID;
            public string PhoneNumber;
            public int AddressID;
        }
        public class AddressKey
        {
            public string Street;
            public string City;
            public string State;
            public string ZipCode;
            public string Country;
        }
        public class Address
        {
            public AddressKey Key;
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

        /// <summary>
		/// Sample code for managing People and their Addresses
        /// </summary>
        public void Run()
        {
            Console.WriteLine("{0}: PeopleDirectory demo started...", DateTime.Now);

            // create Server (open the data file) and begin a transaction...
            using (var Server = new ObjectServer("SopBin\\OServer.dta", true))
            {
                IStoreFactory sf = new StoreFactory();
                PeopleStore = sf.Get<long, Person>(Server.SystemFile.Store, "People");
                PeopleStoreByName = sf.Get<PersonKey, long>(Server.SystemFile.Store, "PeopleByName", new PersonComparer());

                string AddressFilename = "oFile2";
                Sop.IFile f = Server.GetFile(AddressFilename);
                if (f == null)
                    f = Server.FileSet.Add(AddressFilename);
                AddressStore = sf.Get<long, Address>(f.Store, "Addresses");
                AddressStoreByAddress = sf.Get<AddressKey, long>(f.Store, "AddressesByAddress", new AddressComparer());

                if (PeopleStore.Count == 0)
                    Populate();
                else
                {
                    Console.WriteLine("Processing {0} records", PeopleStore.Count * 4);
                    ReadAll();
                }
                // when code reaches here, 'no exception happened, 'just commit the transaction.
                Server.Commit();
            }   // if error occurred, transaction will be rolled back automatically.

            Console.WriteLine("{0}: PeopleDirectory demo ended...", DateTime.Now);
        }
        void Populate()
        {
            int ZipCodeCtr = 5000;
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
                AddressStore.Add(addr.AddressID, addr);
                PeopleStore.Add(p.PersonID, p);
                PeopleStoreByName.Add(p.Key, p.PersonID);
                AddressStoreByAddress.Add(addr.Key, addr.AddressID);
                if (i % 500 == 0)
                {
                    ZipCodeCtr++;
                    AddressStore.Flush();
                    PeopleStore.Flush();
                    PeopleStoreByName.Flush();
                    AddressStoreByAddress.Flush();
                }
            }
            AddressStore.Flush();
            PeopleStore.Flush();
            PeopleStoreByName.Flush();
            AddressStoreByAddress.Flush();
        }
        void ReadAll()
        {
            PeopleStoreByName.MoveFirst();
            PeopleStoreByName.HintBatchCount = 103;
            PersonKey pk;
            int Ctr = 0;
            do
            {
                Ctr++;
                pk = PeopleStoreByName.CurrentKey;

                long PersonID = PeopleStoreByName.CurrentValue;

                if (PeopleStore.Search(PersonID))
                {
                    Person p = PeopleStore.CurrentValue;
                    if (AddressStore.Search(p.AddressID))
                    {
                        Address addr = AddressStore.CurrentValue;
                    }
                }
            } while (PeopleStoreByName.MoveNext());
        }

        const int MaxCount = 25000;

        ISortedDictionary<long, Person> PeopleStore;
        ISortedDictionary<PersonKey, long> PeopleStoreByName;
        ISortedDictionary<long, Address> AddressStore;
        ISortedDictionary<AddressKey, long> AddressStoreByAddress;
    }
}
