using Sop.SpecializedDataStore;
using System;
using System.Collections.Generic;

namespace Sop.Samples
{
    public class PeopleDirectoryWithUpdate
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
            string filename = null;

            // create Server (open the data file) and begin a transaction...
            using (var Server = new ObjectServer("SopBin\\OServer.dta", true))
            {
                filename = Server.Filename;
                IStoreFactory sf = new StoreFactory();
                for (int i = 0; i < 10; i++)
                {
                    ISortedDictionary<long, Person> PeopleStore;
                    PeopleStore = sf.Get<long, Person>(Server.SystemFile.Store, string.Format("People{0}", i));
                    Populate(Server, PeopleStore, 21308, 0);
                }
                // when code reaches here, 'no exception happened, 'just commit the transaction.
                Server.CycleTransaction();
            }   // if error occurred, transaction will be rolled back automatically.

            // create Server (open the data file) and begin a transaction...
            using (var Server = new ObjectServer("SopBin\\OServer.dta", true))
            {
                IStoreFactory sf = new StoreFactory();
                for (int i = 0; i < 10; i++)
                {
                    ISortedDictionary<long, Person> PeopleStore;
                    PeopleStore = sf.Get<long, Person>(Server.SystemFile.Store, string.Format("People{0}", i));
                    Populate(Server, PeopleStore, 21308 + 30002, 21308);
                }
                // when code reaches here, 'no exception happened, 'just commit the transaction.
                Server.CycleTransaction();
            }   // if error occurred, transaction will be rolled back automatically.
            Console.WriteLine("{0}: PeopleDirectory demo ended...", DateTime.Now);
        }
        void Populate(ObjectServer server,
            ISortedDictionary<long, Person> PeopleStore,
            int maxCount,
            int seed
            )
        {
            int ZipCodeCtr = 5000;
            for (int i = seed; i < maxCount; i++)
            {
                if (i == maxCount -2)
                {
                    object o = 90;
                }
                int aid = i;
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
                PeopleStore.Add(p.PersonID, p);
                if (i % 5000 == 0)
                {
                    ZipCodeCtr++;
                    PeopleStore.Flush();
                }
            }
        }
        void ReadAll(ObjectServer server,
            ISortedDictionary<long, Person> PeopleStore,
            int maxCount
            )
        {
            for (int i = 0; i < maxCount; i++)
            {
                var v = PeopleStore.CurrentValue;
                if (!PeopleStore.MoveNext())
                    break;
            }
        }
    }
}
