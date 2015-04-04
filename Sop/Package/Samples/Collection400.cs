using System;
using System.Collections.Generic;

namespace Sop.Samples
{
    public class Collection400
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
        #endregion

		/// <summary>
		/// Sample code that tests creation and population of 400 tables with 5000 records each.
		/// </summary>
        public void Run()
        {
            Console.WriteLine("{0}: Collection400 demo started...", DateTime.Now);
            const int CollCount = 40;

            StoreFactory StoreFactory = new Sop.StoreFactory();
            for (int i = 0; i < CollCount; i++)
            {
                string CollectionName = string.Format("People{0}", i);
                ISortedDictionary<long, Person> store = StoreFactory.Get<long, Person>(Server.SystemFile.Store, CollectionName);
                if (store.Count == 0)
                {
                    Populate(store);
                    store.Transaction.Commit();
                    server.BeginTransaction();
                }
            }
            for (int i = 0; i < CollCount; i++)
            {
                string CollectionName = string.Format("People{0}", i);
                if (Server.SystemFile.Store.Contains(CollectionName))
                {
                    ISortedDictionary<long, Person> store = StoreFactory.Get<long, Person>(Server.SystemFile.Store, CollectionName);
                    ReadAll(store);
                }
            }
            Console.WriteLine("{0}: Collection400 demo ended...", DateTime.Now);
        }
        void Populate(ISortedDictionary<long, Person> PeopleStore)
        {
            for (int i = 0; i < MaxCount; i++)
            {
                int pid = (int)PeopleStore.GetNextSequence();
                Person p = new Person()
                {
                    PersonID = pid,
                    Key = new PersonKey()
                    {
                        FirstName = string.Format("Joe{0}", pid),
                        LastName = string.Format("Peter{0}", pid)
                    },
                    PhoneNumber = "510-555-9999"
                };
                PeopleStore.Add(p.PersonID, p);
                if (i % 10000 == 0)
                {
                    PeopleStore.Transaction.Commit();
                    server.BeginTransaction();
                }
            }
        }
        void ReadAll(ISortedDictionary<long, Person> PeopleStore)
        {
            if (PeopleStore.MoveFirst())
            {
                PeopleStore.HintBatchCount = 103;
                int Ctr = 0;
                do
                {
                    Ctr++;
                    Person p = PeopleStore.CurrentValue;
                } while (PeopleStore.MoveNext());
                if (Ctr != PeopleStore.Count)
                    Console.WriteLine("Failed! Read {0}, expected {1}", Ctr, PeopleStore.Count);
                else
                    Console.WriteLine("Read {0} items.", Ctr);
            }
        }

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
        const int MaxCount = 5000;
        Sop.IObjectServer server;
    }
}
