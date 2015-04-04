using System;
using System.Collections.Generic;
using Sop.SpecializedDataStore;

namespace Sop.Samples
{
    public class NestedSortedDictionary
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
		/// Sample code to show how to create nested Sorted Dictionaries.
		/// I.e. - Collections within Collection scenario.
        /// </summary>
        public void Run()
        {
            Console.WriteLine("{0}: NestedSortedDictionary demo started...", DateTime.Now);

            const int CollCount = 50;
            ITransaction Trans;

            IStoreFactory Factory = new StoreFactory();

			//** Create/Get the Main Collection which we will store the nested "People" Sorted Dictionaries in below code...
            ISortedDictionary<string, GeneralPurpose<long, Person>> Collections = 
                Factory.Get<string, GeneralPurpose<long, Person>>(Server.SystemFile.Store, "MainCollection");

            for (int i = 0; i < CollCount; i++)
            {
                string CollectionName = string.Format("People{0}", i);
                ISortedDictionary<long, Person> store = Factory.Get<long, Person>(Collections, CollectionName);
                Trans = store.Transaction;
				if (store.Count == 0)
				{
					Populate(store);
					store.Flush();
					//store.Dispose();
				}
                //else
                //    store.Dispose();
                if (i >= 10)
                {
					//** get the table 5 tables "ago"
                    store = Factory.Get<long, Person>(Collections, string.Format("People{0}", i - 5));
					//** delete the table retrieved...
					//store.Clear();

					store.Rename("foo");

                    ISortedDictionary<long, Person> fooStore = Factory.Get<long, Person>(Collections, "foo");
                    //store.Delete();
                }
                Trans.Commit();
                server.BeginTransaction();
            }
            for (int i = 0; i < CollCount; i++)
            {
                string CollectionName = string.Format("People{0}", i);
                ISortedDictionary<long, Person> store = Factory.Get<long, Person>(Collections, CollectionName, createIfNotExist:false);
                if (store != null)
                {
                    ReadAll(store);
                    //store.Dispose();
                }
            }
            Console.WriteLine("{0}: NestedSortedDictionary demo ended...", DateTime.Now);
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
