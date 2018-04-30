using System;
using System.Collections.Generic;

namespace Sop.Samples
{
    public class Store400 : Sample
    {
        public class Person
        {
            public int PersonId;
            public string FirstName;
            public string LastName;
            public string PhoneNumber;
        }
        /// <summary>
        /// Sample code that tests creation and population of 400 tables with 5000 records each.
        /// </summary>
        public void Run()
        {
            Console.WriteLine("{0}: Store400 demo started...", DateTime.Now);
            const int CollCount = 50;

            using (var Server = new ObjectServer(ServerFilename))
            {
                for (int i = 0; i < CollCount; i++)
                {
                    string CollectionName = string.Format("SystemFile/People{0}", i);
                    var store = Server.StoreNavigator.GetStore<long, Person>(CollectionName);
                }
                Server.CycleTransaction();
                for (int i = 0; i < CollCount; i++)
                {
                    string CollectionName = string.Format("SystemFile/People{0}", i);
                    var store = Server.StoreNavigator.GetStore<long, Person>(CollectionName);
                    if (store.Count == 0)
                    {
                        Populate(store);
                        if (i > 0 && i % 10 == 0)
                        {
                            // no need to cycle(commit/begin) trans, but just to 
                            // simulate highly transactional app...
                            Server.CycleTransaction();
                        }
                    }
                }
                for (int i = 0; i < CollCount; i++)
                {
                    string CollectionName = string.Format("SystemFile/People{0}", i);
                    if (Server.StoreNavigator.Contains(CollectionName))
                    {
                        var store = Server.StoreNavigator.GetStore<long, Person>(CollectionName);
                        ReadAll(store);
                    }
                }
            }
            Console.WriteLine("{0}: Store400 demo ended...", DateTime.Now);
            DeleteDataFolder(ServerFilename);
        }
        void Populate(ISortedDictionary<long, Person> PeopleStore, int count = MaxCount, int seed = 0)
        {
            for (int i = seed; i < count; i++)
            {
                int pid = (int)PeopleStore.GetNextSequence();
                Person p = new Person()
                {
                    PersonId = pid,
                    FirstName = string.Format("Joe{0}", pid),
                    LastName = string.Format("Peter{0}", pid),
                    PhoneNumber = "510-555-9999"
                };
                PeopleStore.Add(p.PersonId, p);
                if (i > 0 && i % 10000 == 0)
                {
                    PeopleStore.File.Server.CycleTransaction();
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
        public const string ServerFilename = "SopBin\\OServer.dta";
        const int MaxCount = 5000;
    }
}
