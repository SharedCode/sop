using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;

using Sop.OnDisk;
using Sop.SpecializedDataStore;

namespace Sop.Samples
{
    public class RenameItemsOfStore
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

            using (var Server = new ObjectServer("SopBin\\OServer.dta"))
            {
                bool readStore = false;
                string CollectionName = "SystemFile/People";
                var store = Server.StoreNavigator.GetStore<long, Person>(CollectionName);
                if (store.Count == 0)
                {
                    Populate(store);
                    if (Server.Transaction != null)
                        Server.Commit();
                    Server.BeginTransaction();
                }
                else
                    readStore = true;

                // rename keys of some records (Remove entries then re-Add w/ different keys).
                for (int i = 1200; i < 1955; i++ )
                {
                    Person p;
                    p = store[i];
                    store.Remove(i);
                    store.Add(i + 10000, p);
                }

                Server.Commit();
                Server.BeginTransaction();
                if (readStore)
                {
                    ReadAll(store);
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
                    Console.WriteLine("Read {0} items. NOTE: this Count will increase for each run...", Ctr);
            }
        }

        const int MaxCount = 2000;
    }
}
