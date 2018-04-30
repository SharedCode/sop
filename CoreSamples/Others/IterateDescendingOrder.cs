using System;
using System.Collections.Generic;

namespace Sop.Samples
{
    public class IterateDescendingOrder
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
		/// Sample to show how to retrieve in descending order items of B-Tree in SOP.
		/// Count of records processed is 50,000.
        /// </summary>
        public void Run()
        {
            Console.WriteLine("{0}: IterateDescendingOrder demo started...", DateTime.Now);

            IStoreFactory sf = new StoreFactory();

            PeopleStore = sf.Get<long, Person>(Server.SystemFile.Store, "People");
			PeopleStoreByName = sf.Get<PersonKey, long>(Server.SystemFile.Store, "PeopleByName", new PersonComparer());

            if (PeopleStore.Count == 0)
                Populate();
            else
            {
                Console.WriteLine("Processing {0} Records", PeopleStore.Count * 2);
                ReadAll();
            }
            //PeopleStore.Transaction.Rollback();
            PeopleStore.Transaction.Commit();
            Console.WriteLine("{0}: IterateDescendingOrder demo ended...", DateTime.Now);
        }
        void Populate()
        {
            int ZipCodeCtr = 5000;
			Person[] PeopleBuffer = new Person[BatchCount];
            int PeopleBufferIndex = 0;
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
                PeopleBuffer[PeopleBufferIndex++] = p;

				//** Insert to PeopleStoreByName every batch of BatchCount People
                //** This allows optimal insertion across two tables as low level File pointer jumps are minimized
				if (PeopleBufferIndex == BatchCount)
                {
                    PeopleStore.Flush();
                    foreach (Person p2 in PeopleBuffer)
                        PeopleStoreByName.Add(p2.Key, p2.PersonID);
                    PeopleStoreByName.Flush();
                    PeopleBufferIndex = 0;
                }
                if (i % 500 == 0)
                    ZipCodeCtr++;
            }
            if (PeopleBufferIndex > 0)
            {
                PeopleStore.Flush();
                foreach (Person p2 in PeopleBuffer)
                    PeopleStoreByName.Add(p2.Key, p2.PersonID);
                PeopleStoreByName.Flush();
            }
            else
            {
                PeopleStore.Flush();
                PeopleStoreByName.Flush();
            }
        }
		const int BatchCount = 1000;
        void ReadAll()
        {
            //** Set Sort Order to descending to retrieve in opposite key ordering
            PeopleStoreByName.SortOrder = SortOrderType.Descending;

			long[] PeopleBuffer = new long[BatchCount];
			int PeopleBufferIndex = 0;
			int Ctr = 0;
            foreach(KeyValuePair<PersonKey, long> kvp in PeopleStoreByName)
            {
                Ctr++;
                long PersonID = kvp.Value;
				PeopleBuffer[PeopleBufferIndex++] = PersonID;
				if (PeopleBufferIndex == BatchCount)
				{
					QueryResult<long>[] PeopleFound;
					if (PeopleStore.Query(QueryExpression<long>.Package(PeopleBuffer), out PeopleFound))
					{
						for (int i = 0; i < PeopleBufferIndex; i++)
						{
							if (PeopleFound[i].Found)
							{
								Person p = (Person)PeopleFound[i].Value;
							}
						}
					}
					PeopleBufferIndex = 0;
				}
            }
			if (PeopleBufferIndex > 0)
			{
				QueryResult<long>[] PeopleFound;
				if (PeopleStore.Query(QueryExpression<long>.Package(PeopleBuffer), out PeopleFound))
				{
					for (int i = 0; i < PeopleBufferIndex; i++)
					{
						if (PeopleFound[i].Found)
						{
							Person p = (Person)PeopleFound[i].Value;
						}
					}
				}
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

        const int MaxCount = 25000;

        Sop.IObjectServer server;
        ISortedDictionary<long, Person> PeopleStore;
        ISortedDictionary<PersonKey, long> PeopleStoreByName;
    }
}
