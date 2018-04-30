using System;
using System.Collections.Generic;

namespace Sop.Samples
{
	public class PeopleDirectoryWithMediumDB
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

        const int MaxCount = 270000;
        const int TransactionSize = 30000;
		/// <summary>
		/// Sample code for managing "high volume (5,000,000: 2.5 mil people & 2.5 mil people names)"
		/// in a very decent amount of time... Inserts: around 20 minutes on a fairly equipped 2 yr old laptop.
		/// </summary>
		public void Run()
		{
            Sop.Log.Logger.DefaultLogDirectory = ServerPath;
            //Sop.Log.Logger.Instance.LogLevel = Sop.Log.LogLevels.Verbose;

			Console.WriteLine("{0}: PeopleDirectoryLargeDB demo started...", DateTime.Now);

		    IStoreFactory sf = new StoreFactory();
			PeopleStore = sf.Get<long, Person>(Server.SystemFile.Store, "People");
            PeopleStoreByName = sf.Get<PersonKey, long>(Server.SystemFile.Store, "PeopleByName", new PersonComparer());

            if (PeopleStore.Count == 0)
            {
                Populate();
                ReadAll();
            }
            else
            {
                Console.WriteLine("Processing {0} records", PeopleStore.Count * 2);
                ReadAll();
            }
			Server.Commit();
			Console.WriteLine("{0}: PeopleDirectoryLargeDB demo ended...", DateTime.Now);
		}
		//** insert 5 million records on two containers
		void Populate()
		{
			int ZipCodeCtr = 5000;
			Person[] NewPeople = new Person[1000];
			int NewPeopleIndex = 0;
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
				NewPeople[NewPeopleIndex++] = p;
				//** do inserts on People Store By Name every batch of 1000 records
				//** to minimize disk I/O head jumps, causing more optimal insertion times...
				if (NewPeopleIndex == 1000)
				{
					foreach (Person np in NewPeople)
						PeopleStoreByName.Add(np.Key, np.PersonID);
					NewPeopleIndex = 0;
				}
				//** NOTE: SOP supports very large transactions.
				//** In this case we've set it to commit every x00,000 insertions on two tables.
				//** Each one of these operations is a high speed operation and requires fairly reasonable resource footprint
                if (i > 0 && i % TransactionSize == 0)
				{
					if (NewPeopleIndex > 0)
					{
						for (int i2 = 0; i2 < NewPeopleIndex; i2++)
						{
							Person np = NewPeople[i2];
							PeopleStoreByName.Add(np.Key, np.PersonID);
						}
						NewPeopleIndex = 0;
					}
					ZipCodeCtr++;
                    server.CycleTransaction();
				}
			}
			if (NewPeopleIndex > 0)
			{
				for (int i2 = 0; i2 < NewPeopleIndex; i2++)
				{
					Person np = NewPeople[i2];
					PeopleStoreByName.Add(np.Key, np.PersonID);
				}
			}

			ZipCodeCtr++;
            server.CycleTransaction();
		}
		//** read all the 5 million records
		void ReadAll()
		{
			PeopleStoreByName.MoveFirst();
			PeopleStoreByName.HintBatchCount = 303;
			PersonKey pk;
			int Ctr = 0;
			long[] Pids = new long[1000];
			int i = 0;
		    bool personMissing = false;
			do
			{
				Ctr++;
				pk = PeopleStoreByName.CurrentKey;
				long PersonID = PeopleStoreByName.CurrentValue;
				Pids[i++] = PersonID;
				if (i == 1000)
				{
					//** query a thousand people... batching like this is optimal use of SOP container...
					QueryResult<long>[] People;
					if (PeopleStore.Query(QueryExpression<long>.Package(Pids), out People))
					{
                        foreach(var p in People)
                        {
                            if (!p.Found)
                            {
                                personMissing = true;
                                Console.WriteLine("Person with ID {0} not found.", p.Key);
                            }
                        }
					}
					i = 0;
				}
			} while (PeopleStoreByName.MoveNext());
			if (i > 0)
			{
				QueryResult<long>[] People;
				long[] d = new long[i];
				Array.Copy(Pids, 0, d, 0, i);
				if (PeopleStore.Query(QueryExpression<long>.Package(d), out People))
				{
                    foreach (var p in People)
                    {
                        if (!p.Found)
                        {
                            personMissing = true;
                            Console.WriteLine("Person with ID {0} not found.", p.Key);
                        }
                    }
                }
			}
            if (personMissing)
            {
                Console.WriteLine("Failed! Mising person detected.");
                return;
            }
			if (Ctr != PeopleStore.Count)
				Console.WriteLine("Failed! Read {0}, expected {1}", Ctr, PeopleStore.Count);
			else
				Console.WriteLine("Read {0} items.", Ctr);
		}

        private string ServerPath = "c:\\SopBin";
        Sop.IObjectServer Server
		{
			get
			{
				string ServerFilename = string.Format("{0}\\OServer.dta", ServerPath);
				if (server == null)
					server = Sop.ObjectServer.OpenWithTransaction(ServerFilename);
				return server;
			}
		}

        Sop.IObjectServer server;
		ISortedDictionary<long, Person> PeopleStore;
		ISortedDictionary<PersonKey, long> PeopleStoreByName;
	}
}
