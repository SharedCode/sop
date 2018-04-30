//using System;
//using System.Collections.Generic;
//using System.Collections;
//using System.Text;

//using Sop.Collections.OnDisk;
//using Sop.Virtual;

//namespace Sop.Samples
//{
//    public class PeopleDirectoryLargeDB
//    {
//        #region Record definitions & key comparers
//        public class PersonKey
//        {
//            public string FirstName;
//            public string LastName;
//        }
//        public class Person
//        {
//            public PersonKey Key;
//            public int PersonID;
//            public string PhoneNumber;
//        }
//        public class PersonComparer : IComparer<PersonKey>
//        {
//            public int Compare(PersonKey x, PersonKey y)
//            {
//                int i = string.Compare(x.FirstName, y.FirstName);
//                if (i == 0)
//                    i = string.Compare(x.LastName, y.LastName);
//                return i;
//            }
//        }
//        #endregion

//        const int MaxCount = 100000000;

//        /// <summary>
//        /// Sample code for managing "high volume (5,000,000: 2.5 mil people & 2.5 mil people names)"
//        /// in a very decent amount of time... Inserts: around 40 minutes on a 3 yr old, avg equipped laptop.
//        /// </summary>
//        public void Run()
//        {
//            Console.WriteLine("{0}: PeopleDirectoryLargeDB demo started...", DateTime.Now);

//            //PeopleStore = SortedDictionary.GetXmlSerializerValueSimpleKey<long, Person>(
//            //    true, Server.SystemFile.ObjectStore, "People");
//            PeopleStoreByName = SortedDictionary.GetXmlSerializerKeySimpleValue<PersonKey, long>(
//                true, Server.SystemFile.ObjectStore, new PersonComparer(), "PeopleByName");

//            if (PeopleStoreByName.Count == 0)
//                Populate();
//            else
//            {
//                Console.WriteLine("Processing {0} records", PeopleStoreByName.Count * 2);
//                ReadAll();
//            }
//            PeopleStoreByName.Transaction.Commit();
//            Console.WriteLine("{0}: PeopleDirectoryLargeDB demo ended...", DateTime.Now);
//        }
//        //** insert 5 million records on two containers
//        void Populate()
//        {
//            int ZipCodeCtr = 5000;
//            Person[] NewPeople = new Person[1000];
//            int NewPeopleIndex = 0;
//            for (int i = PeopleStoreByName.Count; i < MaxCount; i++)
//            {
//                int pid = (int)PeopleStoreByName.GetNextSequence();
//                Person p = new Person()
//                {
//                    PersonID = pid,
//                    Key = new PersonKey()
//                    {
//                        FirstName = string.Format("Joe{0}", pid),
//                        LastName = string.Format("Peter{0}", pid)
//                    },
//                    PhoneNumber = "510-555-9999"
//                };

//                //if (p.PersonID == 656648)
//                //{
//                //    object o = 90;
//                //}

//                //PeopleStore.Add(p.PersonID, p);
//                //NewPeople[NewPeopleIndex++] = p;
//                //** do inserts on People Store By Name every batch of 1000 records
//                //** to minimize disk I/O head jumps, causing more optimal insertion times...
//                //if (NewPeopleIndex == 1000)
//                {
//                    //foreach (Person np in NewPeople)
//                    PeopleStoreByName.Add(p.Key, p.PersonID);
//                    NewPeopleIndex = 0;
//                }
//                //** NOTE: SOP supports very large transactions.
//                //** In this case we've set it to commit every 300,000 insertions on two tables.
//                //** Each one of these operations is a high speed operation and requires very 
//                //** low resource footprint
//                if (i % 300000 == 0)
//                {
//                    //if (NewPeopleIndex > 0)
//                    //{
//                    //    for (int i2 = 0; i2 < NewPeopleIndex; i2++)
//                    //    {
//                    //        Person np = NewPeople[i2];
//                    //        PeopleStoreByName.Add(np.Key, np.PersonID);
//                    //    }
//                    //    NewPeopleIndex = 0;
//                    //}
//                    ZipCodeCtr++;
//                    PeopleStoreByName.Transaction.Commit();
//                    Sop.Transaction.Transaction.BeginWithNewRoot(server);
//                }
//            }
//            //if (NewPeopleIndex > 0)
//            //{
//            //    for (int i2 = 0; i2 < NewPeopleIndex; i2++)
//            //    {
//            //        Person np = NewPeople[i2];
//            //        PeopleStoreByName.Add(np.Key, np.PersonID);
//            //    }
//            //}
//            //ZipCodeCtr++;
//            PeopleStoreByName.Transaction.Commit();
//            Sop.Transaction.Transaction.BeginWithNewRoot(server);
//        }
//        //** read all the 5 million records
//        void ReadAll()
//        {
//            PeopleStoreByName.MoveFirst();
//            PeopleStoreByName.HintBatchCount = 303;
//            PersonKey pk;
//            int Ctr = 0;
//            long[] Pids = new long[1000];
//            int i = 0;


//            //for (int x = 0; x < 10000; x++)
//            //{
//            //    PeopleStoreByName.MoveLast();
//            //    PeopleStoreByName.Remove();
//            //}
//            //PeopleStoreByName.Save();
//            //for (int x = 0; x < 10000; x++)
//            //{
//            //    PeopleStoreByName.MoveFirst();
//            //    PeopleStoreByName.Remove();
//            //}
//            //PeopleStoreByName.Transaction.Commit();


//            //Sop.Collections.Generic.SortedDictionary<PersonKey, byte> Lookup = 
//            //    new Sop.Collections.Generic.SortedDictionary<PersonKey, byte>(PeopleStoreByName.Comparer);
//            //for (int x = 1; x < MaxCount; x++)
//            //{
//            //    Lookup.Add(
//            //        new KeyValuePair<PersonKey, byte>(new PersonKey()
//            //        {
//            //            FirstName = string.Format("Joe{0}", x),
//            //            LastName = string.Format("Peter{0}", x)
//            //        }, 0));
//            //}
//            //Lookup.MoveFirst();
//            do
//            {
//                Ctr++;
//                pk = PeopleStoreByName.CurrentKey;
//                //if (PeopleStoreByName.Comparer.Compare(Lookup.CurrentKey, pk) != 0)
//                //{
//                //    object o = 90;
//                //}
//                //Lookup.MoveNext();
//                long PersonID = PeopleStoreByName.CurrentValue;
//                Pids[i++] = PersonID;
//                if (i == 1000)
//                {
//                    //** query a thousand people... batching like this is optimal use of SOP container...
//                    Virtual.QueryResult<long, Person>[] People;
//                    //if (PeopleStore.Query(Pids, out People))
//                    //{
//                    //    //** do something here on found People...
//                    //    object o = 90;
//                    //}
//                    i = 0;
//                }
//            } while (PeopleStoreByName.MoveNext());
//            if (i > 0)
//            {
//                Virtual.QueryResult<long, Person>[] People;
//                long[] d = new long[i];
//                Array.Copy(Pids, 0, d, 0, i);
//                //if (PeopleStore.Query(d, out People))
//                //{
//                //    //** do something here on found People...
//                //    object o = 90;
//                //}
//            }
//            if (Ctr != PeopleStoreByName.Count)
//                Console.WriteLine("Failed! Read {0}, expected {1}", Ctr, PeopleStoreByName.Count);
//            else
//                Console.WriteLine("Read {0} items.", Ctr);
//        }

//        Sop.ObjectServerWithTransaction Server
//        {
//            get
//            {
//                string ServerFilename = "c:\\SopBin\\OServer.dta";
//                if (server == null)
//                    server = Sop.Transaction.Transaction.BeginOpenServer(ServerFilename,
//                        new ServerProfile(ProfileSchemeType.Server, DataBlockSize.FiveTwelve));
//                return server;
//            }
//        }

//        Sop.ObjectServerWithTransaction server;
//        //Virtual.ISortedDictionary<long, Person> PeopleStore;
//        Virtual.ISortedDictionary<PersonKey, long> PeopleStoreByName;
//    }
//}
