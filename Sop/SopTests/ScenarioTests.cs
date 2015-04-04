using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Sop;

namespace SopClientTests
{
    [TestClass]
    public class ScenarioTests
    {
        [TestMethod]
        public void PopulateReadAllDeleteItems_Test()
        {
            // set default directory ahead of LogLevel
            Sop.Log.Logger.DefaultLogDirectory = "c:\\SopBin";
            //Sop.Log.Logger.Instance.LogLevel = Sop.Log.LogLevels.Verbose;
            //**

            IStoreFactory sf = new StoreFactory();
            PeopleStore = sf.GetPersistentValue<long, Person>(Server.SystemFile.Store, "People");
            PeopleStoreByName = sf.GetPersistentKey<PersonKey, long>(Server.SystemFile.Store, "PeopleByName", new PersonComparer());

            // repeat Populate, Read all, Delete each Item routines: iterate for n times...
            const int iterationCount = 6;
            for (int i = 0; i < iterationCount; i++)
            {
                Populate();
                ReadAll();
                if (i % 2 == 0)
                    DeleteEachItem();
                else
                {
                    PeopleStore.Delete();
                    PeopleStoreByName.Delete();
                }
                Assert.IsTrue(PeopleStore.Count == 0, "Expected 0 elements but found {0}", PeopleStore.Count);
            }
        }


        #region Record definitions & key comparers
        public class PersonKey : Sop.Persistence.Persistent
        {
            public string FirstName;
            public string LastName;

            public override void Pack(System.IO.BinaryWriter writer)
            {
                writer.Write(FirstName);
                writer.Write(LastName);
            }

            public override void Unpack(System.IO.BinaryReader reader)
            {
                FirstName = reader.ReadString();
                LastName = reader.ReadString();
            }
        }
        public class Person : Sop.Persistence.Persistent
        {
            public PersonKey Key;
            public int PersonID;
            public string PhoneNumber;

            public override void Pack(System.IO.BinaryWriter writer)
            {
                Key.Pack(writer);
                writer.Write(PersonID);
                writer.Write(PhoneNumber);
            }

            public override void Unpack(System.IO.BinaryReader reader)
            {
                if (Key == null)
                    Key = new PersonKey();
                Key.Unpack(reader);
                PersonID = reader.ReadInt32();
                PhoneNumber = reader.ReadString();
            }
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

        void Populate()
        {
            if (server.Transaction == null)
                server.BeginTransaction();

            int ZipCodeCtr = 5000;
            Person[] NewPeople = new Person[1000];
            int NewPeopleIndex = 0;
            bool oneTimeUpdateRead = true;
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
                    server.Commit();
                    server.BeginTransaction();
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
            server.Commit();
        }
        void DeleteEachItem()
        {
            if (server.Transaction == null)
                server.BeginTransaction();

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
                    if (PeopleStore.Remove(QueryExpression<long>.Package(Pids), out People))
                    {
                        foreach (var p in People)
                        {
                            if (!p.Found)
                            {
                                personMissing = true;
                                Assert.Fail("Person with ID {0} not found.", p.Key);
                            }
                        }
                    }
                    else
                        Assert.Fail("Failed to Remove a 1,000 people starting with PID {0}.", Pids[0]);
                    i = 0;
                }
            } while (PeopleStoreByName.MoveNext());
            if (i > 0)
            {
                QueryResult<long>[] People;
                long[] d = new long[i];
                Array.Copy(Pids, 0, d, 0, i);
                if (PeopleStore.Remove(QueryExpression<long>.Package(d), out People))
                {
                    foreach (var p in People)
                    {
                        if (!p.Found)
                        {
                            personMissing = true;
                            Assert.Fail("Person with ID {0} not found.", p.Key);
                        }
                    }
                }
            }
            if (personMissing)
            {
                Assert.Fail("Failed! Mising person detected.");
                return;
            }
            if (Ctr != MaxCount)
                Assert.Fail("Failed! Deleted {0}, expected {1}", Ctr, MaxCount);
            else
                Console.WriteLine("Deleted {0} items on 2ndary store.", Ctr);

            Ctr = 0;
            while (PeopleStoreByName.MoveFirst())
            {
                Ctr++;
                pk = PeopleStoreByName.CurrentKey;
                PeopleStoreByName.Remove();
            }
            Console.WriteLine("Deleted {0} items.", Ctr);
            server.Commit();
        }
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
                        foreach (var p in People)
                        {
                            if (!p.Found)
                            {
                                personMissing = true;
                                Assert.Fail("Person with ID {0} not found.", p.Key);
                            }
                        }
                    }
                    else
                        Assert.Fail("Failed to Query a 1,000 people starting with PID {0}.", Pids[0]);
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
                            Assert.Fail("Person with ID {0} not found.", p.Key);
                        }
                    }
                }
            }
            if (personMissing)
            {
                Assert.Fail("Failed! Mising person detected.");
                return;
            }
            if (Ctr != PeopleStore.Count)
                Assert.Fail("Failed! Read {0}, expected {1}", Ctr, PeopleStore.Count);
            else
                Console.WriteLine("Read {0} items.", Ctr);
        }


        Sop.IObjectServer Server
        {
            get
            {
                string ServerFilename = "c:\\SopBin\\OServer.dta";
                if (server == null)
                    server = Sop.ObjectServer.OpenWithTransaction(ServerFilename, new Preferences() { MaxCollectionCount = 7 });
                return server;
            }
        }

        Sop.IObjectServer server;
        ISortedDictionary<long, Person> PeopleStore;
        ISortedDictionary<PersonKey, long> PeopleStoreByName;
    }
}
