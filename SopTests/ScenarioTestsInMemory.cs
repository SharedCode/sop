using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Sop;

namespace SopClientTests
{
    [TestClass]
    public class ScenarioTestsInMemory
    {
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

        int sequence;
        int GetNextSequence()
        {
            return ++sequence;
        }
        void Populate()
        {
            int ZipCodeCtr = 5000;
            Person[] NewPeople = new Person[1000];
            int NewPeopleIndex = 0;
            bool oneTimeUpdateRead = true;
            for (int i = 0; i < MaxCount; i++)
            {
                int pid = GetNextSequence();
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
        }
        void DeleteEachItem()
        {
            PeopleStoreByName.MoveFirst();
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
                    foreach (var pid in Pids)
                    {
                        if (!PeopleStore.Remove(pid))
                        {
                            personMissing = true;
                            Assert.Fail("Person with ID {0} not found.", pid);
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
                foreach (var l in d)
                {
                    if (!PeopleStore.Remove(l))
                    {
                        personMissing = true;
                        Assert.Fail("Person with ID {0} not found.", l);
                    }
                }
            }
            if (personMissing)
            {
                Assert.Fail("Failed! Mising person detected.");
                return;
            }
            if (0 != PeopleStore.Count)
                Assert.Fail("Failed! Read {0}, expected {1}", 0, PeopleStore.Count);
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
        }
        void ReadAll()
        {
            PeopleStoreByName.MoveFirst();
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
                    foreach (var pid in Pids)
                    {
                        if (!PeopleStore.Search(pid))
                        {
                            personMissing = true;
                            Assert.Fail("Person with ID {0} not found.", pid);
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
                foreach (var l in d)
                {
                    if (!PeopleStore.Search(l))
                    {
                        personMissing = true;
                        Assert.Fail("Person with ID {0} not found.", l);
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

        Sop.Collections.Generic.ISortedDictionary<long, Person> PeopleStore;
        Sop.Collections.Generic.ISortedDictionary<PersonKey, long> PeopleStoreByName;

        [TestMethod]
        public void Populate_ReadAndDeleteEachItemTest()
        {
            PeopleStore = new Sop.Collections.Generic.SortedDictionary<long, Person>();
            PeopleStoreByName = new Sop.Collections.Generic.SortedDictionary<PersonKey, long>(new PersonComparer());

            Populate();
            ReadAll();
            DeleteEachItem();
            Assert.IsTrue(PeopleStore.Count == 0, "Expected 0 elements but found {0}", PeopleStore.Count);
        }
    }
}
