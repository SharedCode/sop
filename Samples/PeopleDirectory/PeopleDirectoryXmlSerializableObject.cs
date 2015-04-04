using System;
using System.Collections.Generic;
using Sop.Persistence;
using Sop.Transaction;

namespace Sop.Samples
{
    public class PeopleDirectoryXmlSerializableObject
    {
        #region Record definitions & key comparers
        public class Person
        {
            public string FirstName;
            public string LastName;
            public override string ToString()
            {
                return FirstName;
            }
        }
        public class PersonBlob
        {
            public const int BlobAvgSize = 384;
            public byte[] Blob { get; set; }
        }
        public class PersonComparer : IComparer<object>
        {
            public int Compare(object a, object b)
            {
                Person x = (Person)a, y = (Person)b;
                int i = string.Compare(x.FirstName, y.FirstName);
                if (i == 0)
                    i = string.Compare(x.LastName, y.LastName);
                return i;
            }
        }
        #endregion

		class CacheRecord
		{
			public Person p;
            public PersonBlob blob;
		}

		//** change MaxCount to your desired count of items to save to see for yourself how fast SOP performs.
		const int MaxCount = 6000;

		/// <summary>
		/// Manage 250K records with Blobs (7,000 byte sized array).
		/// </summary>
		public void Run()
        {
            Console.WriteLine("{0}: PeopleDirectoryXmlSer demo started...", DateTime.Now);

            // create Server (open the data file) and begin a transaction...
            using (var Server = new ObjectServer("SopBin\\OServer.dta"))
            {
                var PeopleStore = Server.StoreNavigator.GetStore<object, object>("SystemFile/People",
                                        new StoreParameters<object>
                                        {
                                            StoreKeyComparer = new PersonComparer(),
                                            AutoFlush = true
                                        });
                if (PeopleStore.Count == 0)
                    Populate(PeopleStore);
                else
                {
                    Console.WriteLine("Processing {0} records", PeopleStore.Count);
                    ReadAll(ref PeopleStore);
                }

                // Commit the transaction explicitly. NOTE: pls. see the ObjectServer ctor "commitOnDispose" parameter. 
                // Transaction finalization can be automatically handled as needed. Default is to rollback on ObjectServer dispose.
                Server.Commit();

            }   // when Server goes out of scope, it will auto commit or rollback (default) a pending transaction.
            Console.WriteLine("{0}: PeopleDirectoryXmlSer demo ended...", DateTime.Now);
        }
        void Populate(ISortedDictionary<object, object> PeopleStore)
        {
            int ZipCodeCtr = 5000;
			CacheRecord[] BatchedRecords = new CacheRecord[BatchCount];
			int BatchedIndex = 0;
            for (int i = 0; i < MaxCount; i++)
            {
                int pid = (int)PeopleStore.GetNextSequence();
                Person p = new Person()
                {
                    FirstName = string.Format("Joe{0}", pid),
                    LastName = string.Format("Peter{0}", pid)
                };
                BatchedRecords[BatchedIndex] = new CacheRecord()
                {
                    p = p,
                    blob = new PersonBlob
                    {
                        Blob = new byte[PersonBlob.BlobAvgSize]
                    }
                };
                BatchedRecords[BatchedIndex].blob.Blob[0] = 1;
                BatchedRecords[BatchedIndex].blob.Blob[5] = 54;
                BatchedIndex++;
				if (BatchedIndex == BatchCount)
				{
					for (int i2 = 0; i2 < BatchedIndex; i2++)
					{
						PeopleStore.Add(BatchedRecords[i2].p, BatchedRecords[i2].blob);
					}
					PeopleStore.Flush();
					if (i % 500 == 0)
						ZipCodeCtr++;
					BatchedIndex = 0;
				}
            }
			if (BatchedIndex > 0)
			{
                for (int i2 = 0; i2 < BatchedIndex; i2++)
                {
                    PeopleStore.Add(BatchedRecords[i2].p, BatchedRecords[i2].blob);
                }
                PeopleStore.Flush();
			}
        }
		const int BatchCount = 1000;
        void ReadAll(ref ISortedDictionary<object, object> PeopleStore)
        {
            var personBatch = new Person[1000];
            int Ctr = 0;
            for (int i = 0; i < MaxCount; i++)
            {
                Ctr++;
                var p = new Person()
                {
                    FirstName = string.Format("Joe{0}", Ctr),
                    LastName = string.Format("Peter{0}", Ctr)
                };
                var personBlob = PeopleStore[p];
                if (personBlob == null)
                    Console.WriteLine("Failed! 'can't find person {0}.", p.FirstName);
            }
            Console.WriteLine("Processed {0} records.", Ctr);
        }
    }
}
