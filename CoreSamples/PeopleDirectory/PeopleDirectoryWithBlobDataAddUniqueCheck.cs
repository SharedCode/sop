using System;
using System.Collections.Generic;
using Sop.Persistence;
using Sop.Transaction;

namespace Sop.Samples
{
    public class PeopleDirectoryWithBlobDataAddUniqueCheck
    {
        #region Record definitions & key comparers
        public class Person : IPersistent
        {
            public string FirstName;
            public string LastName;
            public override string ToString()
            {
                return FirstName;
            }

            #region IPersistent
            public bool IsDisposed { get; set; }
            public void Pack(System.IO.BinaryWriter writer)
            {
                writer.Write(FirstName);
                writer.Write(LastName);
            }
            public void Unpack(System.IO.BinaryReader reader)
            {
                FirstName = reader.ReadString();
                LastName = reader.ReadString();
            }
            public int HintSizeOnDisk
            {
                get
                {
                    return 200;
                }
            }
            #endregion
        }
        public class PersonBlob : IPersistent
        {
            // Data stored in Store's Data Segment > 16KB will be streamed to disk.
            // NOTE: this 16KB data size threshold can be changed to your desired size, 
            //  pls. see File.Profile.BigDataBlockCount, and update it to your desired number of blocks.
            //  Default is 32 blocks (32 * 512 block size in bytes = 16KB).
            public const int BlobAvgSize = 16384;
            public byte[] Blob { get; set; }
            public bool IsDisposed { get; set; }
            public void Pack(System.IO.BinaryWriter writer)
            {
                writer.Write(Blob.Length);
                writer.Write(Blob);
            }
            public void Unpack(System.IO.BinaryReader reader)
            {
                var c = reader.ReadInt32();
                Blob = reader.ReadBytes(c);
            }
            public int HintSizeOnDisk
            {
                get
                {
                    return BlobAvgSize;
                }
            }
        }
        public class PersonComparer : IComparer<Person>
        {
            public int Compare(Person x, Person y)
            {
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
		const int MaxCount = 250000;

		/// <summary>
		/// Manage 250K records with Blobs (7,000 byte sized array).
		/// </summary>
		public void Run()
        {
            Console.WriteLine("{0}: PeopleDirectoryWithBlobDataAddUniqueCheck demo started...", DateTime.Now);

            // create Server (open the data file) and begin a transaction...
            using (var Server = new ObjectServer("SopBin\\OServer.dta"))
            {
                var PeopleStore = Server.StoreNavigator.GetStore<Person, PersonBlob>("SystemFile/People",
                                        new StoreParameters<Person>
                                        {
                                            StoreKeyComparer = new PersonComparer(),
                                            AutoFlush = true,
                                            IsDataInKeySegment = false,
                                            //IsUnique = true
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
            Console.WriteLine("{0}: PeopleDirectoryWithBlobDataAddUniqueCheck demo ended...", DateTime.Now);
        }
        void Populate(ISortedDictionary<Person, PersonBlob> PeopleStore)
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
        void ReadAll(ref ISortedDictionary<Person, PersonBlob> PeopleStore)
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
                if (PeopleStore.AddIfNotExist(p, null))
                    Console.WriteLine("Failed! 'expected add to fail due to duplicate key. {0}", p.FirstName);
            }
            Console.WriteLine("Processed {0} records.", Ctr);
        }
    }
}
