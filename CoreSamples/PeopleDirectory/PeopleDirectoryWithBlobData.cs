using System;
using System.Collections.Generic;
using Sop.Persistence;

namespace Sop.Samples
{
    public class PeopleDirectoryWithBlobData
    {
        #region Record definitions & key comparers
        public class Person : IPersistent
        {
            public string FirstName;
            public string LastName;

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
            Console.WriteLine("{0}: PeopleDirectoryUsingIPersistent demo started...", DateTime.Now);

            // create Server (open the data file) and begin a transaction...
            using (var Server = new ObjectServer("SopBin\\OServer.dta"))
            {

                IStoreFactory sf = new StoreFactory();

                // set last parameter (IsDataInKeySegment) to false as we'll store Blob data of 16 KB size each.
                PeopleStore = sf.GetPersistent<Person, PersonBlob>(Server.SystemFile.Store, "People", new PersonComparer(), true, false);
                // Set the Store to AutoFlush so inserted Blob data will get mapped to disk right away 
                // and not buffered in Store's MRU cache (a.k.a. - streaming).
                PeopleStore.AutoFlush = true;

                if (PeopleStore.Count == 0)
                    Populate();
                else
                {
                    Console.WriteLine("Processing {0} records", PeopleStore.Count);

                    ReadAll();
                }
                Server.Commit();
            }

            Console.WriteLine("{0}: PeopleDirectoryUsingIPersistent demo ended...", DateTime.Now);
        }
        void Populate()
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
		void ReadAll()
        {
            PeopleStore.MoveFirst();
            PeopleStore.HintBatchCount = 103;
			var pk = new PersonBlob[BatchCount];
            int Ctr = 0;

            do
            {
                Ctr++;
				var blob = PeopleStore.CurrentValue;
            } while (PeopleStore.MoveNext());
            Console.WriteLine("Processed {0} records.", Ctr);
        }

        ISortedDictionary<Person, PersonBlob> PeopleStore;
    }
}
