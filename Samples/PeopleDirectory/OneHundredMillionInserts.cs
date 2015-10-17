using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using Sop.Linq;

namespace Sop.Samples
{
    public class OneHundredMillionInserts: Sample
    {
        public void Run()
        {
            Console.WriteLine("Start One Hundred Million Inserts demo.");
            var time1 = DateTime.Now;
            using (var Server = new ObjectServer(ServerFilename, true,
                new Preferences
                {
                    StoreSegmentSizeInKb = 1024 * 15,
                    MemoryLimitInPercent = 75,
                    MaxStoreCount = 1,
                    BTreeSlotLength = 500,
                    IsDataInKeySegment = true
                }))
            // set store segment size to 5MB & RAM utilization up to 70%.
            {
                // Pre-populate store to simulate production store with existing items.
                if (Insert)
                {
                    AddItems(Server);
                    Console.WriteLine("Insertion of {0} key/value pairs took {1} mins.",
                        ItemCount, DateTime.Now.Subtract(time1).TotalMinutes);
                }
                else
                    ReadItems(Server);
                Console.WriteLine("End of One Hundred Million Inserts demo.");
            }
        }
        public bool Insert = true;

        const int ItemCount = 100000000;
        private void AddItems(IObjectServer server)
        {
            IStoreFactory sf = new StoreFactory();
            var PeopleStore = sf.Get<long, string>(server.SystemFile.Store, "Lookup");
            const int batchSize = 100000;
            KeyValuePair<long, string>[] batch = new KeyValuePair<long, string>[batchSize];
            for (int i = 0; i < ItemCount;)
            {
                for (int ii = 0; ii < batchSize; ii++, i++)
                {
                    var id = i;
                    batch[ii] = new KeyValuePair<long, string>(id, string.Format("Hello World #{0}.", id));
                }
                PeopleStore.Add(batch);
                Console.WriteLine("{0}: Wrote a batch of {1} items, record count {2}.", DateTime.Now, batchSize, i);
                if (i % (batchSize * 10) == 0)
                    PeopleStore.Flush();
            }
            PeopleStore = null;
        }
        private void ReadItems(IObjectServer server)
        {
            IStoreFactory sf = new StoreFactory();
            var PeopleStore = sf.Get<long, string>(server.SystemFile.Store, "Lookup");
            long id = 1234590;
            var item = PeopleStore[id];
            Console.WriteLine("Read string with ID {0}, result: {1}.", id, item);

            id = 99234590;
            item = PeopleStore[id];
            Console.WriteLine("Read string with ID {0}, result: {1}.", id, item);
            PeopleStore = null;
        }

        public const string ServerFilename = "SopBin\\OServer.dta";
    }
}
