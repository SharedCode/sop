using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using Sop.Linq;

namespace Sop.Samples
{
    public class ManyClientSimulator : Sample
    {
        public class Person
        {
            public long PersonId;
            public string FirstName;
            public string LastName;
            public string PhoneNumber;
        }

        public void Run()
        {
            Console.WriteLine("Start of Many Client Simulator demo.");
            var time1 = DateTime.Now;
            using (var Server = new ObjectServer(ServerFilename, true, 
                new Preferences {  StoreSegmentSizeInKb = 1024  * 5}))
                // set store segment size to 5MB, more intensive 
                // data I/O could use lesser Segment resize. Default is 1MB.
            {
                // Pre-populate store to simulate production store with existing items.
                AddItems(Server);

                List<Action> actions = new List<Action>();
                // create threads that will populate Virtual Cache and retrieve the items.
                for (int i = 0; i < ThreadCount; i++)
                {
                    // specify Insertion delegate
                    if (i % 2 == 0)
                    {
                        if (i < DataInsertionThreadCount * 2)
                        {
                            actions.Add(() =>
                            {
                                AddItems(Server);
                            });
                            continue;
                        }
                    }
                    // specify Reader delegate
                    actions.Add(() =>
                    {
                        ReadItems(Server);
                    });
                }

                List<Task> tasks = new List<Task>();
                // launch or start the threads all at once.
                foreach (var a in actions)
                {
                    var t = TaskRun(a);
                    if (t == null)
                        continue;
                    tasks.Add(t);
                }
                // wait until all threads are finished.
                if (Threaded)
                    Task.WaitAll(tasks.ToArray());
                //IStoreFactory sf = new StoreFactory();
                //var PeopleStore = sf.Get<long, Person>(Server.SystemFile.Store, "People");
                Console.WriteLine("Processed, inserted ({0} threads) & queried/enumerated multiple times ({1} threads),", 
                    DataInsertionThreadCount, ThreadCount - DataInsertionThreadCount);

                // just to "exercise" the API a bit more, get Store via Factory. :)
                IStoreFactory sf = new StoreFactory();
                using (var PeopleStore = sf.Get<long, Person>(Server.SystemFile.Store, "People"))
                {
                    var totalIO = (ThreadCount - DataInsertionThreadCount) * 1000 + DataInsertionThreadCount * ItemCount;
                    Console.WriteLine("a total of {0} records & total of {1} I/Os in {2} mins.",
                        PeopleStore.Count, totalIO, DateTime.Now.Subtract(time1).TotalMinutes);
                }
                Console.WriteLine("End of Many Client Simulator demo.");
            }
        }

        public bool Threaded = true;
        private Task TaskRun(Action action)
        {
            if (!Threaded)
            {
                action();
                return null;
            }
            return Task.Run(action);
        }

        const int ItemCount = 10000;
        private void AddItems(IObjectServer server)
        {
            IStoreFactory sf = new StoreFactory();
            var PeopleStore = sf.Get<long, Person>(server.SystemFile.Store, "People");
            const int batchSize = 1000;
            KeyValuePair<long, Person>[] batch = new KeyValuePair<long, Person>[batchSize];
            for (int i = 0; i < ItemCount;)
            {
                for (int ii = 0; ii < batchSize; ii++, i++)
                {
                    var id = PeopleStore.GetNextSequence();
                    batch[ii] = new KeyValuePair<long, Person>(id,
                        new Person
                        {
                            PersonId = id,
                            FirstName = string.Format("Joe{0}", id),
                            LastName = string.Format("Petit{0}", id),
                            PhoneNumber = "555-999-4444"
                        });
                }
                PeopleStore.Locker.Invoke(() => { PeopleStore.Add(batch); });
                Console.WriteLine("{0}: Wrote a batch of {1} items.", DateTime.Now, batchSize);
                System.Threading.Thread.Sleep(1);
            }
            PeopleStore = null;
        }
        private void ReadItems(IObjectServer server)
        {
            IStoreFactory sf = new StoreFactory();
            var PeopleStore = sf.Get<long, Person>(server.SystemFile.Store, "People");
            var r = new Random();
            var maxValue = (int)(PeopleStore.CurrentSequence / ItemCount);
            if (maxValue <= 0)
                maxValue = 1;
            maxValue *= 10;
            var ItemsToRead = 1000;
            var i = r.Next(maxValue) * ItemsToRead;
            var keys = new long[1000];

            int logicalIndex = 0;
            for (int i2 = 0; i2 < ItemsToRead / keys.Length; i2++)
            {
                int c;
                for (c = 0; c < keys.Length; c++)
                {
                    keys[c] = ++logicalIndex + i + 1;
                }
                // just use Store and do Linq to Objects. Store & enumerators are thread safe.
                // do batch query.
                var qry = from a in PeopleStore.Query(keys, true) select a;
                c = 0;
                foreach (var p in qry)
                {
                    if (p.Value == null)
                    {
                        Console.WriteLine("Person with no Value found from DB.");
                        continue;
                    }
                    var personName = string.Format("{0} {1}", p.Value.FirstName, p.Value.LastName);
                    if (p.Key % 25 == 0)
                        Console.WriteLine("Person found {0} from DB.", personName);
                    if (keys[c] != p.Key)
                        Console.WriteLine(string.Format("Failed, didn't find person with key {0}, found {1} instead.", keys[c], p.Key));
                    c++;
                }
                // don't be a resource hog. :)
                if (i2 % 2 == 0)
                    System.Threading.Thread.Sleep(1);
            }
            PeopleStore = null;
        }
        public int DataInsertionThreadCount = 5;
        public int ThreadCount = 20;
        public const string ServerFilename = "SopBin\\OServer.dta";
    }
}
