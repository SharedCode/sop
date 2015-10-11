using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Sop.Samples
{
    public class ManyClientSimulator : Sample
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
        /// Sample code for managing "high volume (5,000,000: 2.5 mil people & 2.5 mil people names)"
        /// in a very decent amount of time... Inserts: around 20 minutes on a fairly equipped 2 yr old laptop.
        /// </summary>
        public void Run()
        {
            const int ThreadCount = 20;
            using (var Server = new ObjectServer(ServerFilename, true))
            {
                IStoreFactory sf = new StoreFactory();

                List<Action> actions = new List<Action>();
                // create threads that will populate Virtual Cache and retrieve the items.
                for (int i = 0; i < ThreadCount; i++)
                {
                    // function to execute by the thread.
                    if (i % 10 == 0)
                    {
                        actions.Add(() =>
                        {
                            AddItems(Server, sf);
                        });
                        continue;
                    }
                    actions.Add(() =>
                    {
                        ReadItems(Server, sf);
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
                if (_threaded)
                    Task.WaitAll(tasks.ToArray());
                Console.WriteLine("End of VirtualCache demo.");
            }
        }

        private bool _threaded = true;
        private Task TaskRun(Action action)
        {
            if (!_threaded)
            {
                action();
                return null;
            }
            return Task.Run(action);
        }

        private void AddItems(IObjectServer server, IStoreFactory sf)
        {
            var PeopleStore = sf.Get<long, Person>(server.SystemFile.Store, "People");

        }
        private void ReadItems(IObjectServer server, IStoreFactory sf)
        {
            var PeopleStore = sf.Get<long, Person>(server.SystemFile.Store, "People");

        }

        public const string ServerFilename = "SopBin\\OServer.dta";
    }
}
