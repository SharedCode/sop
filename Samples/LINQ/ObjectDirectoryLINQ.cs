using System;
using System.Collections.Generic;
using System.Linq;

using Sop.Persistence;
using Sop.Transaction;

namespace Sop.Samples
{
    public class ObjectDirectoryLINQ
    {
        #region Models
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
        public class Address
        {
            public string Street;
            public string City;
            public string State;
            public int ZipCode;
            public string Country;
        }
        public class Department
        {
            public int Id;
            public string Name;
        }
        #endregion

		const int MaxCount = 6000;

		public void Run()
        {
            Console.WriteLine("{0}: ObjectDirectoryLINQ demo started...", DateTime.Now);

            // create Server (open the data file) and begin a transaction...
            using (var Server = new ObjectServer("SopBin\\OServer.dta", commitOnDispose: true))
            {
                // get a Store with string as key and object as value, for saving these object types: 
                //  Person, Address, Department, PersonBlob.
                var Store = Server.StoreNavigator.GetStore<string, object>("SystemFile/People");
                if (Store.Count == 0)
                    Populate(Store);
                else
                {
                    Console.WriteLine("Processing {0} records", Store.Count);

                    var result2 = from a in Store
                                  where (a.Key.StartsWith("Person"))
                                  select new { key = a.Key };
                    foreach (var r in result2)
                    {
                        Console.WriteLine("LINQ found entity {0}.", r);
                    }
                    //ReadAll(ref Store);
                }

                // no need to commit as "commitOnDispose" is set to true.
                //Server.Commit();
            }   // when Server goes out of scope, it will auto commit or rollback (default) a pending transaction.

            Console.WriteLine("{0}: ObjectDirectoryLINQ demo ended...", DateTime.Now);
        }
        void Populate(ISortedDictionary<string, object> Store)
        {
            for (int i = 0; i < MaxCount; i++)
            {
                int pid = (int)Store.GetNextSequence();
                string key = string.Empty;
                object value = null;
                switch (i % 4)
                {
                    case 0:
                        key = string.Format("PersonBlob{0}", pid);
                        value = new PersonBlob
                        {
                            Blob = new byte[PersonBlob.BlobAvgSize]
                        };
                        break;
                    case 1:
                        key = string.Format("Person{0}", pid);
                        value = new Person
                        {
                            FirstName = string.Format("Joe {0}", pid),
                            LastName = string.Format("Curly {0}", pid)
                        };
                        break;
                    case 2:
                        key = string.Format("Address{0}", pid);
                        value = new Address
                        {
                            Street = string.Format("123 Love Lane {0}", pid),
                            City = "Fremont",
                            State = "California",
                            Country = "U.S.A.",
                            ZipCode = 94599
                        };
                        break;
                    case 3:
                        key = string.Format("Dept{0}", pid);
                        value = new Department
                        {
                            Id = pid,
                            Name = string.Format("Dept {0}", pid)
                        };
                        break;
                }
                Store.Add(key, value);
                if (i % 2000 == 0)
                    Store.Flush();
            }
        }
    }
}
