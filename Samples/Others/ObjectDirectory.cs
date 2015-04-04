using System;
using System.Collections.Generic;
using System.Linq;
using Sop.Persistence;
using Sop.Transaction;

namespace Sop.Samples
{
    public class ObjectDirectory
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
            public const int BlobAvgSize = 50;  //384;
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

		const int MaxCount = 1000000;

		public void Run()
        {
            Console.WriteLine("{0}: ObjectDirectory demo started...", DateTime.Now);

            // create Server (open the data file) and begin a transaction...
            using (var Server = new ObjectServer("SopBin\\OServer.dta"))
            {
                // get a Store with string as key and object as value, for saving these object types: 
                //  Person, Address, Department, PersonBlob.
                var Store = Server.StoreNavigator.GetStore<string, object>("SystemFile/People",
                    // specify data Values to get stored in data segment as we're intending to save 
                    // XML serialized different types of Objects where data can be somewhat bigger than usual.
                    // Use Key segment if data Values are somewhat small in size.
                    new StoreParameters<string> { IsDataInKeySegment = false });

                if (Store.Count == 0)
                    Populate(Store);
                else
                {
                    Console.WriteLine("Processing {0} records", Store.Count);
                    ReadAll(ref Store);
                }

                // Commit the transaction explicitly. NOTE: pls. see the ObjectServer ctor "commitOnDispose" parameter. 
                // Transaction finalization can be automatically handled as needed. Default is to rollback on ObjectServer dispose.
                Server.Commit();

            }   // when Server goes out of scope, it will auto commit or rollback (default) a pending transaction.
            Console.WriteLine("{0}: ObjectDirectory demo ended...", DateTime.Now);
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
            }
        }
        void ReadAll(ref ISortedDictionary<string, object> Store)
        {
            int Ctr = 0;
            string key = null;
            const int batchCount = 1000;
            string[] batch = new string[batchCount];
            for (int i = 0; i < MaxCount; i++)
            {
                Ctr++;
                switch (i % 4)
                {
                    case 0:
                        key = string.Format("PersonBlob{0}", Ctr);
                        break;
                    case 1:
                        key = string.Format("Person{0}", Ctr);
                        break;
                    case 2:
                        key = string.Format("Address{0}", Ctr);
                        break;
                    case 3:
                        key = string.Format("Dept{0}", Ctr);
                        break;
                }
                batch[i % batchCount] = key;
                if (i > 0 && i % batchCount == 0)
                {
                    QueryResult<string>[] result;
                    if (!Store.Query(batch.Select((a) => new QueryExpression<string>() { Key = a }).ToArray(), out result))
                        Console.WriteLine("Failed! 'can't find object {0}.", key);
                    else
                    {
                        foreach(var o in result)
                        {
                            if (!o.Found)
                                Console.WriteLine("Failed! 'can't find object {0}.", o.Key);
                            if (o.Value == null)
                                Console.WriteLine("Failed! 'found object {0} has null Value.", o.Key);
                        }
                    }
                }
            }
            Console.WriteLine("Processed {0} records.", Ctr);
        }
    }
}
