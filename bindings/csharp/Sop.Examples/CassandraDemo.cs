using System;
using System.Collections.Generic;
using System.IO;
using Sop;

namespace Sop.Examples
{
    public static class CassandraDemo
    {
        public static void Run()
        {
            Console.WriteLine("--- Cassandra & Redis Demo ---");
            Console.WriteLine("Note: This demo requires running Cassandra and Redis instances on localhost.");
            Console.WriteLine("Ensure you have created the keyspace in Cassandra:");
            Console.WriteLine("CREATE KEYSPACE sop_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");

            var config = new CassandraConfig
            {
                ClusterHosts = new List<string> { "localhost" },
                Consistency = 1, // LocalQuorum
                ConnectionTimeout = 5000,
                ReplicationClause = "{'class':'SimpleStrategy', 'replication_factor':1}",
                Authenticator = new CassandraAuthenticator
                {
                    Username = "",
                    Password = ""
                }
            };

            try
            {
                Console.WriteLine("Initializing Cassandra connection...");
                Cassandra.Initialize(config);
                Console.WriteLine("Cassandra initialized successfully.");

                Console.WriteLine("Initializing Redis connection...");
                Redis.Initialize("redis://localhost:6379");
                Console.WriteLine("Redis initialized successfully.");

                // Create Clustered Database
                using var ctx = new Context();
                string dbPath = "data/cassandra_demo";
                if (Directory.Exists(dbPath)) Directory.Delete(dbPath, true);

                Console.WriteLine($"Creating Cassandra-backed Database at {dbPath}...");
                var db = new Database(new DatabaseOptions
                {
                    StoresFolders = new List<string> { dbPath },
                    Keyspace = "sop_test"
                });

                // 1. Insert
                Console.WriteLine("Starting Write Transaction...");
                using (var trans = db.BeginTransaction(ctx))
                {
                    var btree = db.NewBtree<string, string>(ctx, "cassandra_btree", trans);
                    
                    Console.WriteLine("Adding item 'key1'...");
                    btree.Add(ctx, new Item<string, string>("key1", "value1"));
                    
                    trans.Commit();
                    Console.WriteLine("Committed.");
                }

                // 2. Read
                Console.WriteLine("Starting Read Transaction...");
                using (var trans = db.BeginTransaction(ctx, TransactionMode.ForReading))
                {
                    var btree = db.OpenBtree<string, string>(ctx, "cassandra_btree", trans);
                    
                    if (btree.Find(ctx, "key1"))
                    {
                        var items = btree.GetValues(ctx, "key1");
                        Console.WriteLine($"Found item: Key={items[0].Key}, Value={items[0].Value}");
                    }
                    else
                    {
                        Console.WriteLine("Item not found!");
                    }
                    
                    trans.Commit();
                }
            }
            catch (SopException e)
            {
                Console.WriteLine($"Operation failed: {e.Message}");
            }
            finally
            {
                try { Redis.Close(); } catch { }
                try { Cassandra.Close(); } catch { }
            }

            Console.WriteLine("--- End of Cassandra Demo ---");
        }
    }
}
