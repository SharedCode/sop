using System;
using System.Collections.Generic;
using System.IO;

namespace Sop.Examples
{
    public static class ClusteredDemo
    {
        public static void Run()
        {
            Console.WriteLine("--- Clustered Database Demo ---");
            Console.WriteLine("Note: This demo requires running Cassandra and Redis instances on localhost.");
            
            try 
            {
                // Initialize Global Connections
                Console.WriteLine("Initializing Redis...");
                Redis.Initialize("redis://localhost:6379");

                // Create Clustered Database
                using var ctx = new Context();
                string dbPath = "data/clustered_demo";
                if (Directory.Exists(dbPath)) Directory.Delete(dbPath, true);

                Console.WriteLine($"Creating Clustered Database at {dbPath}...");
                var db = new Database(new DatabaseOptions
                {
                    StoresFolders = new List<string> { dbPath },
                    Type = (int)DatabaseType.Clustered
                });

                Console.WriteLine("Starting Transaction...");
                using (var trans = db.BeginTransaction(ctx))
                {
                    Console.WriteLine("Creating B-Tree 'cluster_btree'...");
                    var btree = db.NewBtree<string, string>(ctx, "cluster_btree", trans);
                    
                    Console.WriteLine("Adding item...");
                    btree.Add(ctx, new Item<string, string>("key1", "value1"));
                    
                    Console.WriteLine("Committing...");
                    trans.Commit();
                }
                
                Console.WriteLine("Clustered operation successful.");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Clustered demo failed (expected if services are not running): {e.Message}");
            }
            finally
            {
                try
                {
                    Redis.Close();
                }
                catch { }
            }
             Console.WriteLine("--- End of Clustered Demo ---");
        }
    }
}
