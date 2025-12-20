using System;
using System.Collections.Generic;
using System.IO;

namespace Sop.CLI
{
    public static class FullReplicationConfigDemo
    {
        public static void Run()
        {
            Console.WriteLine("--- Full Replication Config Demo ---");
            Console.WriteLine("Note: This demo requires running Redis instance on localhost.");

            // Tell SOP to turn on logging to std err(console).
            Logger.Configure(LogLevel.Info);

            try 
            {
                // Create Clustered Database
                using var ctx = new Context();
                string dbPath = "data/full_repl_demo";

                List<string> storesFolders = new List<string>{ dbPath + "/d1", dbPath + "/d2" };
                var ec = new Dictionary<string, ErasureCodingConfig>
                {
                    {
                        "",
                        new ErasureCodingConfig
                        {
                            DataShards = 1,
                            ParityShards = 1,
                            BaseFolderPathsAcrossDrives = storesFolders.ToArray()
                        }
                    }
                };

                Console.WriteLine($"Creating Clustered Database at {dbPath}...");
                var dbOpts = new DatabaseOptions
                {
                    StoresFolders = storesFolders,
                    Type = (int)DatabaseType.Clustered,
                    RedisConfig = new RedisConfig { Address = "localhost:6379" },
                    ErasureConfig = ec,
                };

                // Setup the database so the config can be persisted and read/used by other tools like SOP httpService.
                Database.Setup(ctx, dbOpts);

                var db = new Database(dbOpts);

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
             Console.WriteLine("--- End of Full Replication Demo ---");
        }
    }
}
