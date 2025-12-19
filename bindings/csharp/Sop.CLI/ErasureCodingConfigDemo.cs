using System;
using System.Collections.Generic;
using System.IO;

namespace Sop.CLI
{
    public static class ErasureCodingConfigDemo
    {
        public static void Run()
        {
            Console.WriteLine("--- Erasure Config Demo ---");
            Console.WriteLine("Note: This demo requires running Redis instance on localhost.");

            // Tell SOP to turn on logging to std err(console).
            Logger.Configure(LogLevel.Info);

            try 
            {
                // Create Clustered Database
                using var ctx = new Context();
                string dbPath = "data/ec_demo";

                var ec = new Dictionary<string, ErasureCodingConfig>
                {
                    {
                        "",
                        new ErasureCodingConfig
                        {
                            DataShards = 1,
                            ParityShards = 1,
                            BaseFolderPathsAcrossDrives = new string[] { dbPath + "/d1", dbPath + "/d2" }
                        }
                    }
                };

                Console.WriteLine($"Creating Clustered Database at {dbPath}...");
                var db = new Database(new DatabaseOptions
                {
                    StoresFolders = new List<string> { dbPath },
                    Type = (int)DatabaseType.Clustered,
                    RedisConfig = new RedisConfig { Address = "localhost:6379" },
                    ErasureConfig = ec,
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
                // Remote the Btree store to cleanup getting ready for next run.
                db.RemoveBtree(ctx, "cluster_btree");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Clustered demo failed (expected if services are not running): {e.Message}");
            }
             Console.WriteLine("--- End of Erasure Coding Demo ---");
        }
    }
}
