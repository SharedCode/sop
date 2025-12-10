using System;
using System.Collections.Generic;
using System.IO;

namespace Sop.Examples
{
    public static class BtreeBatched
    {
        public static void Run()
        {
            Console.WriteLine("--- Batched B-Tree Operations Demo ---");

            using var ctx = new Context();
            string dbPath = "data/batched_demo_db";
            if (Directory.Exists(dbPath)) Directory.Delete(dbPath, true);

            var db = new Database(new DatabaseOptions
            {
                StoresFolders = new List<string> { dbPath },
                Type = (int)DatabaseType.Standalone
            });

            // 1. Batched Add
            Console.WriteLine("\n1. Batched Add (100 items)...");
            using (var trans = db.BeginTransaction(ctx))
            {
                var btree = db.NewBtree<string, string>(ctx, "batched_btree", trans);
                
                var items = new List<Item<string, string>>();
                for (int i = 0; i < 100; i++)
                {
                    items.Add(new Item<string, string> { Key = $"key_{i}", Value = $"value_{i}" });
                }

                btree.Add(ctx, items);
                trans.Commit();
            }
            Console.WriteLine("Committed.");

            // 2. Batched Update
            Console.WriteLine("\n2. Batched Update (100 items)...");
            using (var trans = db.BeginTransaction(ctx))
            {
                var btree = db.OpenBtree<string, string>(ctx, "batched_btree", trans);
                
                var items = new List<Item<string, string>>();
                for (int i = 0; i < 100; i++)
                {
                    items.Add(new Item<string, string> { Key = $"key_{i}", Value = $"updated_value_{i}" });
                }

                btree.Update(ctx, items);
                trans.Commit();
            }
            Console.WriteLine("Committed.");

            // Verify Update
            using (var trans = db.BeginTransaction(ctx, TransactionMode.ForReading))
            {
                var btree = db.OpenBtree<string, string>(ctx, "batched_btree", trans);
                var item = btree.GetValues(ctx, new List<Item<string, string>> { new Item<string, string> { Key = "key_50" } });
                Console.WriteLine($"Verified key_50 value: {item[0].Value}");
                trans.Commit();
            }

            // 3. Batched Remove
            Console.WriteLine("\n3. Batched Remove (100 items)...");
            using (var trans = db.BeginTransaction(ctx))
            {
                var btree = db.OpenBtree<string, string>(ctx, "batched_btree", trans);
                
                var keys = new List<string>();
                for (int i = 0; i < 100; i++)
                {
                    keys.Add($"key_{i}");
                }

                btree.Remove(ctx, keys);
                trans.Commit();
            }
            Console.WriteLine("Committed.");

            // Verify Remove
            using (var trans = db.BeginTransaction(ctx, TransactionMode.ForReading))
            {
                var btree = db.OpenBtree<string, string>(ctx, "batched_btree", trans);
                long count = btree.Count();
                Console.WriteLine($"Verified count: {count}");
                trans.Commit();
            }

            Console.WriteLine("--- End of Batched Demo ---");
        }
    }
}
