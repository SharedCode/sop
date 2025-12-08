using System;
using System.Collections.Generic;
using Sop;

namespace Sop.Examples;

public static class BtreeBasic
{
    public static void Run()
    {
        Console.WriteLine("\n--- Running Basic B-Tree Operations ---");

        using var ctx = new Context();
        
        // 1. Open Database
        var dbOpts = new DatabaseOptions 
        { 
            StoresFolders = new List<string> { "sop_data_basic" },
            Type = (int)DatabaseType.Standalone
        };
        var db = new Database(dbOpts);

        // 2. Start Transaction
        using var trans = db.BeginTransaction(ctx);
        try
        {
            // 3. Create/Open B-Tree
            var btree = db.NewBtree<string, string>(ctx, "users_basic", trans);

            // 4. Add Items (Create)
            Console.WriteLine("Adding users...");
            btree.Add(ctx, new Item<string, string>("user1", "Alice"));
            btree.Add(ctx, new Item<string, string>("user2", "Bob"));
            btree.Add(ctx, new Item<string, string>("user3", "Charlie"));

            // 5. Find & Get (Read)
            if (btree.Find(ctx, "user1"))
            {
                var items = btree.GetValues(ctx, "user1");
                Console.WriteLine($"Found: {items[0].Key} -> {items[0].Value}");
            }

            // 6. Update
            Console.WriteLine("Updating user2...");
            btree.Update(ctx, new List<Item<string, string>> 
            { 
                new Item<string, string>("user2", "Bob Updated") 
            });

            if (btree.Find(ctx, "user2"))
            {
                var items = btree.GetValues(ctx, "user2");
                Console.WriteLine($"Updated: {items[0].Key} -> {items[0].Value}");
            }

            // 7. Remove (Delete)
            Console.WriteLine("Removing user3...");
            btree.Remove(ctx, new List<string> { "user3" });

            if (!btree.Find(ctx, "user3"))
            {
                Console.WriteLine("user3 removed successfully.");
            }

            // 8. Commit
            trans.Commit();
            Console.WriteLine("Transaction committed.");
        }
        catch (Exception ex)
        {
            trans.Rollback();
            Console.WriteLine($"Error: {ex.Message}");
        }
    }
}
