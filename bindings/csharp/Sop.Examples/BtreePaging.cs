using System;
using System.Collections.Generic;
using Sop;

namespace Sop.Examples;

public static class BtreePaging
{
    public static void Run()
    {
        Console.WriteLine("\n--- Running B-Tree Paging & Navigation ---");

        using var ctx = new Context();
        var dbOpts = new DatabaseOptions 
        { 
            StoresFolders = new List<string> { "sop_data_paging" },
            Type = (int)DatabaseType.Standalone
        };
        var db = new Database(dbOpts);

        using var trans = db.BeginTransaction(ctx);
        try
        {
            var btree = db.NewBtree<int, string>(ctx, "logs", trans);

            // Populate with 100 items
            Console.WriteLine("Populating 100 log entries...");
            for (int i = 0; i < 100; i++)
            {
                btree.Add(ctx, new Item<int, string>(i, $"Log Entry {i}"));
            }

            // Page 1: First 10 items
            Console.WriteLine("\n--- Page 1 (Items 0-9) ---");
            var pagingInfo = new PagingInfo 
            { 
                PageSize = 10, 
                PageOffset = 0 
            };
            
            // GetKeys is efficient - it scans the index without fetching values
            var page1 = btree.GetKeys(ctx, pagingInfo);
            foreach (var item in page1)
            {
                Console.Write($"{item.Key} ");
            }
            Console.WriteLine();

            // Page 2: Next 10 items
            Console.WriteLine("\n--- Page 2 (Items 10-19) ---");
            // In a real app, you might calculate offset or use the last key from previous page to seek.
            // Here we just use offset for simplicity if supported, or we can simulate "Next Page" logic.
            // Note: SOP's GetKeys typically starts from the current cursor position if not reset.
            
            // Let's move cursor to 10 explicitly for demo
            btree.Find(ctx, 10);
            
            var page2 = btree.GetKeys(ctx, new PagingInfo { PageSize = 10 });
            foreach (var item in page2)
            {
                Console.Write($"{item.Key} ");
            }
            Console.WriteLine();

            // Navigation: First/Last
            Console.WriteLine("\n--- Navigation ---");
            if (btree.First(ctx))
            {
                var first = btree.GetCurrentKey(ctx);
                Console.WriteLine($"First Key: {first.Key}");
            }

            if (btree.Last(ctx))
            {
                var last = btree.GetCurrentKey(ctx);
                Console.WriteLine($"Last Key: {last.Key}");
            }

            trans.Commit();
        }
        catch
        {
            trans.Rollback();
            throw;
        }
    }
}
