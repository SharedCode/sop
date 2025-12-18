using System;
using System.Collections.Generic;
using Sop;

namespace Sop.CLI;

public class ProductKey
{
    public string Category { get; set; } = string.Empty;
    public int ProductId { get; set; }
    public bool IsActive { get; set; } // Metadata we might want to update
    public double Price { get; set; }  // Metadata we might want to update

    public override string ToString() => $"{Category}/{ProductId} (Active:{IsActive}, Price:{Price})";
}

public static class BtreeMetadata
{
    public static void Run()
    {
        Console.WriteLine("\n--- Running Metadata 'Ride-on' Keys ---");

        using var ctx = new Context();
        var dbOpts = new DatabaseOptions 
        { 
            StoresFolders = new List<string> { "sop_data_meta" },
            Type = (int)DatabaseType.Standalone
        };
        var db = new Database(dbOpts);

        using var trans = db.BeginTransaction(ctx);
        try
        {
            // Only index Category and ProductId. 
            // IsActive and Price are "Ride-on" metadata - stored in the key but not part of the sort order.
            var indexSpec = new IndexSpecification
            {
                IndexFields = new List<IndexFieldSpecification>
                {
                    new IndexFieldSpecification { FieldName = "Category", AscendingSortOrder = true },
                    new IndexFieldSpecification { FieldName = "ProductId", AscendingSortOrder = true }
                }
            };

            var opts = new BtreeOptions("products") { IndexSpecification = indexSpec };
            var products = db.NewBtree<ProductKey, string>(ctx, "products", trans, opts);

            // Add a product with a large description (Value)
            var key = new ProductKey { Category = "Electronics", ProductId = 999, IsActive = true, Price = 100.0 };
            var largeDescription = new string('X', 10000); // Simulate large payload
            products.Add(ctx, new Item<ProductKey, string>(key, largeDescription));

            Console.WriteLine($"Added: {key}");

            // Scenario: We want to change the Price and IsActive status.
            // Traditional way: Find, GetValue (heavy I/O), Update Value, Update.
            // SOP way: Find, GetCurrentKey (light I/O), Update Key, UpdateCurrentKey.

            if (products.Find(ctx, key))
            {
                // 1. Get the key only (fast, no value fetch)
                var currentItem = products.GetCurrentKey(ctx);
                var currentKey = currentItem.Key;

                Console.WriteLine($"Current Metadata: Price={currentKey.Price}, Active={currentKey.IsActive}");

                // 2. Modify metadata
                currentKey.Price = 120.0;
                currentKey.IsActive = false;

                // 3. Update the key in place
                // This is extremely fast because it doesn't touch the 10KB value on disk.
                products.UpdateCurrentKey(ctx, currentItem);
                Console.WriteLine("Metadata updated via UpdateCurrentKey.");
            }

            // Verify
            if (products.Find(ctx, new ProductKey { Category = "Electronics", ProductId = 999 }))
            {
                var updatedItem = products.GetCurrentKey(ctx);
                Console.WriteLine($"New Metadata: Price={updatedItem.Key.Price}, Active={updatedItem.Key.IsActive}");
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
