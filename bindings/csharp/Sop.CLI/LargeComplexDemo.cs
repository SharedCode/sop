using System;
using System.Collections.Generic;
using System.IO;
using Sop;

namespace Sop.CLI;

public class DemoPersonKey
{
    public string Country { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public int Ssn { get; set; }
}

public class DemoPerson
{
    public string Name { get; set; } = string.Empty;
    public int Age { get; set; }
    public string Email { get; set; } = string.Empty;
}

public class DemoProductKey
{
    public string Category { get; set; } = string.Empty;
    public string Sku { get; set; } = string.Empty;
}

public class DemoProduct
{
    public string Name { get; set; } = string.Empty;
    public double Price { get; set; }
    public bool Available { get; set; }
}

public static class LargeComplexDemo
{
    public static void Run()
    {
        string dbPath = "data/large_complex_db";
        if (Directory.Exists(dbPath))
        {
            Directory.Delete(dbPath, true);
        }

        Console.WriteLine($"Generating Large Complex DB at {dbPath}...");

        using var ctx = new Context();
        var dbOpts = new DatabaseOptions 
        { 
            StoresFolders = new List<string> { dbPath },
            Type = (int)DatabaseType.Standalone
        };
        var db = new Database(dbOpts);

        using (var trans = db.BeginTransaction(ctx))
        {
            try
            {
                // Store 1: People
                var personIndexSpec = new IndexSpecification
                {
                    IndexFields = new List<IndexFieldSpecification>
                    {
                        new IndexFieldSpecification { FieldName = "Country", AscendingSortOrder = true },
                        new IndexFieldSpecification { FieldName = "City", AscendingSortOrder = true },
                        new IndexFieldSpecification { FieldName = "Ssn", AscendingSortOrder = true }
                    }
                };
                var bo = new BtreeOptions("people") 
                {
                    IsUnique = true,
                    IsPrimitiveKey = false,
                    // ValueDataSize = (int)ValueDataSize.Small, // Not exposed in C# binding yet, defaults to true (Small)
                    IndexSpecification = personIndexSpec
                };
                
                var peopleStore = db.NewBtree<DemoPersonKey, DemoPerson>(ctx, "people", trans, bo);

                var countries = new[] { "US", "UK", "FR", "DE", "JP" };
                var cities = new[] { "CityA", "CityB", "CityC", "CityD" };
                var rnd = new Random();

                int count = 1000;
                Console.WriteLine($"Adding {count} items with Complex keys...");

                var batch = new List<Item<DemoPersonKey, DemoPerson>>();
                for (int i = 0; i < count; i++)
                {
                    var c = countries[rnd.Next(countries.Length)];
                    var ct = cities[rnd.Next(cities.Length)];
                    var k = new DemoPersonKey { Country = c, City = ct, Ssn = 100000 + i };
                    var v = new DemoPerson { Name = $"Person {i}", Age = rnd.Next(20, 80), Email = $"p{i}@example.com" };

                    batch.Add(new Item<DemoPersonKey, DemoPerson>(k, v));

                    if (batch.Count >= 100)
                    {
                        peopleStore.Add(ctx, batch);
                        batch.Clear();
                        Console.WriteLine($"  Added {i + 1}/{count}");
                    }
                }
                if (batch.Count > 0)
                {
                    peopleStore.Add(ctx, batch);
                }

                // Store 2: Products
                var productIndexSpec = new IndexSpecification
                {
                    IndexFields = new List<IndexFieldSpecification>
                    {
                        new IndexFieldSpecification { FieldName = "Category", AscendingSortOrder = true },
                        new IndexFieldSpecification { FieldName = "Sku", AscendingSortOrder = true }
                    }
                };

                var bo2 = new BtreeOptions("products") 
                { 
                    IsUnique = true,
                    IsPrimitiveKey = false,
                    // ValueDataSize = (int)ValueDataSize.Small,
                    IndexSpecification = productIndexSpec
                };

                var productStore = db.NewBtree<DemoProductKey, DemoProduct>(ctx, "products", trans, bo2);
                
                Console.WriteLine("Adding products...");
                var categories = new[] { "Electronics", "Books", "Clothing" };
                for(int i=0; i<100; i++)
                {
                     var cat = categories[rnd.Next(categories.Length)];
                     var k = new DemoProductKey { Category = cat, Sku = $"SKU-{i}" };
                     var v = new DemoProduct { Name = $"Product {i}", Price = rnd.NextDouble() * 100, Available = true };
                     productStore.Add(ctx, new Item<DemoProductKey, DemoProduct>(k, v));
                }

                trans.Commit();
                Console.WriteLine("Transaction committed successfully.");
                Console.WriteLine($"Database created at {Path.GetFullPath(dbPath)}");
                Console.WriteLine("You can now open this with sop-browser!");
            }
            catch (Exception ex)
            {
                trans.Rollback();
                Console.WriteLine($"Error: {ex.Message}");
            }
        }
    }
}
