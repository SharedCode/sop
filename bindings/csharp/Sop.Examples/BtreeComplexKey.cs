using System;
using System.Collections.Generic;
using Sop;

namespace Sop.Examples;

public class EmployeeKey
{
    public string Region { get; set; } = string.Empty;
    public string Department { get; set; } = string.Empty;
    public int Id { get; set; }

    public override string ToString() => $"{Region}/{Department}/{Id}";
}

public static class BtreeComplexKey
{
    public static void Run()
    {
        Console.WriteLine("\n--- Running Complex Keys & Index Specification ---");

        using var ctx = new Context();
        var dbOpts = new DatabaseOptions 
        { 
            StoresFolders = new List<string> { "sop_data_complex" },
            Type = (int)DatabaseType.Standalone
        };
        var db = new Database(dbOpts);

        using (var trans = db.BeginTransaction(ctx))
        {
            try
            {
                // Define Index Specification
                // This tells SOP how to construct the composite key for sorting and prefix scanning.
                var indexSpec = @"{
                    ""index_fields"": [
                        { ""name"": ""Region"", ""ascending_sort_order"": true },
                        { ""name"": ""Department"", ""ascending_sort_order"": true },
                        { ""name"": ""Id"", ""ascending_sort_order"": true }
                    ]
                }";

                var opts = new BtreeOptions("employees") { IndexSpecification = indexSpec };
                var employees = db.NewBtree<EmployeeKey, string>(ctx, "employees", trans, opts);

                Console.WriteLine("Adding employees...");
                employees.Add(ctx, new Item<EmployeeKey, string>(
                    new EmployeeKey { Region = "US", Department = "Sales", Id = 101 }, "Alice"));
                employees.Add(ctx, new Item<EmployeeKey, string>(
                    new EmployeeKey { Region = "US", Department = "Sales", Id = 102 }, "Bob"));
                employees.Add(ctx, new Item<EmployeeKey, string>(
                    new EmployeeKey { Region = "US", Department = "Engineering", Id = 201 }, "Charlie"));
                employees.Add(ctx, new Item<EmployeeKey, string>(
                    new EmployeeKey { Region = "EU", Department = "Sales", Id = 301 }, "David"));

                // Exact Match
                var keyToFind = new EmployeeKey { Region = "US", Department = "Sales", Id = 101 };
                if (employees.Find(ctx, keyToFind))
                {
                    var items = employees.GetValues(ctx, keyToFind);
                    Console.WriteLine($"Found Exact: {items[0].Key} -> {items[0].Value}");
                }

                trans.Commit();
            }
            catch
            {
                trans.Rollback();
                throw;
            }
        }

        using (var trans2 = db.BeginTransaction(ctx, TransactionMode.ForReading))
        {
            try
            {
                // Simplified Lookup (Anonymous Type)
                // You don't need the original class to search!
                Console.WriteLine("Searching with Anonymous Type...");
                var simpleEmployees = db.OpenBtree<object, string>(ctx, "employees", trans2);
                var anonKey = new { Region = "EU", Department = "Sales", Id = 301 };
                
                if (simpleEmployees.Find(ctx, anonKey))
                {
                    var items = simpleEmployees.GetValues(ctx, anonKey);
                    Console.WriteLine($"Found Anonymous: {items[0].Value}");
                }
                trans2.Commit();
            }
            catch
            {
                trans2.Rollback();
                throw;
            }
        }
    }
}
