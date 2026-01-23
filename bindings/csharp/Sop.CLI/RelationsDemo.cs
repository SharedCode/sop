using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Sop.CLI;

public class User
{
    [JsonPropertyName("id")]
    public string Id { get; set; }
    [JsonPropertyName("name")]
    public string Name { get; set; }
}

public class Order
{
    [JsonPropertyName("id")]
    public string Id { get; set; }
    [JsonPropertyName("user_id")]
    public string UserId { get; set; }
    [JsonPropertyName("amount")]
    public decimal Amount { get; set; }
}

public class RelationsDemo
{
    public static void Run()
    {
        Console.WriteLine("Running Relations Demo...");
        // 1. Setup
        var ctx = new Context();
        var opts = new DatabaseOptions
        {
            StoresFolders = new List<string> { "data" }
        };
        
        // Ensure directory exists
        System.IO.Directory.CreateDirectory("data");
        
        var db = new Database(opts);

        using (var trans = db.BeginTransaction(ctx))
        {
            // 2. Create 'Users' Store (Target)
            Console.WriteLine("Creating 'users' store...");
            // Use strongly-typed User class
            var users = db.NewBtree<string, User>(ctx, "users", trans);
            users.Add(ctx, "user_1", new User { Id = "user_1", Name = "Alice" });

            // 3. Create 'Orders' Store with Relation Metadata (Source)
            Console.WriteLine("Creating 'orders' store with Relation metadata...");
            
            var rel = new Relation(
                new List<string> { "user_id" }, 
                "users", 
                new List<string> { "id" }
            );

            var options = new BtreeOptions("orders")
            {
                Relations = new List<Relation> { rel }
            };

            // Use strongly-typed Order class
            var orders = db.NewBtree<string, Order>(ctx, "orders", trans, options);
            
            // Add a dummy order using strongly-typed object
            orders.Add(ctx, "order_A", new Order { Id = "order_A", UserId = "user_1", Amount = 100 });

            trans.Commit();
            Console.WriteLine("Successfully created stores with Relations!");
        }
    }
}
