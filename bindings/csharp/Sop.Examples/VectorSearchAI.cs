using System;
using System.Collections.Generic;
using Sop;

namespace Sop.Examples;

public static class VectorSearchAI
{
    public static void Run()
    {
        Console.WriteLine("\n--- Running Vector Search (AI/RAG Example) ---");
        Console.WriteLine("Scenario: Semantic Product Search");

        using var ctx = new Context();
        var dbOpts = new DatabaseOptions 
        { 
            StoresFolders = new List<string> { "sop_data_vector" },
            Type = (int)DatabaseType.Standalone
        };
        var db = new Database(dbOpts);

        using var trans = db.BeginTransaction(ctx);
        try
        {
            // 1. Open Vector Store
            var vectorStore = db.OpenVectorStore(ctx, "products_vectors", trans);

            // 2. Upsert Embeddings
            // In a real app, these float[] vectors would come from OpenAI/HuggingFace embeddings of the description.
            // Here we simulate:
            // [1, 0, 0] -> Electronics/Computers
            // [0, 1, 0] -> Clothing/Fashion
            // [0, 0, 1] -> Home/Garden

            Console.WriteLine("Indexing products with embeddings...");

            vectorStore.Upsert(new VectorItem 
            { 
                Id = "prod_1", 
                Vector = new float[] { 0.9f, 0.1f, 0.0f }, // Highly "Electronic"
                Payload = new Dictionary<string, object> { { "name", "Gaming Laptop" }, { "price", 1500 } }
            });

            vectorStore.Upsert(new VectorItem 
            { 
                Id = "prod_2", 
                Vector = new float[] { 0.85f, 0.15f, 0.0f }, // Mostly "Electronic"
                Payload = new Dictionary<string, object> { { "name", "Wireless Mouse" }, { "price", 50 } }
            });

            vectorStore.Upsert(new VectorItem 
            { 
                Id = "prod_3", 
                Vector = new float[] { 0.1f, 0.9f, 0.0f }, // Highly "Fashion"
                Payload = new Dictionary<string, object> { { "name", "Leather Jacket" }, { "price", 300 } }
            });

            // 3. Query (Semantic Search)
            // User searches for "computer accessories" -> Embedding might look like [0.88, 0.12, 0.0]
            Console.WriteLine("\nQuerying for 'computer accessories' (simulated vector [0.88, 0.12, 0.0])...");
            
            var queryVector = new float[] { 0.88f, 0.12f, 0.0f };
            var results = vectorStore.Query(queryVector, k: 2);

            foreach (var hit in results)
            {
                // The payload is retrieved automatically!
                var name = hit.Payload["name"];
                var price = hit.Payload["price"];
                Console.WriteLine($"Match: {name} () - Score: {hit.Score:F4}");
            }

            trans.Commit();
        }
        catch
        {
            trans.Rollback();
            throw;
        }
        finally
        {
            // Cleanup
            Console.WriteLine("Cleaning up vector store...");
            try 
            {
                db.RemoveVectorStore(ctx, "products_vectors");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Cleanup failed: {ex.Message}");
            }
        }
    }
}
