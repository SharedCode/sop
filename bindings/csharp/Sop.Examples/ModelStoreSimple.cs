using System;
using System.Collections.Generic;
using System.Text;
using Sop;

namespace Sop.Examples;

public static class ModelStoreSimple
{
    public static void Run()
    {
        Console.WriteLine("\n--- Running Model Store Example ---");
        Console.WriteLine("Scenario: Storing Large AI Models (BLOBs)");

        using var ctx = new Context();
        var dbOpts = new DatabaseOptions 
        { 
            StoresFolders = new List<string> { "sop_data_model" },
            Type = (int)DatabaseType.Standalone
        };
        var db = new Database(dbOpts);

        using var trans = db.BeginTransaction(ctx);
        try
        {
            var modelStore = db.OpenModelStore(ctx, "llm_weights", trans);

            // 1. Save a "Model" (simulated large binary data)
            string category = "llm";
            string modelName = "gpt-mini-v1";
            byte[] weights = Encoding.UTF8.GetBytes("...simulated large binary model weights...");
            
            Console.WriteLine($"Saving model '{modelName}' in category '{category}' ({weights.Length} bytes)...");
            modelStore.Save(category, modelName, weights);

            // 2. Load the Model
            Console.WriteLine("Loading model back...");
            var loadedWeights = modelStore.Load<byte[]>(category, modelName);
            
            if (loadedWeights != null)
            {
                string content = Encoding.UTF8.GetString(loadedWeights);
                Console.WriteLine($"Loaded content: {content}");
            }
            else
            {
                Console.WriteLine("Model not found!");
            }

            // 3. Delete
            Console.WriteLine("Deleting model...");
            modelStore.Delete(category, modelName);

            trans.Commit();
        }
        catch
        {
            trans.Rollback();
            throw;
        }
    }
}
