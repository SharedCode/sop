using System;
using System.Collections.Generic;
using System.IO;

namespace Sop.Examples
{
    public static class TextSearchDemo
    {
        public static void Run()
        {
            Console.WriteLine("--- Text Search Demo ---");

            using var ctx = new Context();
            string dbPath = "data/text_search_demo_db";
            if (Directory.Exists(dbPath)) Directory.Delete(dbPath, true);

            var db = new Database(new DatabaseOptions
            {
                StoresFolders = new List<string> { dbPath },
                Type = (int)DatabaseType.Standalone
            });

            // 1. Add Documents
            Console.WriteLine("\n1. Adding documents...");
            using (var trans = db.BeginTransaction(ctx))
            {
                // OpenSearch creates a new store if it doesn't exist
                var search = db.OpenSearch(ctx, "my_text_index", trans);
                
                search.Add("doc1", "The quick brown fox jumps over the lazy dog");
                search.Add("doc2", "SOP is a high performance database");
                search.Add("doc3", "Text search is useful for finding information");
                search.Add("doc4", "The fox is quick and brown");

                trans.Commit();
            }
            Console.WriteLine("Committed.");

            // 2. Search
            Console.WriteLine("\n2. Searching...");
            using (var trans = db.BeginTransaction(ctx))
            {
                var search = db.OpenSearch(ctx, "my_text_index", trans);
                
                PerformSearch(search, "fox");
                PerformSearch(search, "database");
                PerformSearch(search, "quick brown");
                PerformSearch(search, "information");
                PerformSearch(search, "missing");

                trans.Commit();
            }

            Console.WriteLine("--- End of Text Search Demo ---");
        }

        private static void PerformSearch(Search search, string query)
        {
            Console.WriteLine($"\nQuery: '{query}'");
            var results = search.SearchQuery(query);
            
            if (results.Count == 0)
            {
                Console.WriteLine("  No results found.");
            }
            else
            {
                foreach (var result in results)
                {
                    Console.WriteLine($"  DocID: {result.DocID}, Score: {result.Score:F4}");
                }
            }
        }
    }
}
