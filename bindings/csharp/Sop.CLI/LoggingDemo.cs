using System;
using System.IO;
using System.Collections.Generic;

namespace Sop.CLI
{
    public static class LoggingDemo
    {
        public static void Run()
        {
            Console.WriteLine("--- Logging Demo ---");

            // 1. Configure Logging
            string logFile = "sop_demo.log";
            if (File.Exists(logFile)) File.Delete(logFile);

            Console.WriteLine($"Configuring logger to write to {logFile}...");
            Logger.Configure(LogLevel.Debug, logFile);

            // 2. Initialize Context & Database
            using var ctx = new Context();
            string dbPath = "data/logging_demo_db";
            if (Directory.Exists(dbPath)) Directory.Delete(dbPath, true);

            Console.WriteLine($"Opening database at {dbPath}...");
            var db = new Database(new DatabaseOptions
            {
                StoresFolders = new List<string> { dbPath },
                Type = (int)DatabaseType.Standalone
            });

            // 3. Perform Operations
            Console.WriteLine("Starting transaction...");
            using (var trans = db.BeginTransaction(ctx))
            {
                Console.WriteLine("Creating B-Tree...");
                var btree = db.NewBtree<string, string>(ctx, "logging_btree", trans);

                Console.WriteLine("Adding item...");
                btree.Add(ctx, new Item<string, string> { Key = "hello", Value = "world" });

                Console.WriteLine("Committing transaction...");
                trans.Commit();
            }

            // 4. Verify Logs
            if (File.Exists(logFile))
            {
                Console.WriteLine($"\nSuccess! Log file created at {logFile}.");
                Console.WriteLine("First 5 lines of log:");
                var lines = File.ReadAllLines(logFile);
                for (int i = 0; i < Math.Min(5, lines.Length); i++)
                {
                    Console.WriteLine(lines[i]);
                }
            }
            else
            {
                Console.WriteLine("Error: Log file was not created.");
            }
            
            Console.WriteLine("--- End of Logging Demo ---");
        }
    }
}
