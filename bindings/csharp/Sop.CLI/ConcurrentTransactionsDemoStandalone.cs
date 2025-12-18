using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.Security.Cryptography.X509Certificates;

namespace Sop.CLI
{
    public static class ConcurrentTransactionsDemoStandalone
    {
        public static void Run()
        {
            const string StoreName = "concurrent_tree";
            
            // Enable verbose logging to stderr for debugging
            Logger.Configure(LogLevel.Warn, "");

            Console.WriteLine("--- Concurrent Transactions Demo (Standalone) ---");
            Console.WriteLine("Demonstrating multi-threaded access without client-side locks.");
            Console.WriteLine("SOP handles ACID transactions, conflict detection, and merging.");
            Console.WriteLine("This runs in Standalone mode (no Redis required).");

            string dbPath = "data/concurrent_demo_standalone";
            if (Directory.Exists(dbPath)) Directory.Delete(dbPath, true);

            using var ctx = new Context();
            var db = new Database(new DatabaseOptions
            {
                StoresFolders = new List<string> { dbPath },
                Type = (int)DatabaseType.Standalone
            });

            // 1. Setup: Create the B-Tree in a separate transaction first
            // IMPORTANT: Pre-seed the B-Tree with one item to establish the root node.
            // This prevents race conditions on the very first commit when multiple threads 
            // try to initialize an empty tree simultaneously.
            // NOTE: This requirement is simply to have at least one item in the tree.
            // It can be a real application item or a dummy seed item.
            using (var trans = db.BeginTransaction(ctx))
            {
                var btree = db.NewBtree<int, string>(ctx, StoreName, trans);
                btree.Add(ctx, new Item<int, string> { Key = -1, Value = "Root Seed Item" });
                trans.Commit();
            }

            Console.WriteLine("Launching parallel tasks...");
            
            int threadCount = 20;
            int itemsPerThread = 200;
            var threads = new List<Thread>();

            var rnd = new Random();

            for (int i = 0; i < threadCount; i++)
            {
                int threadId = i;
                var thread = new Thread(() => 
                {
                    // Use the shared Context (ctx) like Python does, instead of creating one per thread.
                    // SOP Context is thread-safe for ID retrieval.
                    
                    // NOTE: No 'lock' statement used here.
                    // Each thread gets its own Transaction.
                    // SOP manages isolation.
                    // This allows concurrent threads to do transactions without lock mgmt.
                    
                    int retryCount = 0;
                    bool committed = false;
                    while (!committed && retryCount < 10)
                    {
                        try 
                        {
                            Console.WriteLine($"Thread {threadId} starting transaction...");
                            using var trans = db.BeginTransaction(ctx);
                            Console.WriteLine($"Thread {threadId} opening btree...");

                            List<Item<int, string>> batch = new List<Item<int, string>>(itemsPerThread);
                            var btree = db.OpenBtree<int, string>(ctx, StoreName, trans);
                            for (int j = 0; j < itemsPerThread; j++)
                            {
                                // Unique keys per thread ensures no conflicts -> "Data Merge"
                                // If keys overlapped, SOP would detect conflicts and the commit might fail 
                                // (requiring a retry loop in a real app).
                                int key = (threadId * itemsPerThread) + j;
                                Console.WriteLine($"Thread {threadId} adding key {key}...");
                                batch.Add(new Item<int, string> { Key = key, Value = $"Thread {threadId} - Item {j}" });
                            }
                            if (!btree.Add(ctx, batch))
                            {                                
                                Console.WriteLine($"Thread {threadId} failed to write batch");
                            }

                            Console.WriteLine($"Thread {threadId} committing...");
                            trans.Commit();
                            committed = true;
                            Console.WriteLine($"Thread {threadId} committed successfully.");
                        }
                        catch (Exception ex)
                        {
                            retryCount++;
                            // Random backoff to reduce contention
                            int delay = new Random().Next(100, 500) * retryCount;
                            Console.WriteLine($"Thread {threadId} conflict detected (Retry {retryCount}): {ex.Message}");
                            Thread.Sleep(delay);
                        }
                    }
                    
                    if (!committed)
                        Console.WriteLine($"Thread {threadId} failed after retries.");
                });
                threads.Add(thread);
                thread.Start();

                // Jitter sleep between threads
                int delay = rnd.Next(20, 500);
                Console.WriteLine($"Waiting {delay}ms before starting next thread...");
                Thread.Sleep(delay);
            }

            foreach (var t in threads) t.Join();

            // Verify
            using (var trans = db.BeginTransaction(ctx, TransactionMode.ForReading))
            {
                var btree = db.OpenBtree<int, string>(ctx, StoreName, trans);
                
                long count = 0;
                if (btree.First(ctx))
                {
                    do
                    {
                        count++;
                    } while (btree.Next(ctx));
                }

                // Expected count includes the seed item
                long expectedCount = (threadCount * itemsPerThread) + 1;
                Console.WriteLine($"Final Count: {count} (Expected: {expectedCount})");

                if (count == expectedCount)
                    Console.WriteLine("SUCCESS: All transactions merged correctly.");
                else
                    Console.WriteLine("FAILURE: Count mismatch.");
            }
            // Cleanup our mess.
            db.RemoveBtree(ctx, StoreName);
        }
    }
}
