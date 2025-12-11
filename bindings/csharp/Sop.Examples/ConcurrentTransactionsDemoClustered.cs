using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.IO;

namespace Sop.Examples
{
    public static class ConcurrentTransactionsDemo
    {
        public static void Run()
        {
            // Enable verbose logging to stderr for debugging
            Logger.Configure(LogLevel.Debug, "");

            // Initialize Redis for Clustered mode
            try 
            {
                Redis.Initialize("redis://localhost:6379");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to connect to Redis: {ex.Message}");
                return;
            }

            Console.WriteLine("--- Concurrent Transactions Demo (Clustered) ---");
            Console.WriteLine("Demonstrating multi-threaded access without client-side locks.");
            Console.WriteLine("SOP handles ACID transactions, conflict detection, and merging.");
            Console.WriteLine("This runs in Clustered mode (Redis required).");

            string dbPath = "data/concurrent_demo_clustered";

            using var ctx = new Context();
            var db = new Database(new DatabaseOptions
            {
                StoresFolders = new List<string> { dbPath },
                Type = (int)DatabaseType.Clustered
            });

            // 1. Setup: Create the B-Tree in a separate transaction first
            // IMPORTANT: Pre-seed the B-Tree with one item to establish the root node.
            // This prevents race conditions on the very first commit when multiple threads 
            // try to initialize an empty tree simultaneously.
            using (var trans = db.BeginTransaction(ctx))
            {
                var btree = db.NewBtree<int, string>(ctx, "concurrent_tree", trans);
                btree.Add(ctx, new Item<int, string> { Key = -1, Value = "Root Seed Item" });
                trans.Commit();
            }

            Console.WriteLine("Launching parallel tasks...");
            
            int threadCount = 20;
            int itemsPerThread = 200;
            var threads = new List<Thread>();

            // We use a barrier to start threads roughly at the same time
            using var barrier = new Barrier(threadCount);

            for (int i = 0; i < threadCount; i++)
            {
                int threadId = i;
                var thread = new Thread(() => 
                {
                    // Use the shared Context (ctx) like Python does, instead of creating one per thread.
                    // SOP Context is thread-safe for ID retrieval.
                    
                    barrier.SignalAndWait();
                    
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
                            var btree = db.OpenBtree<int, string>(ctx, "concurrent_tree", trans);

                            for (int j = 0; j < itemsPerThread; j++)
                            {
                                // Unique keys per thread ensures no conflicts -> "Data Merge"
                                // If keys overlapped, SOP would detect conflicts and the commit might fail 
                                // (requiring a retry loop in a real app).
                                int key = (threadId * itemsPerThread) + j;
                                Console.WriteLine($"Thread {threadId} adding key {key}...");
                                btree.Add(ctx, new Item<int, string> { Key = key, Value = $"Thread {threadId} - Item {j}" });
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
            }

            foreach (var t in threads) t.Join();

            // Verify
            using (var trans = db.BeginTransaction(ctx, TransactionMode.ForReading))
            {
                var btree = db.OpenBtree<int, string>(ctx, "concurrent_tree", trans);
                
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
            
            db.RemoveBtree(ctx, dbPath);
            Redis.Close();
        }
    }
}
