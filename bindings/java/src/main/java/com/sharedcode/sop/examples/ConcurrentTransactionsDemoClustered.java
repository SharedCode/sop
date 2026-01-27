package com.sharedcode.sop.examples;

import com.sharedcode.sop.*;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ConcurrentTransactionsDemoClustered {

    public static void main(String[] args) {
        run();
    }

    public static void run() {
        final String storeName = "concurrent_tree";

        // Enable verbose logging to stderr for debugging
        try {
            Logger.configure(LogLevel.Warn, "");
        } catch (SopException e) {
            e.printStackTrace();
        }

        System.out.println("--- Concurrent Transactions Demo (Clustered) ---");
        System.out.println("Demonstrating multi-threaded access without client-side locks.");
        System.out.println("SOP handles ACID transactions, conflict detection, and merging.");
        System.out.println("This runs in Clustered mode (Redis required).");

        String dbPath = "sop_data_concurrent_clustered";
        // Clean up previous run
        deleteDirectory(new File(dbPath));

        try (Context ctx = new Context()) {
            DatabaseOptions dbOpts = new DatabaseOptions();
            dbOpts.stores_folders = Collections.singletonList(dbPath);
            dbOpts.type = DatabaseType.Clustered;
            dbOpts.redis_config = new DatabaseOptions.RedisConfig();
            dbOpts.redis_config.address = "localhost:6379";
            dbOpts.redis_config.db = 0;

            Database db = new Database(dbOpts);

            // 1. Setup: Create the B-Tree in a separate transaction first
            try (Transaction trans = db.beginTransaction(ctx)) {
                BTree<Integer, String> btree = db.newBtree(ctx, storeName, trans, null, Integer.class, String.class);
                btree.add(new Item<>( -1, "Root Seed Item"));
                trans.commit();
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            System.out.println("Launching parallel tasks...");

            int threadCount = 20;
            int itemsPerThread = 200;
            List<Thread> threads = new ArrayList<>();
            Random rnd = new Random();

            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                Thread thread = new Thread(() -> {
                    int retryCount = 0;
                    boolean committed = false;
                    while (!committed && retryCount < 2) {
                        try {
                            System.out.println("Thread " + threadId + " starting transaction...");
                            try (Transaction trans = db.beginTransaction(ctx)) {
                                System.out.println("Thread " + threadId + " opening btree...");

                                List<Item<Integer, String>> batch = new ArrayList<>(itemsPerThread);
                                BTree<Integer, String> btree = db.openBtree(ctx, storeName, trans, Integer.class, String.class);
                                
                                for (int j = 0; j < itemsPerThread; j++) {
                                    int key = (threadId * itemsPerThread) + j;
                                    batch.add(new Item<>(key, "Thread " + threadId + " - Item " + j));
                                }
                                
                                if (!btree.add(batch)) {
                                    System.out.println("Thread " + threadId + " failed to write batch");
                                }

                                System.out.println("Thread " + threadId + " committing...");
                                trans.commit();
                                committed = true;
                                System.out.println("Thread " + threadId + " committed successfully.");
                            }
                        } catch (Exception ex) {
                            retryCount++;
                            int delay = new Random().nextInt(400) + 100 * retryCount;
                            System.out.println("Thread " + threadId + " conflict detected (Retry " + retryCount + "): " + ex.getMessage());
                            try {
                                Thread.sleep(delay);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }

                    if (!committed) {
                        System.out.println("Thread " + threadId + " failed after retries.");
                    }
                });
                threads.add(thread);
                thread.start();

                try {
                    int delay = rnd.nextInt(480) + 20;
                    System.out.println("Waiting " + delay + "ms before starting next thread...");
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            for (Thread t : threads) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // Verify
            try (Transaction trans = db.beginTransaction(ctx, TransactionMode.ForReading)) {
                BTree<Integer, String> btree = BTree.open(ctx, storeName, trans, Integer.class, String.class);

                long count = 0;
                if (btree.first()) {
                    do {
                        count++;
                    } while (btree.next());
                }

                long expectedCount = (threadCount * itemsPerThread) + 1;
                System.out.println("Final Count: " + count + " (Expected: " + expectedCount + ")");

                if (count == expectedCount) {
                    System.out.println("SUCCESS: All transactions merged correctly.");
                } else {
                    System.out.println("FAILURE: Count mismatch.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            // Cleanup
            try {
                db.removeBtree(ctx, storeName);
                Redis.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void deleteDirectory(File file) {
        if (file.exists()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    deleteDirectory(f);
                }
            }
            file.delete();
        }
    }
}
