use sop::{Context, Database, DatabaseOptions, Item, L2CacheType, DatabaseType};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() {
    println!("--- Concurrent Transactions Demo (Standalone) ---");
    println!("Demonstrating multi-threaded access without client-side locks.");
    
    // 1. Initialize Context
    // We wrap it in Arc to share across threads.
    let ctx = Arc::new(Context::new());
    if let Some(err) = ctx.error() {
        eprintln!("Error creating context: {}", err);
        return;
    }

    // 2. Configure Database
    let mut options = DatabaseOptions::default();
    options.stores_folders = Some(vec!["sop_data_concurrent".to_string()]);
    options.db_type = DatabaseType::Standalone;
    options.cache_type = L2CacheType::InMemory;
    
    let db = match Database::new(&ctx, options) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Error creating database: {}", e);
            return;
        }
    };
    println!("Database created with ID: {}", db.id);

    // 3. Seed the B-Tree
    // IMPORTANT: Pre-seed the B-Tree with one item to establish the root node.
    {
        let trans = db.begin_transaction(&ctx).unwrap();
        let btree = db.new_btree::<i64, String>(&ctx, "concurrent_tree", &trans, None).unwrap();
        btree.add(&ctx, -1, "Root Seed Item".to_string()).unwrap();
        trans.commit(&ctx).unwrap();
        println!("Root seed item added.");
    }

    // 4. Launch Parallel Tasks
    let thread_count = 10; // Reduced from 20 for quicker demo
    let items_per_thread = 100;
    let mut handles = vec![];

    // We need to share the database handle as well
    let db = Arc::new(db);

    for i in 0..thread_count {
        let ctx_clone = Arc::clone(&ctx);
        let db_clone = Arc::clone(&db);
        let thread_id = i;

        let handle = thread::spawn(move || {
            let mut retry_count = 0;
            let mut committed = false;
            
            while !committed && retry_count < 10 {
                if retry_count > 0 {
                    println!("Thread {} retrying ({}/10)...", thread_id, retry_count);
                    thread::sleep(Duration::from_millis(100));
                }

                // Start Transaction
                let trans = match db_clone.begin_transaction(&ctx_clone) {
                    Ok(t) => t,
                    Err(e) => {
                        eprintln!("Thread {} failed to begin transaction: {}", thread_id, e);
                        retry_count += 1;
                        continue;
                    }
                };

                // Open Btree
                // Note: In a real app, you might cache the btree handle, but here we open it per transaction
                // to simulate independent clients.
                let btree = match db_clone.open_btree::<i64, String>(&ctx_clone, "concurrent_tree", &trans, None) {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!("Thread {} failed to open btree: {}", thread_id, e);
                        // If we can't open the btree, maybe we should rollback?
                        // But trans is local.
                        let _ = trans.rollback(&ctx_clone);
                        retry_count += 1;
                        continue;
                    }
                };

                // Add Items
                let mut batch = Vec::with_capacity(items_per_thread);
                for j in 0..items_per_thread {
                    let key = (thread_id as i64 * items_per_thread as i64) + j as i64;
                    let value = format!("Thread {} - Item {}", thread_id, j);
                    batch.push(Item::new(key, value));
                }

                // We use upsert instead of update to handle potential re-runs if needed, 
                // and to allow adding new items.
                match btree.upsert_batch(&ctx_clone, batch) {
                    Ok(_) => {},
                    Err(e) => {
                        eprintln!("Thread {} failed to add batch: {}", thread_id, e);
                        let _ = trans.rollback(&ctx_clone);
                        retry_count += 1;
                        continue;
                    }
                }

                // Commit
                match trans.commit(&ctx_clone) {
                    Ok(_) => {
                        committed = true;
                        println!("Thread {} committed successfully.", thread_id);
                    },
                    Err(e) => {
                        eprintln!("Thread {} failed to commit: {}", thread_id, e);
                        // Transaction is likely already failed on server side, but we can try rollback locally
                        let _ = trans.rollback(&ctx_clone);
                        retry_count += 1;
                    }
                }
            }
            
            if !committed {
                eprintln!("Thread {} failed after 10 retries.", thread_id);
            }
        });
        handles.push(handle);

        // Add sleep jitter to stagger thread starts (20-200ms)
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
        let millis = 20 + (nanos % 381);
        thread::sleep(Duration::from_millis(millis as u64));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("All threads finished.");
    
    // Verify count
    let trans = db.begin_transaction(&ctx).unwrap();
    // We need to open the btree again to get a handle bound to this transaction?
    // Actually, the btree handle holds the ID. But the operations take the context.
    // Wait, the btree operations don't take the transaction explicitly in the Rust API?
    // Let's check lib.rs.
    // btree.add(&ctx, item) -> manageBtree(ctx.id, ...)
    // The transaction is associated with the Btree when it was created/opened?
    // In Go: manageDatabase(NewBtree) -> returns Btree ID.
    // The Btree object in Go is associated with the Transaction it was created with.
    // So yes, we need a new Btree handle for the new transaction.
    
    let btree = db.open_btree::<i64, String>(&ctx, "concurrent_tree", &trans, None).unwrap();
    
    match btree.count() {
        Ok(count) => println!("Total items in B-Tree: {}", count),
        Err(e) => eprintln!("Error getting count: {}", e),
    }
    
    let mut success_count = 0;
    for i in 0..thread_count {
        let key = i as i64 * items_per_thread as i64;
        match btree.find(&ctx, key) {
            Ok(found) => {
                if found {
                    success_count += 1;
                } else {
                    eprintln!("Verification failed: Key {} not found.", key);
                }
            },
            Err(e) => eprintln!("Verification error for key {}: {}", key, e),
        }
    }
    
    trans.commit(&ctx).unwrap();

    db.remove_btree(&ctx, "concurrent_tree").unwrap();
    
    println!("Verification: Found representative keys for {}/{} threads.", success_count, thread_count);
}
