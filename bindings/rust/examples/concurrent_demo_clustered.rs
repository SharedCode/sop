use sop::{Context, Database, DatabaseOptions, Item, L2CacheType, DatabaseType, RedisConfig};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() {
    println!("--- Concurrent Transactions Demo (Clustered) ---");
    println!("Demonstrating multi-threaded access without client-side locks.");
    println!("This runs in Clustered mode (Redis required).");

    // 1. Initialize Context
    let ctx = Arc::new(Context::new());
    if let Some(err) = ctx.error() {
        eprintln!("Error creating context: {}", err);
        return;
    }

    // 3. Configure Database
    let mut options = DatabaseOptions::default();
    let data_folder = "data/sop_data_concurrent_clustered";
    options.stores_folders = Some(vec![data_folder.to_string()]);
    options.db_type = DatabaseType::Clustered;
    options.redis_config = Some(RedisConfig {
        address: Some("localhost:6379".to_string()),
        password: None,
        db: 0,
        url: None,
    });
    // In clustered mode, we typically use Redis for L2 cache as well, or InMemory if preferred.
    // The C# example doesn't explicitly set cache type, so it defaults to InMemory or Redis depending on config.
    // Let's use Redis for cache too.
    options.cache_type = L2CacheType::Redis;
    
    let db = match Database::new(&ctx, options) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Error creating database: {}", e);
            return;
        }
    };
    println!("Database created with ID: {}", db.id);

    // Ensure clean slate by removing the btree if it exists from previous runs
    let _ = db.remove_btree(&ctx, "concurrent_tree");

    // 4. Seed the B-Tree
    {
        let trans = db.begin_transaction(&ctx).unwrap();
        let btree = db.new_btree::<i64, String>(&ctx, "concurrent_tree", &trans, None).unwrap();
        btree.add(&ctx, -1, "Root Seed Item".to_string()).unwrap();
        trans.commit(&ctx).unwrap();
        println!("Root seed item added.");
    }

    // 5. Launch Parallel Tasks
    let thread_count = 10; 
    let items_per_thread = 100;
    let mut handles = vec![];

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

                let trans = match db_clone.begin_transaction(&ctx_clone) {
                    Ok(t) => t,
                    Err(e) => {
                        eprintln!("Thread {} failed to begin transaction: {}", thread_id, e);
                        retry_count += 1;
                        continue;
                    }
                };

                let btree = match db_clone.open_btree::<i64, String>(&ctx_clone, "concurrent_tree", &trans, None) {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!("Thread {} failed to open btree: {}", thread_id, e);
                        let _ = trans.rollback(&ctx_clone);
                        retry_count += 1;
                        continue;
                    }
                };

                let mut batch = Vec::with_capacity(items_per_thread);
                for j in 0..items_per_thread {
                    let key = (thread_id as i64 * items_per_thread as i64) + j as i64;
                    let value = format!("Thread {} - Item {}", thread_id, j);
                    batch.push(Item::new(key, value));
                }

                match btree.upsert_batch(&ctx_clone, batch) {
                    Ok(_) => {},
                    Err(e) => {
                        eprintln!("Thread {} failed to add batch: {}", thread_id, e);
                        let _ = trans.rollback(&ctx_clone);
                        retry_count += 1;
                        continue;
                    }
                }

                match trans.commit(&ctx_clone) {
                    Ok(_) => {
                        committed = true;
                        println!("Thread {} committed successfully.", thread_id);
                    },
                    Err(e) => {
                        eprintln!("Thread {} failed to commit: {}", thread_id, e);
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
    let btree = db.open_btree::<i64, String>(&ctx, "concurrent_tree", &trans, None).unwrap();

    // Count each item on btree.
    let mut actual_count = 0;
    if btree.first(&ctx).unwrap() {
        actual_count += 1;
        while btree.next(&ctx).unwrap() {
            actual_count += 1;
        }
    }
    
    match btree.count() {
        Ok(count) => println!("Total items in B-Tree: {}", count),
        Err(e) => eprintln!("Error getting count: {}", e),
    }
    println!("Actual count: {}", actual_count);

    // Verify seed
    match btree.find(&ctx, -1) {
        Ok(found) => if !found { eprintln!("Seed item (-1) NOT found!"); } else { println!("Seed item found."); },
        Err(e) => eprintln!("Error finding seed: {}", e),
    }
    
    trans.commit(&ctx).unwrap();
    db.remove_btree(&ctx, "concurrent_tree").unwrap();
}
