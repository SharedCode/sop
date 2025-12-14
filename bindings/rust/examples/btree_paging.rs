use sop::{Context, Database, DatabaseOptions, L2CacheType, DatabaseType, PagingInfo};

fn main() {
    println!("--- Running B-Tree Paging & Navigation ---");

    let ctx = Context::new();
    if let Some(err) = ctx.error() {
        eprintln!("Error creating context: {}", err);
        return;
    }

    let mut options = DatabaseOptions::default();
    options.stores_folders = Some(vec!["sop_data_paging".to_string()]);
    options.db_type = DatabaseType::Standalone;
    options.cache_type = L2CacheType::InMemory;

    let db = match Database::new(&ctx, options) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Error creating database: {}", e);
            return;
        }
    };

    let trans = db.begin_transaction(&ctx).unwrap();
    
    // 1. Create Btree
    let btree = match db.new_btree::<i32, String>(&ctx, "logs", &trans, None) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("Error creating btree: {}", e);
            return;
        }
    };

    // 2. Populate with 100 items
    println!("Populating 100 log entries...");
    for i in 0..100 {
        if let Err(e) = btree.add(&ctx, i, format!("Log Entry {}", i)) {
            eprintln!("Error adding item {}: {}", i, e);
        }
    }

    // 3. Page 1: First 10 items
    println!("\n--- Page 1 (Items 0-9) ---");
    
    let paging_info = PagingInfo {
        page_size: 10,
        page_offset: 0,
    };

    match btree.get_keys(&ctx, Some(paging_info)) {
        Ok(keys) => {
            for key in keys {
                print!("{} ", key);
            }
            println!();
        },
        Err(e) => eprintln!("Error getting page 1: {}", e),
    }

    // 4. Page 2: Next 10 items
    println!("\n--- Page 2 (Items 10-19) ---");
    
    // Move cursor to 10 explicitly for demo
    if let Err(e) = btree.find(&ctx, 10) {
        eprintln!("Error finding key 10: {}", e);
    }

    let paging_info2 = PagingInfo {
        page_size: 10,
        page_offset: 0,
    };

    match btree.get_keys(&ctx, Some(paging_info2)) {
        Ok(keys) => {
            for key in keys {
                print!("{} ", key);
            }
            println!();
        },
        Err(e) => eprintln!("Error getting page 2: {}", e),
    }

    // 5. Navigation: First/Last
    println!("\n--- Navigation ---");
    
    if let Ok(true) = btree.first(&ctx) {
        if let Ok(Some(item)) = btree.current_key(&ctx) {
            println!("First Key: {}", item.key);
        }
    }

    if let Ok(true) = btree.last(&ctx) {
        if let Ok(Some(item)) = btree.current_key(&ctx) {
            println!("Last Key: {}", item.key);
        }
    }

    trans.commit(&ctx).unwrap();
    println!("Finished.");
}
