use sop::{Context, Database, DatabaseOptions, Item, DatabaseType};
use std::fs;
use std::path::Path;

fn main() {
    println!("--- Batched B-Tree Operations Demo ---");

    let ctx = Context::new();
    let db_path = "data/batched_demo_db";
    if Path::new(db_path).exists() {
        fs::remove_dir_all(db_path).unwrap();
    }

    let db = Database::new(&ctx, DatabaseOptions {
        stores_folders: Some(vec![db_path.to_string()]),
        db_type: DatabaseType::Standalone,
        ..Default::default()
    }).unwrap();

    // 1. Batched Add
    println!("\n1. Batched Add (100 items)...");
    let trans = db.begin_transaction(&ctx).unwrap();
    let btree = db.new_btree::<String, String>(&ctx, "batched_btree", &trans, None).unwrap();
    
    let mut items = Vec::new();
    for i in 0..100 {
        items.push(Item::new(format!("key_{}", i), format!("value_{}", i)));
    }

    btree.add_batch(&ctx, items).unwrap();
    trans.commit(&ctx).unwrap();
    println!("Committed.");

    // 2. Batched Update
    println!("\n2. Batched Update (100 items)...");
    let trans = db.begin_transaction(&ctx).unwrap();
    let btree = db.open_btree::<String, String>(&ctx, "batched_btree", &trans, None).unwrap();
    
    let mut items = Vec::new();
    for i in 0..100 {
        items.push(Item::new(format!("key_{}", i), format!("updated_value_{}", i)));
    }

    btree.update_batch(&ctx, items).unwrap();
    trans.commit(&ctx).unwrap();
    println!("Committed.");

    // Verify Update
    let trans = db.begin_transaction(&ctx).unwrap();
    let btree = db.open_btree::<String, String>(&ctx, "batched_btree", &trans, None).unwrap();
    if let Some(item) = btree.get_value(&ctx, "key_50".to_string()).unwrap() {
        if let Some(val) = &item.value {
            println!("Verified key_50 value: {}", val);
        }
    }
    trans.commit(&ctx).unwrap();

    // 3. Batched Remove
    println!("\n3. Batched Remove (100 items)...");
    let trans = db.begin_transaction(&ctx).unwrap();
    let btree = db.open_btree::<String, String>(&ctx, "batched_btree", &trans, None).unwrap();
    
    let mut keys = Vec::new();
    for i in 0..100 {
        keys.push(format!("key_{}", i));
    }

    btree.remove_batch(&ctx, keys).unwrap();
    trans.commit(&ctx).unwrap();
    println!("Committed.");

    // Verify Remove
    // Note: Count() is not yet implemented in Rust binding? 
    // Let's check if find returns false.
    let trans = db.begin_transaction(&ctx).unwrap();
    let btree = db.open_btree::<String, String>(&ctx, "batched_btree", &trans, None).unwrap();
    let found = btree.find(&ctx, "key_50".to_string()).unwrap();
    println!("Verified key_50 found: {}", found);
    trans.commit(&ctx).unwrap();

    println!("--- End of Batched Demo ---");
}
