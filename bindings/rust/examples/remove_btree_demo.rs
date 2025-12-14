use sop::{Context, Database, DatabaseOptions};
use std::fs;
use std::path::Path;

fn main() {
    println!("--- Remove B-Tree Demo ---");

    let ctx = Context::new();
    let db_path = "data/remove_btree_demo_db";
    if Path::new(db_path).exists() {
        fs::remove_dir_all(db_path).unwrap();
    }

    println!("Opening database at {}...", db_path);
    let db = Database::new(&ctx, DatabaseOptions {
        stores_folders: Some(vec![db_path.to_string()]),
        ..Default::default()
    }).unwrap();

    // 1. Create B-Tree and Add Data
    println!("1. Creating B-Tree 'temp_btree' and adding data...");
    {
        let trans = db.begin_transaction(&ctx).unwrap();
        let btree = db.new_btree::<String, String>(&ctx, "temp_btree", &trans, None).unwrap();
        btree.add(&ctx, "key1".to_string(), "value1".to_string()).unwrap();
        trans.commit(&ctx).unwrap();
    }
    println!("   Committed.");

    // 2. Verify B-Tree Exists
    println!("2. Verifying B-Tree exists...");
    {
        let trans = db.begin_transaction(&ctx).unwrap();
        let btree = db.open_btree::<String, String>(&ctx, "temp_btree", &trans, None).unwrap();
        if btree.find(&ctx, "key1".to_string()).unwrap() {
            println!("   Found 'key1' in 'temp_btree'.");
        } else {
            println!("   Error: 'key1' not found!");
        }
        trans.commit(&ctx).unwrap();
    }

    // 3. Remove B-Tree
    println!("3. Removing B-Tree 'temp_btree'...");
    db.remove_btree(&ctx, "temp_btree").unwrap();
    println!("   Committed removal.");

    // 4. Verify B-Tree is Gone (or Empty/Recreated)
    // Note: In SOP, opening a removed B-Tree might recreate it empty, or fail depending on implementation.
    // Let's check if it's empty.
    println!("4. Verifying B-Tree is removed (should be empty if reopened)...");
    {
        let trans = db.begin_transaction(&ctx).unwrap();
        let btree = db.open_btree::<String, String>(&ctx, "temp_btree", &trans, None).unwrap();
        if btree.find(&ctx, "key1".to_string()).unwrap() {
            println!("   Error: 'key1' still found!");
        } else {
            println!("   Success: 'key1' not found (B-Tree was removed/reset).");
        }
        trans.commit(&ctx).unwrap();
    }

    println!("--- End of Remove B-Tree Demo ---");
}
