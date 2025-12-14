use sop::{Context, Database, DatabaseOptions, LogLevel, manage_logging};
use std::fs;
use std::path::Path;

fn main() {
    println!("--- Logging Demo ---");

    // 1. Configure Logging
    let log_file = "sop_demo.log";
    if Path::new(log_file).exists() {
        fs::remove_file(log_file).unwrap();
    }

    println!("Configuring logger to write to {}...", log_file);
    if let Err(e) = manage_logging(LogLevel::Debug, log_file) {
        eprintln!("Failed to configure logging: {}", e);
        return;
    }

    // 2. Initialize Context & Database
    let ctx = Context::new();
    let db_path = "data/logging_demo_db";
    if Path::new(db_path).exists() {
        fs::remove_dir_all(db_path).unwrap();
    }

    println!("Opening database at {}...", db_path);
    let db = Database::new(&ctx, DatabaseOptions {
        stores_folders: Some(vec![db_path.to_string()]),
        ..Default::default()
    }).unwrap();

    // 3. Perform Operations
    println!("Starting transaction...");
    let trans = db.begin_transaction(&ctx).unwrap();
    
    println!("Creating B-Tree...");
    let btree = db.new_btree::<String, String>(&ctx, "logging_btree", &trans, None).unwrap();

    println!("Adding item...");
    btree.add(&ctx, "hello".to_string(), "world".to_string()).unwrap();

    println!("Committing transaction...");
    trans.commit(&ctx).unwrap();

    // 4. Verify Logs
    if Path::new(log_file).exists() {
        println!("\nSuccess! Log file created at {}.", log_file);
        println!("First 5 lines of log:");
        let content = fs::read_to_string(log_file).unwrap();
        for (i, line) in content.lines().enumerate() {
            if i >= 5 { break; }
            println!("{}", line);
        }
    } else {
        println!("Error: Log file was not created.");
    }
    
    println!("--- End of Logging Demo ---");
}
