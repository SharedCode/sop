use sop::{Context, Database, DatabaseOptions};
use std::fs;
use std::path::Path;
use std::str;

fn main() {
    println!("\n--- Running Model Store Example ---");
    println!("Scenario: Storing Large AI Models (BLOBs)");

    let ctx = Context::new();
    let db_path = "sop_data_model";
    if Path::new(db_path).exists() {
        fs::remove_dir_all(db_path).unwrap();
    }

    let db = Database::new(&ctx, DatabaseOptions {
        stores_folders: Some(vec![db_path.to_string()]),
        ..Default::default()
    }).unwrap();

    let trans = db.begin_transaction(&ctx).unwrap();
    
    // OpenModelStore creates a new store if it doesn't exist
    let model_store = db.open_model_store(&ctx, "llm_weights", &trans).unwrap();

    // 1. Save a "Model" (simulated large binary data)
    let category = "llm";
    let model_name = "gpt-mini-v1";
    let weights = "...simulated large binary model weights...".as_bytes().to_vec();
    
    println!("Saving model '{}' in category '{}' ({} bytes)...", model_name, category, weights.len());
    model_store.save(&ctx, category, model_name, weights.clone()).unwrap();

    // 2. Load the Model
    println!("Loading model back...");
    match model_store.load(&ctx, category, model_name) {
        Ok(loaded_weights) => {
            let content = str::from_utf8(&loaded_weights).unwrap();
            println!("Loaded content: {}", content);
        },
        Err(e) => println!("Error loading model: {}", e),
    }

    // 3. Delete
    println!("Deleting model...");
    model_store.delete(&ctx, category, model_name).unwrap();

    trans.commit(&ctx).unwrap();
    
    println!("--- End of Model Store Demo ---");
}
