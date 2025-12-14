use sop::{Context, Database, DatabaseOptions, VectorItem, VectorQueryOptions};
use std::fs;
use std::path::Path;
use std::collections::HashMap;

fn main() {
    println!("--- Vector Store Demo ---");

    let ctx = Context::new();
    let db_path = "data/vector_demo_db";
    if Path::new(db_path).exists() {
        fs::remove_dir_all(db_path).unwrap();
    }

    let db = Database::new(&ctx, DatabaseOptions {
        stores_folders: Some(vec![db_path.to_string()]),
        ..Default::default()
    }).unwrap();

    let trans = db.begin_transaction(&ctx).unwrap();
    
    // OpenVectorStore creates a new store if it doesn't exist
    let vector_store = db.open_vector_store(&ctx, "my_vectors", &trans).unwrap();

    // 1. Upsert Vectors
    println!("Upserting vectors...");
    
    let v1 = VectorItem {
        id: "vec1".to_string(),
        vector: vec![0.1, 0.2, 0.3],
        payload: HashMap::new(),
    };
    vector_store.upsert(&ctx, v1).unwrap();

    let v2 = VectorItem {
        id: "vec2".to_string(),
        vector: vec![0.9, 0.8, 0.7],
        payload: HashMap::new(),
    };
    vector_store.upsert(&ctx, v2).unwrap();

    trans.commit(&ctx).unwrap();
    println!("Committed.");

    // 2. Search
    println!("Searching...");
    let trans = db.begin_transaction(&ctx).unwrap();
    let vector_store = db.open_vector_store(&ctx, "my_vectors", &trans).unwrap();

    let query = VectorQueryOptions {
        vector: vec![0.1, 0.2, 0.35], // Close to vec1
        k: 2,
        filter: None,
    };

    match vector_store.search(&ctx, query) {
        Ok(results) => {
            for result in results {
                println!("Found: {} (Score: {:.4})", result.id, result.score);
            }
        },
        Err(e) => println!("Error searching: {}", e),
    }

    trans.commit(&ctx).unwrap();
    
    println!("--- End of Vector Store Demo ---");
}
