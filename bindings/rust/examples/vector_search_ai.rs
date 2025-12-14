use sop::{Context, Database, DatabaseOptions, VectorItem, VectorQueryOptions, DatabaseType, L2CacheType};
use std::collections::HashMap;
use serde_json::json;

fn main() {
    println!("\n--- Running Vector Search (AI/RAG Example) ---");
    println!("Scenario: Semantic Product Search");

    let ctx = Context::new();
    if let Some(err) = ctx.error() {
        eprintln!("Error creating context: {}", err);
        return;
    }

    let mut options = DatabaseOptions::default();
    options.stores_folders = Some(vec!["sop_data_vector".to_string()]);
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

    // 1. Open Vector Store
    let vector_store = match db.open_vector_store(&ctx, "products_vectors", &trans) {
        Ok(vs) => vs,
        Err(e) => {
            eprintln!("Error opening vector store: {}", e);
            return;
        }
    };

    // 2. Upsert Embeddings
    println!("Indexing products with embeddings...");

    let mut payload1 = HashMap::new();
    payload1.insert("name".to_string(), json!("Gaming Laptop"));
    payload1.insert("price".to_string(), json!(1500));

    let item1 = VectorItem {
        id: "prod_1".to_string(),
        vector: vec![0.9, 0.1, 0.0],
        payload: payload1,
    };

    if let Err(e) = vector_store.upsert(&ctx, item1) {
        eprintln!("Error upserting item 1: {}", e);
    }

    let mut payload2 = HashMap::new();
    payload2.insert("name".to_string(), json!("Wireless Mouse"));
    payload2.insert("price".to_string(), json!(50));

    let item2 = VectorItem {
        id: "prod_2".to_string(),
        vector: vec![0.85, 0.15, 0.0],
        payload: payload2,
    };

    if let Err(e) = vector_store.upsert(&ctx, item2) {
        eprintln!("Error upserting item 2: {}", e);
    }

    trans.commit(&ctx).unwrap();
    println!("Indexing complete.");

    // 3. Search
    let trans2 = db.begin_transaction(&ctx).unwrap();
    
    println!("Searching for 'electronic devices' (simulated vector [0.9, 0.0, 0.0])...");
    
    let query = VectorQueryOptions {
        vector: vec![0.9, 0.0, 0.0],
        k: 2,
        filter: None,
    };

    match vector_store.search(&ctx, query) {
        Ok(results) => {
            for res in results {
                println!("Found: {} (Score: {:.4}) - Payload: {:?}", res.id, res.score, res.payload);
            }
        },
        Err(e) => eprintln!("Error searching: {}", e),
    }

    trans2.commit(&ctx).unwrap();
}
