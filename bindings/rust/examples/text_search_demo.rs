use sop::{Context, Database, DatabaseOptions};
use std::fs;
use std::path::Path;

fn main() {
    println!("--- Text Search Demo ---");

    let ctx = Context::new();
    let db_path = "data/text_search_demo_db";
    if Path::new(db_path).exists() {
        fs::remove_dir_all(db_path).unwrap();
    }

    let db = Database::new(&ctx, DatabaseOptions {
        stores_folders: Some(vec![db_path.to_string()]),
        ..Default::default()
    }).unwrap();

    // 1. Add Documents
    println!("\n1. Adding documents...");
    {
        let trans = db.begin_transaction(&ctx).unwrap();
        // OpenSearch creates a new store if it doesn't exist
        let search = db.open_search(&ctx, "my_text_index", &trans).unwrap();
        
        search.add(&ctx, "doc1", "The quick brown fox jumps over the lazy dog").unwrap();
        search.add(&ctx, "doc2", "SOP is a high performance database").unwrap();
        search.add(&ctx, "doc3", "Text search is useful for finding information").unwrap();
        search.add(&ctx, "doc4", "The fox is quick and brown").unwrap();

        trans.commit(&ctx).unwrap();
    }
    println!("Committed.");

    // 2. Search
    println!("\n2. Searching...");
    {
        let trans = db.begin_transaction(&ctx).unwrap();
        let search = db.open_search(&ctx, "my_text_index", &trans).unwrap();
        
        perform_search(&ctx, &search, "fox");
        perform_search(&ctx, &search, "database");
        perform_search(&ctx, &search, "quick brown");
        perform_search(&ctx, &search, "information");
        perform_search(&ctx, &search, "missing");

        trans.commit(&ctx).unwrap();
    }

    println!("--- End of Text Search Demo ---");
}

fn perform_search(ctx: &Context, search: &sop::Search, query: &str) {
    println!("\nQuery: '{}'", query);
    match search.search(ctx, query) {
        Ok(results) => {
            if results.is_empty() {
                println!("  No results found.");
            } else {
                for result in results {
                    println!("  DocID: {}, Score: {:.4}", result.doc_id, result.score);
                }
            }
        },
        Err(e) => println!("  Error searching: {}", e),
    }
}
