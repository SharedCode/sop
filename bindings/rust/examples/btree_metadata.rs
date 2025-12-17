use sop::{Context, Database, DatabaseOptions, DatabaseType, BtreeOptions, Btree, IndexSpecification, IndexFieldSpecification};
use serde::{Serialize, Deserialize};
use std::fs;
use std::path::Path;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ProductKey {
    #[serde(rename = "Category")]
    category: String,
    #[serde(rename = "ProductId")]
    product_id: i32,
    #[serde(rename = "IsActive")]
    is_active: bool,
    #[serde(rename = "Price")]
    price: f64,
}

impl ProductKey {
    fn new(category: &str, id: i32, active: bool, price: f64) -> Self {
        Self {
            category: category.to_string(),
            product_id: id,
            is_active: active,
            price,
        }
    }
}

fn main() {
    println!("\n--- Running Metadata 'Ride-on' Keys ---");

    let ctx = Context::new();
    let db_path = "data/metadata_demo_db";
    if Path::new(db_path).exists() {
        fs::remove_dir_all(db_path).unwrap();
    }

    let db = Database::new(&ctx, DatabaseOptions {
        stores_folders: Some(vec![db_path.to_string()]),
        db_type: DatabaseType::Standalone,
        ..Default::default()
    }).unwrap();

    let trans = db.begin_transaction(&ctx).unwrap();
    
    // Only index Category and ProductId. 
    // IsActive and Price are "Ride-on" metadata - stored in the key but not part of the sort order.
    let index_spec = IndexSpecification {
        index_fields: vec![
            IndexFieldSpecification { field_name: "Category".to_string(), ascending_sort_order: true },
            IndexFieldSpecification { field_name: "ProductId".to_string(), ascending_sort_order: true },
        ],
    };

    let mut opts = BtreeOptions::default();
    opts.is_primitive_key = false;
    opts.set_index_specification(index_spec);
    
    let products = Btree::<ProductKey, String>::create(&ctx, "products", &trans, Some(opts)).unwrap();

    // Add a product with a large description (Value)
    let key = ProductKey::new("Electronics", 999, true, 100.0);
    let large_description = "X".repeat(10000); // Simulate large payload
    products.add(&ctx, key.clone(), large_description).unwrap();

    println!("Added: {:?}", key);

    // Scenario: We want to change the Price and IsActive status.
    // Traditional way: Find, GetValue (heavy I/O), Update Value, Update.
    // SOP way: Find, GetCurrentKey (light I/O), Update Key, UpdateCurrentKey.

    if products.find(&ctx, key.clone()).unwrap() {
        // 1. Get the key only (fast, no value fetch)
        // current_key returns Option<Item<K, V>>
        if let Some(mut current_item) = products.current_key(&ctx).unwrap() {
            println!("Current Metadata: Price={}, Active={}", current_item.key.price, current_item.key.is_active);

            // 2. Modify metadata
            current_item.key.price = 120.0;
            current_item.key.is_active = false;

            // 3. Update the key in place
            // This is extremely fast because it doesn't touch the 10KB value on disk.
            products.update_current_key(&ctx, current_item).unwrap();
            println!("Metadata updated via UpdateCurrentKey.");
        }
    }

    // Verify
    // We need to construct the key with OLD values to find it? 
    // Or if the index only uses Category and ProductId, we can find it with new values?
    // Since we didn't specify index spec, default is all fields.
    // So we need to search with NEW values.
    let new_key = ProductKey::new("Electronics", 999, false, 120.0);
    if products.find(&ctx, new_key).unwrap() {
        if let Some(updated_item) = products.current_key(&ctx).unwrap() {
            println!("New Metadata: Price={}, Active={}", updated_item.key.price, updated_item.key.is_active);
        }
    }

    trans.commit(&ctx).unwrap();
}
