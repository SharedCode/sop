use sop::{Context, Database, DatabaseOptions, DatabaseType, Btree, BtreeOptions};
use serde::{Serialize, Deserialize};
use std::fs;
use std::path::Path;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct EmployeeKey {
    #[serde(rename = "Region")]
    region: String,
    #[serde(rename = "Department")]
    department: String,
    #[serde(rename = "Id")]
    id: i32,
}

impl EmployeeKey {
    fn new(region: &str, department: &str, id: i32) -> Self {
        Self {
            region: region.to_string(),
            department: department.to_string(),
            id,
        }
    }
}

fn main() {
    println!("\n--- Running Complex Keys & Index Specification ---");

    let ctx = Context::new();
    let db_path = "data/complex_key_demo_db";
    if Path::new(db_path).exists() {
        fs::remove_dir_all(db_path).unwrap();
    }

    let db = Database::new(&ctx, DatabaseOptions {
        stores_folders: Some(vec![db_path.to_string()]),
        db_type: DatabaseType::Standalone,
        ..Default::default()
    }).unwrap();

    let trans = db.begin_transaction(&ctx).unwrap();
    
    // Define Index Specification
    let index_spec = r#"{
        "index_fields": [
            { "name": "Region", "ascending_sort_order": true },
            { "name": "Department", "ascending_sort_order": true },
            { "name": "Id", "ascending_sort_order": true }
        ]
    }"#;

    let mut opts = BtreeOptions::default();
    opts.is_primitive_key = false;
    opts.index_specification = Some(index_spec.to_string());

    let employees = Btree::<EmployeeKey, String>::create(&ctx, "employees", &trans, Some(opts)).unwrap();

    println!("Adding employees...");
    employees.add(&ctx, EmployeeKey::new("US", "Sales", 101), "Alice".to_string()).unwrap();
    employees.add(&ctx, EmployeeKey::new("US", "Sales", 102), "Bob".to_string()).unwrap();
    employees.add(&ctx, EmployeeKey::new("US", "Engineering", 201), "Charlie".to_string()).unwrap();
    employees.add(&ctx, EmployeeKey::new("EU", "Sales", 301), "David".to_string()).unwrap();

    // Exact Match
    let key_to_find = EmployeeKey::new("US", "Sales", 101);
    if employees.find(&ctx, key_to_find.clone()).unwrap() {
        if let Some(item) = employees.get_value(&ctx, key_to_find).unwrap() {
            if let Some(val) = &item.value {
                println!("Found Exact: {:?} -> {}", item.key, val);
            }
        }
    }

    trans.commit(&ctx).unwrap();

    let trans2 = db.begin_transaction(&ctx).unwrap();
    
    // Simplified Lookup (Anonymous Type equivalent in Rust is just another struct with same shape)
    println!("Searching with Anonymous Type (Struct with same shape)...");
    // In Rust, as long as it serializes to the same JSON, it works.
    #[derive(Serialize, Deserialize, Clone)]
    struct AnonKey {
        #[serde(rename = "Region")]
        region: String,
        #[serde(rename = "Department")]
        department: String,
        #[serde(rename = "Id")]
        id: i32,
    }
    
    let mut open_opts = BtreeOptions::default();
    open_opts.is_primitive_key = false;
    let simple_employees = db.open_btree::<AnonKey, String>(&ctx, "employees", &trans2, Some(open_opts)).unwrap();
    let anon_key = AnonKey { region: "EU".to_string(), department: "Sales".to_string(), id: 301 };
    
    if simple_employees.find(&ctx, anon_key).unwrap() {
        // We can't reuse anon_key because it was moved.
        let anon_key = AnonKey { region: "EU".to_string(), department: "Sales".to_string(), id: 301 };
        if let Some(item) = simple_employees.get_value(&ctx, anon_key).unwrap() {
            if let Some(val) = &item.value {
                println!("Found Anonymous: {}", val);
            }
        }
    }
    trans2.commit(&ctx).unwrap();
}
