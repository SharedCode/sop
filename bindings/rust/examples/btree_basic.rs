use sop::{Context, Database, DatabaseOptions, L2CacheType, DatabaseType};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Person {
    #[serde(rename = "FirstName")]
    first_name: String,
    #[serde(rename = "LastName")]
    last_name: String,
    #[serde(rename = "Phone")]
    phone: String,
}

impl Person {
    fn new(first: &str, last: &str, phone: &str) -> Self {
        Self {
            first_name: first.to_string(),
            last_name: last.to_string(),
            phone: phone.to_string(),
        }
    }
}

fn main() {
    println!("SOP Rust Binding - Btree Basic Example");

    // 1. Initialize Context
    let ctx = Context::new();
    if let Some(err) = ctx.error() {
        eprintln!("Error creating context: {}", err);
        return;
    }

    // 2. Configure Database
    let mut options = DatabaseOptions::default();
    options.db_type = DatabaseType::Standalone;
    options.cache_type = L2CacheType::InMemory;
    options.stores_folders = Some(vec!["data/btree_basic_db".to_string()]);
    
    // Clean up previous run
    if std::path::Path::new("data/btree_basic_db").exists() {
        std::fs::remove_dir_all("data/btree_basic_db").unwrap();
    }
    
    // 3. Create/Open Database
    let db = match Database::new(&ctx, options) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Error creating database: {}", e);
            return;
        }
    };
    println!("Database created with ID: {}", db.id);

    // 4. Begin Transaction
    let trans = match db.begin_transaction(&ctx) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Error beginning transaction: {}", e);
            return;
        }
    };

    // 5. Create Btree
    // We use i64 as key and Person as value
    let btree = match db.new_btree::<i64, Person>(&ctx, "PeopleStore", &trans, None) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("Error creating btree: {}", e);
            return;
        }
    };
    println!("Btree 'PeopleStore' created with ID: {}", btree.id);

    // 6. Add Items
    let p1 = Person::new("Joe", "Doe", "555-1234");
    let p2 = Person::new("Jane", "Doe", "555-5678");

    if let Err(e) = btree.add(&ctx, 101, p1) {
        eprintln!("Error adding item 101: {}", e);
    }
    if let Err(e) = btree.add(&ctx, 102, p2) {
        eprintln!("Error adding item 102: {}", e);
    }
    println!("Added 2 items.");

    // 7. Commit
    if let Err(e) = trans.commit(&ctx) {
        eprintln!("Error committing transaction: {}", e);
        return;
    }
    println!("Transaction committed.");

    // 8. Read Back
    let trans2 = db.begin_transaction(&ctx).unwrap();
    
    // Open the btree again with the new transaction
    let btree2 = db.open_btree::<i64, Person>(&ctx, "PeopleStore", &trans2, None).unwrap();
    
    match btree2.get_value(&ctx, 101) {
        Ok(Some(item)) => {
            if let Some(val) = &item.value {
                println!("Found Item 101: {:?}", val);
            }
        },
        Ok(None) => println!("Item 101 not found."),
        Err(e) => eprintln!("Error finding item 101: {}", e),
    }

    trans2.commit(&ctx).unwrap();
    println!("Finished.");
}
