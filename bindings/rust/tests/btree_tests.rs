use sop::{Context, Database, DatabaseOptions, Item, DatabaseType, Btree, BtreeOptions, IndexSpecification, IndexFieldSpecification};
use std::fs;
use uuid::Uuid;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ComplexKey {
    #[serde(rename = "Region")]
    region: String,
    #[serde(rename = "Id")]
    id: i32,
}

#[test]
fn test_complex_key() {
    let temp_dir = std::env::temp_dir().join(format!("sop_test_{}", Uuid::new_v4()));
    fs::create_dir_all(&temp_dir).unwrap();
    let temp_dir_str = temp_dir.to_str().unwrap().to_string();

    // Ensure cleanup
    struct TempDir(std::path::PathBuf);
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let _temp_guard = TempDir(temp_dir.clone());

    let ctx = Context::new();
    let db_opts = DatabaseOptions {
        stores_folders: Some(vec![temp_dir_str.clone()]),
        db_type: DatabaseType::Standalone,
        ..Default::default()
    };
    let db = Database::new(&ctx, db_opts).unwrap();

    let index_spec = IndexSpecification {
        index_fields: vec![
            IndexFieldSpecification { field_name: "Region".to_string(), ascending_sort_order: true },
            IndexFieldSpecification { field_name: "Id".to_string(), ascending_sort_order: true },
        ],
    };

    {
        let trans = db.begin_transaction(&ctx).unwrap();
        let mut opts = BtreeOptions::default();
        opts.set_index_specification(index_spec);
        opts.is_primitive_key = false;
        
        let btree = Btree::<ComplexKey, String>::create(&ctx, "complex", &trans, Some(opts)).unwrap();

        btree.add(&ctx, ComplexKey { region: "US".to_string(), id: 1 }, "Val1".to_string()).unwrap();
        btree.add(&ctx, ComplexKey { region: "EU".to_string(), id: 2 }, "Val2".to_string()).unwrap();
        
        trans.commit(&ctx).unwrap();
    }

    {
        let trans = db.begin_transaction(&ctx).unwrap();
        let mut opts = BtreeOptions::default();
        opts.is_primitive_key = false;
        let btree = db.open_btree::<ComplexKey, String>(&ctx, "complex", &trans, Some(opts)).unwrap();
        
        let found1 = btree.find(&ctx, ComplexKey { region: "US".to_string(), id: 1 }).unwrap();
        assert!(found1);

        let found2 = btree.find(&ctx, ComplexKey { region: "US".to_string(), id: 2 }).unwrap();
        assert!(!found2);
    }
}

#[test]
fn test_user_btree_cud_batch() {
    let temp_dir = std::env::temp_dir().join(format!("sop_test_{}", Uuid::new_v4()));
    fs::create_dir_all(&temp_dir).unwrap();
    let temp_dir_str = temp_dir.to_str().unwrap().to_string();

    // Ensure cleanup
    struct TempDir(std::path::PathBuf);
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let _temp_guard = TempDir(temp_dir.clone());

    let ctx = Context::new();
    let db_opts = DatabaseOptions {
        stores_folders: Some(vec![temp_dir_str.clone()]),
        db_type: DatabaseType::Standalone,
        ..Default::default()
    };
    
    let db = Database::new(&ctx, db_opts).unwrap();

    // 1. Create (Insert)
    {
        let trans = db.begin_transaction(&ctx).unwrap();
        let btree = db.new_btree::<String, String>(&ctx, "users", &trans, None).unwrap();
        
        let mut items = Vec::new();
        for i in 0..100 {
            items.push(Item::new(format!("user_{}", i), format!("User Name {}", i)));
        }
        btree.add_batch(&ctx, items).unwrap();
        trans.commit(&ctx).unwrap();
    }

    // Verify Insert
    {
        let trans = db.begin_transaction(&ctx).unwrap();
        let btree = db.open_btree::<String, String>(&ctx, "users", &trans, None).unwrap();
        
        assert_eq!(100, btree.count().unwrap());
        
        assert!(btree.find(&ctx, "user_50".to_string()).unwrap());
        let item = btree.get_value(&ctx, "user_50".to_string()).unwrap();
        assert!(item.is_some());
        assert_eq!(Some("User Name 50".to_string()), item.unwrap().value);
    }

    // 2. Update
    {
        let trans = db.begin_transaction(&ctx).unwrap();
        let btree = db.open_btree::<String, String>(&ctx, "users", &trans, None).unwrap();
        
        let mut items = Vec::new();
        for i in 0..100 {
            items.push(Item::new(format!("user_{}", i), format!("Updated User {}", i)));
        }
        btree.update_batch(&ctx, items).unwrap();
        trans.commit(&ctx).unwrap();
    }

    // Verify Update
    {
        let trans = db.begin_transaction(&ctx).unwrap();
        let btree = db.open_btree::<String, String>(&ctx, "users", &trans, None).unwrap();
        
        let item = btree.get_value(&ctx, "user_50".to_string()).unwrap();
        assert!(item.is_some());
        assert_eq!(Some("Updated User 50".to_string()), item.unwrap().value);
    }

    // 3. Delete
    {
        let trans = db.begin_transaction(&ctx).unwrap();
        let btree = db.open_btree::<String, String>(&ctx, "users", &trans, None).unwrap();
        
        let mut keys = Vec::new();
        for i in 0..100 {
            keys.push(format!("user_{}", i));
        }
        btree.remove_batch(&ctx, keys).unwrap();
        trans.commit(&ctx).unwrap();
    }

    // Verify Delete
    {
        let trans = db.begin_transaction(&ctx).unwrap();
        let btree = db.open_btree::<String, String>(&ctx, "users", &trans, None).unwrap();
        
        assert_eq!(0, btree.count().unwrap());
        assert!(!btree.find(&ctx, "user_50".to_string()).unwrap());
    }
}
