use sop::{Context, Database, DatabaseOptions, CassandraConfig, CassandraAuthenticator, open_cassandra_connection, close_cassandra_connection, open_redis_connection, close_redis_connection};
use std::fs;
use std::path::Path;

fn main() {
    println!("--- Cassandra & Redis Demo ---");
    println!("Note: This demo requires running Cassandra and Redis instances on localhost.");
    println!("Ensure you have created the keyspace in Cassandra:");
    println!("CREATE KEYSPACE sop_test WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};");

    let config = CassandraConfig {
        cluster_hosts: vec!["localhost".to_string()],
        consistency: 1, // LocalQuorum
        connection_timeout: 5000,
        replication_clause: "{'class':'SimpleStrategy', 'replication_factor':1}".to_string(),
        authenticator: CassandraAuthenticator {
            username: "".to_string(),
            password: "".to_string(),
        },
    };

    println!("Initializing Cassandra connection...");
    if let Err(e) = open_cassandra_connection(config) {
        eprintln!("Failed to initialize Cassandra: {}", e);
        return;
    }
    println!("Cassandra initialized successfully.");

    println!("Initializing Redis connection...");
    if let Err(e) = open_redis_connection("redis://localhost:6379") {
        eprintln!("Failed to initialize Redis: {}", e);
        let _ = close_cassandra_connection();
        return;
    }
    println!("Redis initialized successfully.");

    // Create Clustered Database
    let ctx = Context::new();
    let db_path = "data/cassandra_demo";
    if Path::new(db_path).exists() {
        fs::remove_dir_all(db_path).unwrap();
    }

    println!("Creating Cassandra-backed Database at {}...", db_path);
    let db = Database::new(&ctx, DatabaseOptions {
        stores_folders: Some(vec![db_path.to_string()]),
        keyspace: Some("sop_test".to_string()),
        ..Default::default()
    }).unwrap();

    // 1. Insert
    println!("Starting Write Transaction...");
    {
        let trans = db.begin_transaction(&ctx).unwrap();
        let btree = db.open_btree::<String, String>(&ctx, "cassandra_btree", &trans, None).unwrap();
        
        println!("Adding item 'key1'...");
        btree.add(&ctx, "key1".to_string(), "value1".to_string()).unwrap();
        
        trans.commit(&ctx).unwrap();
        println!("Committed.");
    }

    // 2. Read
    println!("Starting Read Transaction...");
    {
        let trans = db.begin_transaction(&ctx).unwrap();
        let btree = db.open_btree::<String, String>(&ctx, "cassandra_btree", &trans, None).unwrap();
        
        if btree.find(&ctx, "key1".to_string()).unwrap() {
            if let Some(item) = btree.get_value(&ctx, "key1".to_string()).unwrap() {
                println!("Found item: Key={}, Value={}", item.key, item.value.unwrap_or_default());
            }
        } else {
            println!("Item not found!");
        }
        
        trans.commit(&ctx).unwrap();
    }

    let _ = close_redis_connection();
    let _ = close_cassandra_connection();

    println!("--- End of Cassandra Demo ---");
}
