package com.sharedcode.sop.examples;

import com.sharedcode.sop.*;
import java.io.File;
import java.util.Collections;
import java.util.List;

public class CassandraDemo {

    public static void main(String[] args) {
        run();
    }

    public static void run() {
        System.out.println("--- Cassandra & Redis Demo ---");
        System.out.println("Note: This demo requires running Cassandra and Redis instances on localhost.");
        System.out.println("Ensure you have created the keyspace in Cassandra:");
        System.out.println("CREATE KEYSPACE sop_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");

        CassandraConfig config = new CassandraConfig();
        config.clusterHosts = Collections.singletonList("localhost");
        config.consistency = 1; // LocalQuorum
        config.connectionTimeout = 5000;
        config.replicationClause = "{'class':'SimpleStrategy', 'replication_factor':1}";
        config.authenticator = new CassandraConfig.CassandraAuthenticator();
        config.authenticator.username = "";
        config.authenticator.password = "";

        try {
            System.out.println("Initializing Cassandra connection...");
            Cassandra.initialize(config);
            System.out.println("Cassandra initialized successfully.");

            System.out.println("Initializing Redis connection...");
            Redis.initialize("redis://localhost:6379");
            System.out.println("Redis initialized successfully.");

            // Create Clustered Database
            try (Context ctx = new Context()) {
                String dbPath = "sop_data_cassandra_demo";
                deleteDirectory(new File(dbPath));

                System.out.println("Creating Cassandra-backed Database at " + dbPath + "...");
                DatabaseOptions dbOpts = new DatabaseOptions();
                dbOpts.stores_folders = Collections.singletonList(dbPath);
                dbOpts.keyspace = "sop_test";
                // Type 1 is Clustered, but for Cassandra we might need to ensure it's set correctly if it differs.
                // In C# example it doesn't explicitly set Type, but DatabaseOptions defaults might be different.
                // However, if Keyspace is set, SOP usually infers or requires Clustered/Cassandra mode.
                // Let's check DatabaseOptions.java to see if we need to add keyspace field.
                
                Database db = new Database(dbOpts);

                // 1. Insert
                System.out.println("Starting Write Transaction...");
                try (Transaction trans = db.beginTransaction(ctx)) {
                    BTree<String, String> btree = db.newBtree(ctx, "cassandra_btree", trans, null, String.class, String.class);
                    
                    System.out.println("Adding item 'key1'...");
                    btree.add(new Item<>("key1", "value1"));
                    
                    trans.commit();
                    System.out.println("Committed.");
                }

                // 2. Read
                System.out.println("Starting Read Transaction...");
                try (Transaction trans = db.beginTransaction(ctx, TransactionMode.ForReading)) {
                    BTree<String, String> btree = db.openBtree(ctx, "cassandra_btree", trans, String.class, String.class);
                    
                    if (btree.find("key1")) {
                        List<Item<String, String>> items = btree.getValues(Collections.singletonList(new Item<>("key1", null)));
                        if (items != null && !items.isEmpty()) {
                            System.out.println("Found item: Key=" + items.get(0).key + ", Value=" + items.get(0).value);
                        }
                    } else {
                        System.out.println("Item not found!");
                    }
                    
                    trans.commit();
                }
            }
        } catch (SopException e) {
            System.out.println("Operation failed: " + e.getMessage());
        } finally {
            try { Redis.close(); } catch (Exception e) {}
            try { Cassandra.close(); } catch (Exception e) {}
        }

        System.out.println("--- End of Cassandra Demo ---");
    }

    private static void deleteDirectory(File file) {
        if (file.exists()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    deleteDirectory(f);
                }
            }
            file.delete();
        }
    }
}
