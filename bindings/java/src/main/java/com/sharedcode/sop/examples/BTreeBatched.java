package com.sharedcode.sop.examples;

import com.sharedcode.sop.*;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BTreeBatched {
    public static void run() {
        System.out.println("--- Batched B-Tree Operations Demo ---");

        try (Context ctx = new Context()) {
            String dbPath = "data/batched_demo_db";
            File dbDir = new File(dbPath);
            if (dbDir.exists()) {
                deleteDirectory(dbDir);
            }

            DatabaseOptions dbOpts = new DatabaseOptions();
            dbOpts.stores_folders = Collections.singletonList(dbPath);
            dbOpts.type = DatabaseType.Standalone;
            
            Database db = new Database(dbOpts);

            // 1. Batched Add
            System.out.println("\n1. Batched Add (100 items)...");
            try (Transaction trans = db.beginTransaction(ctx)) {
                BTree<String, String> btree = BTree.create(ctx, "batched_btree", trans, null, String.class, String.class);
                
                List<Item<String, String>> items = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    items.add(new Item<>("key_" + i, "value_" + i));
                }

                btree.add(items);
                trans.commit();
            }
            System.out.println("Committed.");

            // 2. Batched Update
            System.out.println("\n2. Batched Update (100 items)...");
            try (Transaction trans = db.beginTransaction(ctx)) {
                BTree<String, String> btree = db.openBtree(ctx, "batched_btree", trans, String.class, String.class);
                
                List<Item<String, String>> items = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    items.add(new Item<>("key_" + i, "updated_value_" + i));
                }

                btree.update(items);
                trans.commit();
            }
            System.out.println("Committed.");

            // Verify Update
            try (Transaction trans = db.beginTransaction(ctx)) {
                BTree<String, String> btree = db.openBtree(ctx, "batched_btree", trans, String.class, String.class);
                List<Item<String, String>> items = btree.getValues(Collections.singletonList(new Item<>("key_50", null)));
                if (items != null && !items.isEmpty()) {
                    System.out.println("Verified key_50 value: " + items.get(0).value);
                }
                trans.commit();
            }

            // 3. Batched Remove
            System.out.println("\n3. Batched Remove (100 items)...");
            try (Transaction trans = db.beginTransaction(ctx)) {
                BTree<String, String> btree = db.openBtree(ctx, "batched_btree", trans, String.class, String.class);
                
                List<String> keys = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    keys.add("key_" + i);
                }

                btree.remove(keys);
                trans.commit();
            }
            System.out.println("Committed.");

            // Verify Remove
            try (Transaction trans = db.beginTransaction(ctx)) {
                BTree<String, String> btree = db.openBtree(ctx, "batched_btree", trans, String.class, String.class);
                long count = btree.count();
                System.out.println("Verified count: " + count);
                trans.commit();
            }

            System.out.println("--- End of Batched Demo ---");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                deleteDirectory(file);
            }
        }
        directory.delete();
    }

    public static void main(String[] args) {
        run();
    }
}
