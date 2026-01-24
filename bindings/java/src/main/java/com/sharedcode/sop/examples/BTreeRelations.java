package com.sharedcode.sop.examples;

import com.sharedcode.sop.*;
import java.util.Arrays;

public class BTreeRelations {

    // Strongly typed User class
    public static class User {
        public String id;
        public String name;
        public User() {}
        public User(String id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    // Strongly typed Order class
    public static class Order {
        public String id;
        public String user_id; // Snake case to match relation field
        public double amount;
        public Order() {}
        public Order(String id, String userId, double amount) {
            this.id = id;
            this.user_id = userId;
            this.amount = amount;
        }
    }

    public static void main(String[] args) {
        Context ctx = new Context();
        DatabaseOptions dbOpts = new DatabaseOptions();
        dbOpts.stores_folders = Arrays.asList("data");
        Database db = new Database(dbOpts);

        try (Transaction trans = db.beginTransaction(ctx)) {
            // 1. Create 'Users' Store (Target)
            System.out.println("Creating 'users' store...");
            BTree<String, User> users = BTree.create(ctx, "users", trans, null, String.class, User.class);
            users.add("user_1", new User("user_1", "Alice"));

            // 2. Create 'Orders' Store with Relation Metadata (Source)
            System.out.println("Creating 'orders' store with Relation metadata...");

            // Define relation: orders.user_id -> users.id
            Relation rel = new Relation(
                Arrays.asList("user_id"),
                "users",
                Arrays.asList("id")
            );

            BTreeOptions opts = new BTreeOptions("orders");
            opts.relations = Arrays.asList(rel);

            // Create store
            BTree<String, Order> orders = BTree.create(ctx, "orders", trans, opts, String.class, Order.class);
            
            // Add a dummy order using strongly-typed object
            orders.add("order_A", new Order("order_A", "user_1", 100.0));

            trans.commit();
            System.out.println("Successfully created stores with Relations!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
