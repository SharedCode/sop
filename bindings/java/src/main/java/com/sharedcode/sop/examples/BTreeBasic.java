package com.sharedcode.sop.examples;

import com.sharedcode.sop.*;
import java.util.Collections;
import java.util.List;

public class BTreeBasic {
    public static void run() {
        System.out.println("\n--- Running Basic B-Tree Operations ---");

        try (Context ctx = new Context()) {
            // 1. Open Database
            DatabaseOptions dbOpts = new DatabaseOptions();
            dbOpts.stores_folders = Collections.singletonList("sop_data_basic");
            // Assuming 0 is Standalone, similar to C# default or explicit enum
            dbOpts.type = DatabaseType.Standalone; 
            
            Database db = new Database(dbOpts);

            // 2. Start Transaction
            try (Transaction trans = db.beginTransaction(ctx)) {
                // 3. Create/Open B-Tree
                // Note: Java generics are erased, so we pass class tokens
                BTree<String, String> btree = db.newBtree(ctx, "users_basic", trans, null, String.class, String.class);

                // 4. Add Items (Create)
                System.out.println("Adding users...");
                btree.add(new Item<>("user1", "Alice"));
                btree.add(new Item<>("user2", "Bob"));
                btree.add(new Item<>("user3", "Charlie"));

                // 5. Find & Get (Read)
                if (btree.find("user1")) {
                    List<Item<String, String>> items = btree.getValues(Collections.singletonList(new Item<>("user1", null)));
                    if (items != null && !items.isEmpty()) {
                        System.out.println("Found: " + items.get(0).key + " -> " + items.get(0).value);
                    }
                }

                // 6. Update
                System.out.println("Updating user2...");
                btree.update(Collections.singletonList(new Item<>("user2", "Bob Updated")));

                if (btree.find("user2")) {
                    List<Item<String, String>> items = btree.getValues(Collections.singletonList(new Item<>("user2", null)));
                    if (items != null && !items.isEmpty()) {
                        System.out.println("Updated: " + items.get(0).key + " -> " + items.get(0).value);
                    }
                }

                // 7. Remove (Delete)
                System.out.println("Removing user3...");
                btree.remove("user3");

                if (!btree.find("user3")) {
                    System.out.println("user3 removed successfully.");
                }

                // 8. Commit
                trans.commit();
                System.out.println("Transaction committed.");
            } catch (Exception ex) {
                System.out.println("Error: " + ex.getMessage());
                ex.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        run();
    }
}
