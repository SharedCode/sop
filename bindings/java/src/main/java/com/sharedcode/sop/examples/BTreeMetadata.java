package com.sharedcode.sop.examples;

import com.sharedcode.sop.*;
import java.util.Collections;

public class BTreeMetadata {

    public static class ProductKey {
        public String category;
        public int productId;
        public boolean isActive;
        public double price;

        public ProductKey() {}

        public ProductKey(String category, int productId, boolean isActive, double price) {
            this.category = category;
            this.productId = productId;
            this.isActive = isActive;
            this.price = price;
        }

        @Override
        public String toString() {
            return category + "/" + productId + " (Active:" + isActive + ", Price:" + price + ")";
        }
    }

    public static void run() {
        System.out.println("\n--- Running Metadata 'Ride-on' Keys ---");

        try (Context ctx = new Context()) {
            DatabaseOptions dbOpts = new DatabaseOptions();
            dbOpts.stores_folders = Collections.singletonList("sop_data_meta");
            dbOpts.type = DatabaseType.Standalone;
            
            Database db = new Database(dbOpts);

            try (Transaction trans = db.beginTransaction(ctx)) {
                // Only index Category and ProductId. 
                // IsActive and Price are "Ride-on" metadata - stored in the key but not part of the sort order.
                String indexSpec = "{\n" +
                        "    \"index_fields\": [\n" +
                        "        { \"field_name\": \"category\", \"ascending_sort_order\": true },\n" +
                        "        { \"field_name\": \"productId\", \"ascending_sort_order\": true }\n" +
                        "    ]\n" +
                        "}";

                BTreeOptions opts = new BTreeOptions("products");
                opts.indexSpecification = indexSpec;
                
                BTree<ProductKey, String> products = BTree.create(ctx, "products", trans, opts, ProductKey.class, String.class);

                // Add a product with a large description (Value)
                ProductKey key = new ProductKey("Electronics", 999, true, 100.0);
                // Simulate large payload
                StringBuilder sb = new StringBuilder();
                for(int i=0; i<1000; i++) sb.append("X"); 
                String largeDescription = sb.toString();
                
                products.add(new Item<>(key, largeDescription));

                System.out.println("Added: " + key);

                // Scenario: We want to change the Price and IsActive status.
                // Traditional way: Find, GetValue (heavy I/O), Update Value, Update.
                // SOP way: Find, GetCurrentKey (light I/O), Update Key, UpdateCurrentKey.

                if (products.find(key)) {
                    // 1. Get the key only (fast, no value fetch)
                    Item<ProductKey, String> currentItem = products.getCurrentKey();
                    ProductKey currentKey = currentItem.key;

                    System.out.println("Current Metadata: Price=" + currentKey.price + ", Active=" + currentKey.isActive);

                    // 2. Modify metadata
                    currentKey.price = 120.0;
                    currentKey.isActive = false;

                    // 3. Update the key in place
                    // This is extremely fast because it doesn't touch the large value on disk.
                    products.updateCurrentKey(currentItem);
                    System.out.println("Metadata updated via UpdateCurrentKey.");
                }

                // Verify
                // We need to find by the indexed fields. Since we modified non-indexed fields, 
                // the "logical" key (Category, ProductId) is the same, so we can find it.
                if (products.find(new ProductKey("Electronics", 999, false, 0.0))) { 
                    Item<ProductKey, String> updatedItem = products.getCurrentKey();
                    System.out.println("New Metadata: Price=" + updatedItem.key.price + ", Active=" + updatedItem.key.isActive);
                }

                trans.commit();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        run();
    }
}
