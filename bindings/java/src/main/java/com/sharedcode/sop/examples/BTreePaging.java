package com.sharedcode.sop.examples;

import com.sharedcode.sop.*;
import java.util.Collections;
import java.util.List;

public class BTreePaging {
    public static void run() {
        System.out.println("\n--- Running B-Tree Paging & Navigation ---");

        try (Context ctx = new Context()) {
            DatabaseOptions dbOpts = new DatabaseOptions();
            dbOpts.stores_folders = Collections.singletonList("sop_data_paging");
            dbOpts.type = DatabaseType.Standalone;
            
            Database db = new Database(dbOpts);

            try (Transaction trans = db.beginTransaction(ctx)) {
                BTree<Integer, String> btree = db.newBtree(ctx, "logs", trans, null, Integer.class, String.class);

                // Populate with 100 items
                System.out.println("Populating 100 log entries...");
                for (int i = 0; i < 100; i++) {
                    btree.add(new Item<>(i, "Log Entry " + i));
                }

                // Page 1: First 10 items
                System.out.println("\n--- Page 1 (Items 0-9) ---");
                PagingInfo pagingInfo = new PagingInfo();
                pagingInfo.pageSize = 10;
                pagingInfo.pageOffset = 0;
                
                // GetKeys is efficient - it scans the index without fetching values
                List<Item<Integer, String>> page1 = btree.getKeys(pagingInfo);
                if (page1 != null) {
                    for (Item<Integer, String> item : page1) {
                        System.out.print(item.key + " ");
                    }
                }
                System.out.println();

                // Page 2: Next 10 items
                System.out.println("\n--- Page 2 (Items 10-19) ---");
                // Let's move cursor to 10 explicitly for demo
                btree.find(10);
                
                PagingInfo pagingInfo2 = new PagingInfo();
                pagingInfo2.pageSize = 10;
                
                List<Item<Integer, String>> page2 = btree.getKeys(pagingInfo2);
                if (page2 != null) {
                    for (Item<Integer, String> item : page2) {
                        System.out.print(item.key + " ");
                    }
                }
                System.out.println();

                // Navigation: First/Last
                System.out.println("\n--- Navigation ---");
                if (btree.moveToFirst()) {
                    Item<Integer, String> first = btree.getCurrentKey();
                    if (first != null) {
                        System.out.println("First Key: " + first.key);
                    }
                }

                if (btree.moveToLast()) {
                    Item<Integer, String> last = btree.getCurrentKey();
                    if (last != null) {
                        System.out.println("Last Key: " + last.key);
                    }
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
