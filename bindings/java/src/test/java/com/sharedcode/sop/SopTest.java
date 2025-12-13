package com.sharedcode.sop;

import org.junit.Test;
import static org.junit.Assert.*;
import java.io.File;

public class SopTest extends BaseTest {

    @Test
    public void testSopFlow() throws Exception {
        try (Context ctx = new Context()) {
            // 1. Ensure Database
            // In C#, we pass options. Here we pass null for now as we don't have full options support yet.
            // But we need to make sure the backend creates a database.
            // The previous test used "test_java_db" as name.
            // If we pass null options, the backend might create a default one or fail if it expects something.
            // Let's try to mimic what we had.
            // Actually, we can pass a DatabaseOptions object if we want.
            DatabaseOptions opts = new DatabaseOptions();
            // We can't set name in options because it's not there.
            // But maybe we can rely on default behavior.
            
            Database db = new Database(opts);
            
            // 2. Begin Transaction
            String btreeName = "test_btree_" + System.currentTimeMillis();
            try (Transaction tx = db.beginTransaction(ctx)) {
                assertNotNull(tx);
                
                // 3. New BTree
                BTreeOptions btreeOpts = new BTreeOptions(btreeName);
                try (BTree<String, String> btree = BTree.create(ctx, btreeName, tx, btreeOpts, String.class, String.class)) {
                    assertNotNull(btree);
                    
                    // 4. Use BTree
                    btree.add("key1", "value1");
                    btree.add("key2", "value2");
                    
                    boolean found = btree.find("key1");
                    assertTrue("Should find key1", found);
                }
                
                // 5. Commit
                tx.commit();
            }
            
            // 6. Read back
            try (Transaction tx2 = db.beginTransaction(ctx)) {
                try (BTree<String, String> btree = BTree.open(ctx, btreeName, tx2, String.class, String.class)) {
                    boolean found = btree.find("key1");
                    assertTrue("Should find key1 after commit", found);
                    
                    btree.moveToFirst();
                    Item<String, String> item = btree.getCurrentKey();
                    assertNotNull(item);
                    assertEquals("key1", item.key);

                    Item<String, String> valItem = btree.getCurrentValue();
                    assertNotNull(valItem);
                    assertEquals("key1", valItem.key);
                    assertEquals("value1", valItem.value);
                }
                tx2.commit();
            }
        }
    }
}
