package com.sharedcode.sop;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class BTreeBatchTest extends BaseTest {
    private List<String> testDirs = new ArrayList<>();

    @After
    public void tearDown() {
        for (String dir : testDirs) {
            deleteDirectory(new File(dir));
        }
    }

    private void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }

    private String createTempDir(String suffix) {
        String path = System.getProperty("java.io.tmpdir") + File.separator + "sop_cud_test_" + suffix + "_" + UUID.randomUUID().toString();
        testDirs.add(path);
        return path;
    }

    @Test
    public void testUserBtreeCudBatch() throws Exception {
        String path = createTempDir("user_btree");
        try (Context ctx = new Context()) {
            DatabaseOptions dbOpts = new DatabaseOptions();
            dbOpts.stores_folders = Collections.singletonList(path);
            dbOpts.type = DatabaseType.Standalone; // Standalone
            
            Database db = new Database(dbOpts);

            // 1. Create (Insert)
            try (Transaction t = db.beginTransaction(ctx)) {
                BTreeOptions bo = new BTreeOptions("users");
                bo.isUnique = true;
                
                BTree<String, String> b3 = BTree.create(ctx, "users", t, bo, String.class, String.class);
                
                List<Item<String, String>> items = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    items.add(new Item<>("user_" + i, "User Name " + i));
                }
                
                b3.add(items);
                t.commit();
            }

            // Verify Insert
            try (Transaction t = db.beginTransaction(ctx)) {
                BTree<String, String> b3 = BTree.open(ctx, "users", t, String.class, String.class);
                long count = b3.count();
                assertEquals(100, count);
                
                boolean found = b3.find("user_50");
                assertTrue(found);
                
                List<Item<String, String>> keysToGet = new ArrayList<>();
                keysToGet.add(new Item<>("user_50", null));
                
                List<Item<String, String>> items = b3.getValues(keysToGet);
                assertEquals(1, items.size());
                assertEquals("User Name 50", items.get(0).value);
                t.commit();
            }

            // 2. Update
            try (Transaction t = db.beginTransaction(ctx)) {
                BTree<String, String> b3 = BTree.open(ctx, "users", t, String.class, String.class);
                
                List<Item<String, String>> items = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    items.add(new Item<>("user_" + i, "Updated User " + i));
                }
                
                b3.update(items);
                t.commit();
            }

            // Verify Update
            try (Transaction t = db.beginTransaction(ctx)) {
                BTree<String, String> b3 = BTree.open(ctx, "users", t, String.class, String.class);
                
                List<Item<String, String>> keysToGet = new ArrayList<>();
                keysToGet.add(new Item<>("user_50", null));
                
                List<Item<String, String>> items = b3.getValues(keysToGet);
                assertEquals(1, items.size());
                assertEquals("Updated User 50", items.get(0).value);
                t.commit();
            }

            // 3. Delete
            try (Transaction t = db.beginTransaction(ctx)) {
                BTree<String, String> b3 = BTree.open(ctx, "users", t, String.class, String.class);
                
                List<String> keys = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    keys.add("user_" + i);
                }
                
                b3.remove(keys);
                t.commit();
            }

            // Verify Delete
            try (Transaction t = db.beginTransaction(ctx)) {
                BTree<String, String> b3 = BTree.open(ctx, "users", t, String.class, String.class);
                long count = b3.count();
                assertEquals(0, count);
                t.commit();
            }
        }
    }
}
