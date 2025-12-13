package com.sharedcode.sop;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

public class BTreeTest extends BaseTest {

    private String tempDir;

    static {
        // Set JNA library path to where libjsondb.dylib is located
        File libDir = new File("../../bindings/main");
        if (!libDir.exists()) {
            libDir = new File("bindings/main");
        }
        if (libDir.exists()) {
            System.setProperty("jna.library.path", libDir.getAbsolutePath());
        }
    }

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("sop_test_" + UUID.randomUUID()).toFile().getAbsolutePath();
    }

    @After
    public void tearDown() throws Exception {
        File dir = new File(tempDir);
        if (dir.exists()) {
            deleteDirectory(dir);
        }
    }

    private void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                deleteDirectory(file);
            }
        }
        directory.delete();
    }

    @Test
    public void testUserBtreeCudBatch() throws Exception {
        try (Context ctx = new Context()) {
            DatabaseOptions dbOpts = new DatabaseOptions();
            dbOpts.stores_folders = Collections.singletonList(tempDir);
            dbOpts.type = DatabaseType.Standalone; // Standalone
            
            Database db = new Database(dbOpts);

            // 1. Create (Insert)
            try (Transaction t = db.beginTransaction(ctx)) {
                BTree<String, String> btree = BTree.create(ctx, "users", t, null, String.class, String.class);
                List<Item<String, String>> items = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    items.add(new Item<>("user_" + i, "User Name " + i));
                }
                btree.add(items);
                t.commit();
            }

            // Verify Insert
            try (Transaction t = db.beginTransaction(ctx)) {
                // Note: In Java binding we use open() instead of NewBtree() for existing ones, 
                // or create() which handles open if exists? 
                // C# NewBtree handles both. Java create() calls NewBtree action.
                // Let's use open() as it maps to OpenBtree action.
                BTree<String, String> btree = BTree.open(ctx, "users", t, String.class, String.class);
                
                assertEquals(100, btree.count());
                
                assertTrue(btree.find("user_50"));
                List<Item<String, String>> items = btree.getValues(Collections.singletonList(new Item<>("user_50", null)));
                assertEquals(1, items.size());
                assertEquals("User Name 50", items.get(0).value);
                t.commit();
            }

            // 2. Update
            try (Transaction t = db.beginTransaction(ctx)) {
                BTree<String, String> btree = BTree.open(ctx, "users", t, String.class, String.class);
                List<Item<String, String>> items = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    items.add(new Item<>("user_" + i, "Updated User " + i));
                }
                btree.update(items);
                t.commit();
            }

            // Verify Update
            try (Transaction t = db.beginTransaction(ctx)) {
                BTree<String, String> btree = BTree.open(ctx, "users", t, String.class, String.class);
                List<Item<String, String>> items = btree.getValues(Collections.singletonList(new Item<>("user_50", null)));
                assertEquals(1, items.size());
                assertEquals("Updated User 50", items.get(0).value);
                t.commit();
            }

            // 3. Delete
            try (Transaction t = db.beginTransaction(ctx)) {
                BTree<String, String> btree = BTree.open(ctx, "users", t, String.class, String.class);
                List<String> keys = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    keys.add("user_" + i);
                }
                btree.remove(keys);
                t.commit();
            }

            // Verify Delete
            try (Transaction t = db.beginTransaction(ctx)) {
                BTree<String, String> btree = BTree.open(ctx, "users", t, String.class, String.class);
                assertEquals(0, btree.count());
                t.commit();
            }
        }
    }
}
