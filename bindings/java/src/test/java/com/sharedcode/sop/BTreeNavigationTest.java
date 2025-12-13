package com.sharedcode.sop;

import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class BTreeNavigationTest extends BaseTest {
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
        String path = System.getProperty("java.io.tmpdir") + File.separator + "sop_nav_test_" + suffix + "_" + UUID.randomUUID().toString();
        testDirs.add(path);
        return path;
    }

    @Test
    public void testNavigation() throws Exception {
        String path = createTempDir("nav_btree");
        try (Context ctx = new Context()) {
            DatabaseOptions dbOpts = new DatabaseOptions();
            dbOpts.stores_folders = Collections.singletonList(path);
            dbOpts.type = DatabaseType.Standalone; // Standalone
            
            Database db = new Database(dbOpts);

            try (Transaction t = db.beginTransaction(ctx)) {
                BTree<Integer, String> b3 = BTree.create(ctx, "nav_btree", t, null, Integer.class, String.class);

                // Add 100 items
                List<Item<Integer, String>> items = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    items.add(new Item<>(i, "val" + i));
                }
                b3.add(items);
                t.commit();
            }

            // Test Forward Navigation
            try (Transaction t = db.beginTransaction(ctx)) {
                BTree<Integer, String> b3 = BTree.open(ctx, "nav_btree", t, Integer.class, String.class);

                assertTrue(b3.moveToFirst());

                // Fetch 10 items starting from offset 0
                PagingInfo pagingInfo = new PagingInfo();
                pagingInfo.pageOffset = 0;
                pagingInfo.pageSize = 10;
                pagingInfo.fetchCount = 10;
                pagingInfo.direction = 0; // Forward

                List<Item<Integer, String>> keys = b3.getKeys(pagingInfo);

                assertEquals(10, keys.size());
                assertEquals(Integer.valueOf(0), keys.get(0).key);
                assertEquals(Integer.valueOf(9), keys.get(9).key);

                // Get Values for these keys
                List<Item<Integer, String>> values = b3.getValues(keys);
                assertEquals(10, values.size());
                assertEquals("val0", values.get(0).value);
                assertEquals("val9", values.get(9).value);

                t.commit();
            }

            // Test Backward Navigation
            try (Transaction t = db.beginTransaction(ctx)) {
                BTree<Integer, String> b3 = BTree.open(ctx, "nav_btree", t, Integer.class, String.class);

                assertTrue(b3.moveToLast());

                // Fetch 10 items backwards
                PagingInfo pagingInfo = new PagingInfo();
                pagingInfo.pageOffset = 0;
                pagingInfo.pageSize = 10;
                pagingInfo.fetchCount = 10;
                pagingInfo.direction = 1; // Backward

                List<Item<Integer, String>> keys = b3.getKeys(pagingInfo);

                assertEquals(10, keys.size());
                // Backward: 99, 98, ... 90
                assertEquals(Integer.valueOf(99), keys.get(0).key);
                assertEquals(Integer.valueOf(90), keys.get(9).key);

                List<Item<Integer, String>> values = b3.getValues(keys);
                assertEquals(10, values.size());
                assertEquals("val99", values.get(0).value);
                assertEquals("val90", values.get(9).value);
                
                t.commit();
            }
        }
    }
}
