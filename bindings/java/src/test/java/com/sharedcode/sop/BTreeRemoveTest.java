package com.sharedcode.sop;

import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class BTreeRemoveTest extends BaseTest {
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
        String path = System.getProperty("java.io.tmpdir") + File.separator + "sop_remove_test_" + suffix + "_" + UUID.randomUUID().toString();
        testDirs.add(path);
        return path;
    }

    @Test
    public void testRemoveBtree() throws Exception {
        String path = createTempDir("remove_btree");
        try (Context ctx = new Context()) {
            DatabaseOptions dbOpts = new DatabaseOptions();
            dbOpts.stores_folders = Collections.singletonList(path);
            dbOpts.type = DatabaseType.Standalone; // Standalone
            
            Database db = new Database(dbOpts);

            // 2. Create a store to delete
            try (Transaction t = db.beginTransaction(ctx)) {
                BTree<String, String> store = BTree.create(ctx, "temp_store", t, null, String.class, String.class);
                store.add("foo", "bar");
                t.commit();
            }

            // 3. Remove the store
            db.removeBtree(ctx, "temp_store");

            // 4. Verify removal
            // Trying to open it should fail or return null/error
            try (Transaction t = db.beginTransaction(ctx)) {
                try {
                    BTree.open(ctx, "temp_store", t, String.class, String.class);
                    fail("Should have thrown exception when opening removed btree");
                } catch (SopException e) {
                    // Expected
                }
                t.commit();
            }
        }
    }
}
