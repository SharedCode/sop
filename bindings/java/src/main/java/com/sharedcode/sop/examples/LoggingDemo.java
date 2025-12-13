package com.sharedcode.sop.examples;

import com.sharedcode.sop.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

public class LoggingDemo {

    public static void main(String[] args) {
        run();
    }

    public static void run() {
        System.out.println("--- Logging Demo ---");

        // 1. Configure Logging
        String logFile = "sop_demo.log";
        File f = new File(logFile);
        if (f.exists()) f.delete();

        System.out.println("Configuring logger to write to " + logFile + "...");
        try {
            Logger.configure(LogLevel.Debug, logFile);
        } catch (SopException e) {
            e.printStackTrace();
            return;
        }

        // 2. Initialize Context & Database
        try (Context ctx = new Context()) {
            String dbPath = "sop_data_logging_demo";
            deleteDirectory(new File(dbPath));

            System.out.println("Opening database at " + dbPath + "...");
            DatabaseOptions dbOpts = new DatabaseOptions();
            dbOpts.stores_folders = Collections.singletonList(dbPath);
            dbOpts.type = DatabaseType.Standalone;

            Database db = new Database(dbOpts);

            // 3. Perform Operations
            System.out.println("Starting transaction...");
            try (Transaction trans = db.beginTransaction(ctx)) {
                System.out.println("Creating B-Tree...");
                BTree<String, String> btree = BTree.create(ctx, "logging_btree", trans, null, String.class, String.class);

                System.out.println("Adding item...");
                btree.add(new Item<>("hello", "world"));

                System.out.println("Committing transaction...");
                trans.commit();
            } catch (Exception e) {
                e.printStackTrace();
            }

            // 4. Verify Logs
            if (f.exists()) {
                System.out.println("\nSuccess! Log file created at " + logFile + ".");
                System.out.println("First 5 lines of log:");
                try {
                    List<String> lines = Files.readAllLines(Paths.get(logFile));
                    for (int i = 0; i < Math.min(5, lines.size()); i++) {
                        System.out.println(lines.get(i));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("Error: Log file was not created.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        
        System.out.println("--- End of Logging Demo ---");
    }

    private static void deleteDirectory(File file) {
        if (file.exists()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File child : files) {
                    deleteDirectory(child);
                }
            }
            file.delete();
        }
    }
}
