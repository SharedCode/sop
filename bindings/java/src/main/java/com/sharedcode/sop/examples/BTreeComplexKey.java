package com.sharedcode.sop.examples;

import com.sharedcode.sop.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BTreeComplexKey {

    public static class EmployeeKey {
        @com.fasterxml.jackson.annotation.JsonProperty("Region")
        public String region;
        @com.fasterxml.jackson.annotation.JsonProperty("Department")
        public String department;
        @com.fasterxml.jackson.annotation.JsonProperty("Id")
        public int id;

        public EmployeeKey() {}

        public EmployeeKey(String region, String department, int id) {
            this.region = region;
            this.department = department;
            this.id = id;
        }

        @Override
        public String toString() {
            return region + "/" + department + "/" + id;
        }
    }

    public static void run() {
        System.out.println("\n--- Running Complex Keys & Index Specification ---");

        try (Context ctx = new Context()) {
            DatabaseOptions dbOpts = new DatabaseOptions();
            dbOpts.stores_folders = Collections.singletonList("sop_data_complex");
            dbOpts.type = DatabaseType.Standalone;
            
            Database db = new Database(dbOpts);

            try (Transaction trans = db.beginTransaction(ctx)) {
                // Define Index Specification
                // This tells SOP how to construct the composite key for sorting and prefix scanning.
                // Using PascalCase to match C# example and ensure compatibility if that's the issue.
                String indexSpec = "{\n" +
                        "    \"index_fields\": [\n" +
                        "        { \"field_name\": \"Region\", \"ascending_sort_order\": true },\n" +
                        "        { \"field_name\": \"Department\", \"ascending_sort_order\": true },\n" +
                        "        { \"field_name\": \"Id\", \"ascending_sort_order\": true }\n" +
                        "    ]\n" +
                        "}";

                BTreeOptions opts = new BTreeOptions("employees");
                opts.indexSpecification = indexSpec;
                
                BTree<EmployeeKey, String> employees = db.newBtree(ctx, "employees", trans, opts, EmployeeKey.class, String.class);

                System.out.println("Adding employees...");
                employees.add(new Item<>(new EmployeeKey("US", "Sales", 101), "Alice"));
                employees.add(new Item<>(new EmployeeKey("US", "Sales", 102), "Bob"));
                employees.add(new Item<>(new EmployeeKey("US", "Engineering", 201), "Charlie"));
                employees.add(new Item<>(new EmployeeKey("EU", "Sales", 301), "David"));

                // Exact Match
                EmployeeKey keyToFind = new EmployeeKey("US", "Sales", 101);
                if (employees.find(keyToFind)) {
                    List<Item<EmployeeKey, String>> items = employees.getValues(Collections.singletonList(new Item<>(keyToFind, null)));
                    if (items != null && !items.isEmpty()) {
                        System.out.println("Found Exact: " + items.get(0).key + " -> " + items.get(0).value);
                    }
                } else {
                    System.out.println("Exact Match NOT FOUND for " + keyToFind);
                }

                // Negative Test
                EmployeeKey keyNotFound = new EmployeeKey("ZZ", "Sales", 999);
                if (employees.find(keyNotFound)) {
                    List<Item<EmployeeKey, String>> items = employees.getValues(Collections.singletonList(new Item<>(keyNotFound, null)));
                    if (items != null && !items.isEmpty()) {
                        System.out.println("Found Negative: " + items.get(0).key + " -> " + items.get(0).value);
                    }
                } else {
                    System.out.println("Negative Test: Correctly not found " + keyNotFound);
                }

                trans.commit();
            } catch (Exception e) {
                e.printStackTrace();
            }

            // Simplified Lookup (Anonymous Type equivalent using Map)
            try (Transaction trans2 = db.beginTransaction(ctx)) { // Default is Read/Write, but we only read. 
                // Note: Java binding doesn't expose TransactionMode yet in beginTransaction, assuming default.
                
                System.out.println("Searching with Map (Anonymous Type equivalent)...");
                // Open as Object key to allow passing Map
                BTree<Object, String> simpleEmployees = db.openBtree(ctx, "employees", trans2, Object.class, String.class);
                
                Map<String, Object> anonKey = new HashMap<>();
                anonKey.put("Region", "EU");
                anonKey.put("Department", "Sales");
                anonKey.put("Id", 301);
                
                if (simpleEmployees.find(anonKey)) {
                    List<Item<Object, String>> items = simpleEmployees.getValues(Collections.singletonList(new Item<>(anonKey, null)));
                    if (items != null && !items.isEmpty()) {
                        System.out.println("Found Anonymous: " + items.get(0).value);
                    }
                }
                trans2.commit();
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
