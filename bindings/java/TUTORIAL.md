# SOP Java Tutorial

This tutorial guides you through building a simple inventory management system using SOP with Java.

## Prerequisites

*   Java 11+
*   Maven
*   (Optional) Redis for Clustered mode

## Step 1: Project Setup

Create a new Maven project and add the `sop-java` dependency (assuming you have built and installed it locally).

```xml
<dependencies>
    <dependency>
        <groupId>com.sharedcode</groupId>
        <artifactId>sop-java</artifactId>
        <version>1.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

## Step 2: Define Your Data Model

SOP stores POJOs (Plain Old Java Objects). Let's define a `Product` class.

```java
import java.io.Serializable;

public class Product implements Serializable {
    public String id;
    public String name;
    public double price;
    public int stock;

    public Product() {} // Required for Jackson deserialization

    public Product(String id, String name, double price, int stock) {
        this.id = id;
        this.name = name;
        this.price = price;
        this.stock = stock;
    }

    @Override
    public String toString() {
        return String.format("Product[id=%s, name=%s, price=%.2f, stock=%d]", id, name, price, stock);
    }
}
```

## Step 3: Initialize the Database

We'll start in **Standalone Mode** (local file system, no Redis).

```java
import com.sharedcode.sop.*;
import java.util.Collections;

public class InventoryApp {
    public static void main(String[] args) {
        // 1. Create Context
        try (Context ctx = new Context()) {
            
            // 2. Configure Database
            DatabaseOptions dbOpts = new DatabaseOptions();
            dbOpts.stores_folders = Collections.singletonList("inventory_data");
            dbOpts.type = DatabaseType.Standalone;
            
            Database db = new Database(dbOpts);
            
            // ... operations ...
        } catch (SopException e) {
            e.printStackTrace();
        }
    }
}
```

## Step 4: Create and Populate the B-Tree

We'll create a B-Tree named "products" where the Key is the Product ID (`String`) and the Value is the `Product` object.

```java
            // 3. Start Transaction (Write)
            try (Transaction trans = db.beginTransaction(ctx)) {
                
                // 4. Create/Open B-Tree
                // Keys are String, Values are Product
                BTree<String, Product> products = BTree.create(ctx, "products", trans, null, String.class, Product.class);

                // 5. Add Items
                products.add("p1", new Product("p1", "Laptop", 999.99, 10));
                products.add("p2", new Product("p2", "Mouse", 25.50, 100));
                products.add("p3", new Product("p3", "Keyboard", 75.00, 50));

                System.out.println("Added 3 products.");

                // 6. Commit
                trans.commit();
            }
```

## Step 5: Read and Update Data

Now let's read a product, update its stock, and save it back.

```java
            // 7. Start Transaction (Read/Write)
            try (Transaction trans = db.beginTransaction(ctx)) {
                BTree<String, Product> products = BTree.open(ctx, "products", trans, String.class, Product.class);

                // 8. Find Item
                if (products.find("p2")) {
                    Item<String, Product> item = products.getCurrentValue();
                    Product p = item.value;
                    
                    System.out.println("Found: " + p);

                    // 9. Update Stock
                    p.stock -= 5; // Sell 5 mice
                    
                    // 10. Update in B-Tree
                    products.updateCurrentValue(item);
                    System.out.println("Updated stock for " + p.name);
                }

                trans.commit();
            }
```

## Step 6: Range Query (Iterate)

Let's list all products.

```java
            try (Transaction trans = db.beginTransaction(ctx, TransactionMode.ForReading)) {
                BTree<String, Product> products = BTree.open(ctx, "products", trans, String.class, Product.class);

                System.out.println("\n--- Product List ---");
                if (products.first()) {
                    do {
                        Item<String, Product> item = products.getCurrentValue();
                        System.out.println(item.value);
                    } while (products.next());
                }
            }
```

## Step 7: Link Stores (Relations for AI)

In SOP, we don't use typical RDBMS foreign keys. We use **Link Stores** (tables with Composite Keys) to map relationships efficiently. This "flat" topology is critical for building schemas that AI Agents can easily navigate using simple tool calls.

**Example**: Mapping Suppliers to Products (Many-to-Many).

```java
            try (Transaction trans = db.beginTransaction(ctx)) {
                // Link Store: Key="SupplierID:ProductID", Value=Placeholder (or metadata like "active")
                // We create a separate B-Tree just for this relationship.
                BTree<String, String> links = BTree.create(ctx, "supplier_products", trans, null, String.class, String.class);
                
                // Add Links: Supplier 's1' supplies 'p1' and 'p2'
                // The key format "ParentID:ChildID" allows efficient prefix scanning.
                links.add("s1:p1", "active");
                links.add("s1:p2", "active");
                
                trans.commit();
            }
```

This structure allows you (or an AI agent) to find all products for `s1` by simply scanning the `supplier_products` store for keys starting with `"s1:"`.

## Step 8: Scaling to Clustered Mode

To switch to **Clustered Mode** (multiple app instances sharing data via Redis):

1.  Start Redis (`redis-server`).
2.  Initialize Redis in your code.
3.  Change `DatabaseOptions.type` to `1`.

```java
// At application startup
Redis.initialize("redis://localhost:6379");

// ... inside main ...
dbOpts.type = DatabaseType.Clustered;

// ... at application shutdown
Redis.close();
```

## Next Steps

*   **Composite Keys**: Use `BTreeComplexKey` to create multi-part keys (e.g., `Region/Category/ID`) for advanced sorting.
*   **Batch Operations**: Use `add(List<Item>)` for high-performance bulk inserts.
*   **Cassandra**: Use `CassandraConfig` to back your store with Cassandra for massive scale.
