# SOP for Java (sop4j)

SOP bindings for Java allow you to use the high-performance SOP storage engine directly from your Java applications.

## Features

*   **Full SOP Capability**: Access all SOP features including B-Tree management, ACID transactions, and Clustered mode (Redis).
*   **Native Performance**: Uses JNA (Java Native Access) to call the optimized Go shared library directly.
*   **Type Safety**: Generic `BTree<K, V>` implementation for type-safe key-value storage.
*   **Zero Dependencies**: The core binding only requires JNA and Jackson (for JSON serialization).

## Prerequisites

*   **Java**: JDK 11 or higher.
*   **Maven**: For building the project.
*   **Go**: (Optional) Only if you need to rebuild the shared library (`libjsondb.dylib`/`.so`/`.dll`).

## Installation

1.  Clone the repository:
    ```bash
    git clone https://github.com/SharedCode/sop.git
    cd sop/bindings/java
    ```

2.  Build the project:
    ```bash
    mvn clean install
    ```

## Quick Start

### 1. Basic Usage (Standalone - Local disk or shared Network drive)

```java
import com.sharedcode.sop.*;
import java.util.Collections;

public class QuickStart {
    public static void main(String[] args) {
        try (Context ctx = new Context()) {
            // Configure Database
            DatabaseOptions dbOpts = new DatabaseOptions();
            dbOpts.stores_folders = Collections.singletonList("sop_data");
            dbOpts.type = DatabaseType.Standalone;
            Database db = new Database(dbOpts);

            // Start Transaction
            try (Transaction trans = db.beginTransaction(ctx)) {
                // Create B-Tree
                BTree<String, String> btree = db.newBtree(ctx, "users", trans, null, String.class, String.class);

                // Add Item
                btree.add("user1", "Alice");

                // Commit
                trans.commit();
            }
        } catch (SopException e) {
            e.printStackTrace();
        }
    }
}
```

### 2. Clustered Mode (Redis)

```java
import com.sharedcode.sop.*;

// Initialize Redis (Global)
Redis.initialize("redis://localhost:6379");

// Use DatabaseType.Clustered (1)
dbOpts.type = DatabaseType.Clustered; 

// ... operations ...

// Cleanup
Redis.close();
```

## SOP Data Management Suite

SOP includes a powerful **Data Management Suite** that provides **full CRUD** capabilities for your B-Tree stores. It goes beyond simple viewing, offering a complete GUI for inspecting, searching, and managing your data at scale.

To launch it, simply run:

```bash
sop-httpserver
```

### Key Capabilities

*   **Universal Database Server**: Acts as a standalone server for local development or a stateless management node in a clustered enterprise swarm (Kubernetes/EC2).
*   **Full Data Management**: Perform comprehensive CRUD (Create, Read, Update, Delete) operations on any record directly from the UI.
*   **High-Performance Search**: Utilizes B-Tree positioning for instant lookups, even in datasets with millions of records. Supports both simple keys and complex composite keys (e.g., searching by `Country` + `City`).
*   **Efficient Navigation**: Smart pagination and traversal controls (First, Previous, Next, Last) allow you to browse massive datasets without performance penalties.
*   **Bulk Operations**: Designed for rapid-fire management of records with a clean, non-distracting interface.
*   **Responsive & Cross-Platform**: Works seamlessly across diverse monitor sizes and devices.
*   **Automatic Setup**: The tool automatically downloads the correct binary for your OS/Architecture upon first run.

**Usage**: By default, it opens on `http://localhost:8080`.
**Arguments**: You can pass standard flags, e.g., `sop-httpserver -port 9090 -database ./my_data`.

### Multiple Databases Configuration (Recommended)

For managing multiple environments (e.g., Dev, Staging, Prod), create a `config.json`:

```json
{
  "port": 8080,
  "databases": [
    {
      "name": "Local Development",
      "path": "./data/dev_db",
      "mode": "standalone"
    },
    {
      "name": "Production Cluster",
      "path": "/mnt/data/prod",
      "mode": "clustered",
      "redis": "redis-prod:6379"
    }
  ]
}
```

Run with: `sop-httpserver -config config.json`

### Important Note on Concurrency

If database(s) are configured in **standalone mode**, ensure that the http server is the only process/app running to manage the database(s). Alternatively, you can add its HTTP REST endpoint to your embedded/standalone app so it can continue its function and serve HTTP pages at the same time.

If **clustered**, no worries, as SOP takes care of Redis-based coordination with other apps and/or SOP HTTP Servers managing databases using SOP in clustered mode.

### 3. Cassandra Backend

SOP integrates with Apache Cassandra to empower it with features it natively lacks, such as full ACID transactions, efficient Blob storage, and Swarm Computing capabilities.

This mode allows organizations to leverage their existing Cassandra infrastructure and Ops teams for managing the **Registry** and its replication. Meanwhile, SOP handles the heavy lifting of data management, providing Erasure Coding-based replication for data blobs and B-Tree nodes.

```java
import com.sharedcode.sop.*;
import java.util.Collections;

// 1. Configure Cassandra
CassandraConfig cassConfig = new CassandraConfig();
cassConfig.clusterHosts = Collections.singletonList("localhost");
cassConfig.replicationClause = "{'class':'SimpleStrategy', 'replication_factor':1}";
Cassandra.initialize(cassConfig);

// 2. Configure Redis (Required for Clustered mode)
Redis.initialize("redis://localhost:6379");

// 3. Configure Database
DatabaseOptions dbOpts = new DatabaseOptions();
dbOpts.type = DatabaseType.Clustered;
dbOpts.keyspace = "sop_test"; // Ensure this keyspace exists in Cassandra
Database db = new Database(dbOpts);

// ... operations ...

// Cleanup
Redis.close();
Cassandra.close();
```

## Examples

The `src/main/java/com/sharedcode/sop/examples` directory contains comprehensive examples:

| Example | Description |
| :--- | :--- |
| `BTreeBasic` | Basic CRUD operations (Add, Update, Remove, Find). |
| `BTreeBatched` | High-performance batch insertion and iteration. |
| `BTreePaging` | Forward and backward paging through large datasets. |
| `BTreeComplexKey` | Using composite keys with custom sort orders. |
| `BTreeMetadata` | Using "ride-on" metadata in keys. |
| `ConcurrentTransactionsDemoStandalone` | Multi-threaded ACID transactions (Standalone). |
| `ConcurrentTransactionsDemoClustered` | Multi-threaded ACID transactions (Clustered/Redis). |
| `CassandraDemo` | Using Cassandra as the storage backend. |
| `LoggingDemo` | Configuring the SOP logger. |

To run an example:

```bash
mvn compile exec:java -Dexec.mainClass="com.sharedcode.sop.examples.BTreeBasic" -Djna.library.path=../../
```

## Architecture

The Java binding is a thin wrapper around the SOP Go library (`libjsondb`).
1.  **Java Layer**: Provides a clean, idiomatic Java API (`BTree`, `Transaction`, `Database`).
2.  **JNA Layer**: Marshals calls to the C-shared library.
3.  **Go Layer**: The core SOP engine handles storage, indexing, and transactions.

## Troubleshooting

*   **`UnsatisfiedLinkError`**: Ensure `-Djna.library.path` points to the directory containing `libjsondb.dylib` (macOS), `libjsondb.so` (Linux), or `libjsondb.dll` (Windows).
*   **`SopException`**: Wraps errors returned by the underlying Go engine. Check the message for details.
