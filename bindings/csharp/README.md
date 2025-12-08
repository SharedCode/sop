# SOP for C# (sop4cs)

**Scalable Objects Persistence (SOP)** is a high-performance, transactional storage engine for C#, powered by a robust Go backend. It combines the raw speed of direct disk I/O with the reliability of ACID transactions and the flexibility of modern AI data management.

## Key Features

*   **Unified Database**: Single entry point for managing Vector, Model, and Key-Value stores.
*   **Transactional B-Tree Store**: Unlimited, persistent B-Tree storage for key-value data.
*   **Complex Keys**: Support for composite keys (structs/classes) with custom index specifications.
*   **Metadata "Ride-on" Keys**: Store metadata directly in the B-Tree key (e.g., timestamps, status flags) to enable high-speed scanning and filtering of millions of records without fetching the heavy value payload. Ideal for "Big Data" management and analytics.
*   **Vector Database**: Built-in vector search (k-NN) for AI embeddings and similarity search.
*   **Text Search**: Transactional, embedded text search engine (BM25).
*   **AI Model Store**: Versioned storage for machine learning models (B-Tree backed).
*   **ACID Compliance**: Full transaction support (Begin, Commit, Rollback) with isolation.
*   **High Performance**: Written in Go with a lightweight C# wrapper (P/Invoke).
*   **Caching**: Integrated Redis-backed L1/L2 caching for speed.
*   **Replication**: Optional Erasure Coding (EC) for fault-tolerant storage across drives.
*   **Multi-Tenancy**: Native support for Cassandra Keyspaces or Directory-based isolation.
*   **Flexible Deployment**: Supports both **Standalone** (local) and **Clustered** (distributed) modes.

## Performance & Big Data Management

SOP is designed for high-throughput, low-latency scenarios, making it suitable for "Big Data" management on commodity hardware.

*   **"Ride-on" Metadata**: By embedding metadata (like `IsDeleted`, `LastUpdated`, `Category`) directly into the Key struct but *excluding* it from the index (using `IndexSpecification`), you can scan millions of keys per second to filter data. This avoids the I/O penalty of fetching the full Value (which might be a large JSON blob or binary file) just to check a status flag.
*   **Direct I/O**: SOP bypasses OS page caches where appropriate to offer consistent, raw disk performance.
*   **Parallelism**: The underlying Go engine utilizes highly concurrent goroutines for managing B-Tree nodes and vector indexes.

## Running the Examples

We provide a comprehensive examples project covering B-Trees, Vector Search, Model Store, and more.

To run the interactive examples suite:

```bash
dotnet run --project bindings/csharp/Sop.Examples/Sop.Examples.csproj
```

The suite includes:
1.  **Basic B-Tree**: CRUD operations.
2.  **Complex Keys**: Composite keys and anonymous type lookups.
3.  **Metadata**: "Ride-on" keys for high-performance updates.
4.  **Paging**: Forward/Backward navigation.
5.  **Vector Search**: Simulated AI/RAG embedding search.
6.  **Model Store**: Large binary object storage.

## Prerequisites

*   **Redis**: Required for caching and transaction coordination (especially in Clustered mode). **Note**: Redis is NOT used for data storage, just for coordination & to offer built-in caching.
*   **Storage**: Local disk space (supports multiple drives/folders).
*   **OS**: macOS (Darwin), Linux, or Windows (AMD64).
*   **.NET SDK**: .NET Core 3.1 or later.

## Installation

1.  **Build the Go Bridge**:
    From the repository root:
    ```bash
    go build -buildmode=c-shared -o bindings/csharp/Sop.Examples/bin/Debug/netcoreapp3.1/libjsondb.dylib ./bindings/main/...
    # Note: Adjust the output path and extension (.so for Linux, .dll for Windows) as needed.
    ```

2.  **Add Reference**:
    Add the `Sop` project to your solution or reference the compiled assembly.

3.  **Native Library**:
    Ensure the compiled `libjsondb` (dylib/so/dll) is in your application's output directory (e.g., `bin/Debug/netX.X/`).

## Quick Start Guide

SOP uses a unified `Database` object to manage all types of stores. All operations are performed within a **Transaction**.

### 1. Initialize Database & Context

First, create a Context and open a Database connection.

```csharp
using Sop;
using System.Collections.Generic;

// Initialize Context
using var ctx = new Context();

// Open Database (Standalone Mode)
var dbOpts = new DatabaseOptions 
{ 
    StoresFolders = new List<string> { "./sop_data" },
    Type = (int)DatabaseType.Standalone
};

var db = new Database(dbOpts);
```

### 2. Start a Transaction

All data operations (Create, Read, Update, Delete) must happen within a transaction.

```csharp
// Begin a transaction
var trans = db.BeginTransaction(ctx);
try
{
    // --- 3. Vector Store (AI) ---
    // Open a Vector Store named "products"
    var vectorStore = db.OpenVectorStore(ctx, "products", trans);
    
    // Upsert a Vector Item
    vectorStore.Upsert(new VectorItem 
    { 
        Id = "prod_101", 
        Vector = new float[] { 0.1f, 0.5f, 0.9f },
        Payload = new Dictionary<string, object> { { "name", "Laptop" }, { "price", 999 } }
    });

    // --- 4. Model Store (AI) ---
    // Open a Model Store named "classifiers"
    var modelStore = db.OpenModelStore(ctx, "classifiers", trans);
    
    // Save a Model (any serializable object)
    modelStore.Save("churn", "v1.0", new { Algorithm = "random_forest", Trees = 100 });

    // --- 5. B-Tree Store (Key-Value) ---
    // Open/Create a B-Tree named "users"
    var btree = db.NewBtree<string, string>(ctx, "users", trans);
    
    // Add a Key-Value pair
    btree.Add(ctx, new Item<string, string>("user_123", "John Doe"));
    
    // Find a value
    if (btree.Find(ctx, "user_123"))
    {
        var items = btree.GetValues(ctx, "user_123");
        Console.WriteLine($"Found User: {items[0].Value}");
    }

    // --- 6. Complex Keys & Index Specification ---
    // Define a composite key structure
    public class EmployeeKey
    {
        public string Region { get; set; }
        public string Department { get; set; }
        public int Id { get; set; }
    }

    // Define Index Specification
    // This enables fast prefix scans (e.g., "Get all employees in US")
    var indexSpec = new IndexSpecification
    {
        IndexFields = new List<IndexFieldSpecification>
        {
            new IndexFieldSpecification { FieldName = "Region", AscendingSortOrder = true },
            new IndexFieldSpecification { FieldName = "Department", AscendingSortOrder = true },
            new IndexFieldSpecification { FieldName = "Id", AscendingSortOrder = true }
        }
    };

    var empOpts = new BtreeOptions("employees") { IndexSpec = indexSpec };
    var employees = db.NewBtree<EmployeeKey, string>(ctx, "employees", trans, empOpts);

    employees.Add(ctx, new Item<EmployeeKey, string>(
        new EmployeeKey { Region = "US", Department = "Sales", Id = 101 }, 
        "Alice"
    ));

    // --- 7. Metadata "Ride-on" Keys (UpdateCurrentKey) ---
    // Efficiently update metadata embedded in the key without fetching/writing the value.
    if (employees.Find(ctx, new EmployeeKey { Region = "US", Department = "Sales", Id = 101 }))
    {
        var currentItem = employees.GetCurrentKey(ctx);
        // Update metadata (e.g. promote employee, change status)
        // Note: In a real scenario, you'd likely have a mutable field in the key.
        // This operation is very fast as it avoids value I/O.
        employees.UpdateCurrentKey(ctx, currentItem);
    }

    // --- 8. Simplified Lookup (Anonymous Types) ---
    // You can search using an anonymous object that matches the key structure.
    // This is useful if you don't have the original Key class definition.
    
    // Open existing B-Tree using 'object' as the key type
    var employeesSimple = db.OpenBtree<object, string>(ctx, "employees", trans);
    
    // Search using an anonymous type
    var searchKey = new { Region = "US", Department = "Sales", Id = 101 };
    
    if (employeesSimple.Find(ctx, searchKey))
    {
        var values = employeesSimple.GetValues(ctx, searchKey);
        Console.WriteLine($"Found Alice using anonymous object: {values[0].Value}");
    }

    // --- 9. Paging Navigation ---
    // Efficiently page through keys (metadata) without fetching values.
    var pagingInfo = new PagingInfo 
    { 
        PageSize = 50, 
        PageOffset = 0 
    };
    
    // Get first page of keys
    var keys = employees.GetKeys(ctx, pagingInfo);
    foreach (var item in keys)
    {
        Console.WriteLine($"Employee: {item.Key.Region}/{item.Key.Department}/{item.Key.Id}");
    }

    // --- 10. Text Search ---
    var idx = db.OpenSearch(ctx, "articles", trans);
    idx.Add("doc1", "The quick brown fox");

    // --- 11. Batched Operations ---
    // Add multiple items in a single call for better performance
    var batchItems = new List<Item<string, string>>
    {
        new Item<string, string>("k1", "v1"),
        new Item<string, string>("k2", "v2")
    };
    btree.Add(ctx, batchItems);

    // Commit the transaction
    trans.Commit();
}
catch
{
    trans.Rollback();
    throw;
}
```

### 3. Querying Data (Read-Only)

```csharp
using var trans = db.BeginTransaction(ctx, mode: 2); // 2 = ForReading
try
{
    // --- Vector Search ---
    var vs = db.OpenVectorStore(ctx, "products", trans);
    var hits = vs.Query(new float[] { 0.1f, 0.5f, 0.8f }, k: 5);
    foreach (var hit in hits)
    {
        Console.WriteLine($"Match: {hit.Id}, Score: {hit.Score}");
    }

    // --- Text Search ---
    var idx = db.OpenSearch(ctx, "articles", trans);
    var results = idx.SearchQuery("fox");
    foreach (var res in results)
    {
        Console.WriteLine($"Doc: {res.DocID}, Score: {res.Score}");
    }

    // --- Model Retrieval ---
    var ms = db.OpenModelStore(ctx, "classifiers", trans);
    var model = ms.Load<dynamic>("churn", "v1.0");
    
    trans.Commit();
}
catch
{
    trans.Rollback();
}
```

## Advanced Configuration

### Logging

Configure the global logger to output to a file or stderr.

```csharp
// Log to a file
Logger.Configure(LogLevel.Debug, "sop.log");

// Log to stderr (default)
Logger.Configure(LogLevel.Info, "");
```

### Redis Connection

Initialize the shared Redis connection for caching and coordination.

```csharp
Redis.Initialize("redis://localhost:6379");

// ... perform operations ...

Redis.Close();
```

### Cassandra Connection

Initialize the shared Cassandra connection for multi-tenant storage.

```csharp
var config = new CassandraConfig
{
    ClusterHosts = new List<string> { "localhost" },
    Keyspace = "sop_test",
    Consistency = 1,
    ReplicationClause = "{'class':'SimpleStrategy', 'replication_factor':1}"
};

Cassandra.Initialize(config);

// ... perform operations ...

Cassandra.Close();
```

### Clustered Database

Create a clustered database with Erasure Coding support.

```csharp
var dbOpts = new DatabaseOptions 
{ 
    StoresFolders = new List<string> { "/mnt/data1", "/mnt/data2" },
    Type = (int)DatabaseType.Clustered,
    ErasureConfig = new Dictionary<string, ErasureCodingConfig>
    {
        { "default", new ErasureCodingConfig { DataShards = 2, ParityShards = 1 } }
    }
};

var db = new Database(dbOpts);
```
