# SOP: A Polyglot Storage Engine for the Modern Stack

In the fragmented world of database technologies, developers often find themselves locking their data into language-specific silos or managing complex drivers for every service in their stack. **Scalable Objects Persistence (SOP)** takes a different approach: a **"Write Once, Run Anywhere"** architecture that delivers high-performance, ACID-compliant B-Tree storage across **Go**, **Java**, **Python**, and **C#**.

## The Architecture: Go at the Core

At the heart of SOP lies a robust, high-concurrency engine written in **Go**. We chose Go for its raw performance, efficient memory management, and superior handling of concurrency via goroutines. This core engine handles:

*   **B-Tree Management**: The heavy lifting of balancing, splitting, and merging nodes.
*   **ACID Transactions**: Two-Phase Commit (2PC) logic, conflict detection, and rollback mechanisms.
*   **Swarm Computing**: The logic that allows distributed, lock-free merging of data.
*   **I/O Optimization**: Direct disk I/O, caching strategies, and Erasure Coding.
*   **Cassandra Integration**: Native support for using Apache Cassandra as the **Registry** backend. SOP complements this by providing the features Cassandra lacks: efficient Blob storage, ACID transactions, and Swarm Computing.

Instead of rewriting this complex logic for every language, SOP compiles this core into a **C-Shared Library** (`libsop.so` on Linux, `libsop.dylib` on macOS, `libsop.dll` on Windows). This shared library exposes a clean, C-compatible ABI (Application Binary Interface) that other languages can consume directly.

## Language Bindings: Native Feel, Native Speed

SOP provides first-class bindings for three major ecosystems, each designed to feel "native" to that language while calling the optimized Go code with near-zero overhead.

### 1. Java (sop4j)
*   **Target Audience**: Enterprise backends, Legacy modernization, Cassandra shops.
*   **Technology**: Uses **JNA (Java Native Access)** to bridge the JVM and the Go library.
*   **Key Features**:
    *   **Cassandra Integration**: Leverages your existing Cassandra infrastructure for the **Registry**, while SOP handles the heavy lifting of data management (Blobs, B-Trees) and provides full ACID compliance.
    *   **Type Safety**: Leverages Java Generics (`BTree<String, Product>`) for compile-time safety.
    *   **Ecosystem**: Integrates seamlessly with Maven and standard Java logging frameworks.

### 2. Python (sop4py)
*   **Target Audience**: Data Scientists, AI/ML Engineers, Scripting.
*   **Technology**: Uses **ctypes** for FFI (Foreign Function Interface).
*   **Key Features**:
    *   **AI-Ready**: Exposes specialized **Vector Stores** and **Model Stores**, making it a perfect local database for RAG (Retrieval-Augmented Generation) applications.
    *   **Data Science Friendly**: Handles complex Python `dataclasses` and integrates well with data processing pipelines.
    *   **Simplicity**: Pythonic context managers (`with db.transaction():`) make ACID transactions effortless.

### 3. C# (sop4cs)
*   **Target Audience**: .NET Core / .NET 5+ developers, Windows environments.
*   **Technology**: Uses **P/Invoke** (Platform Invocation Services).
*   **Key Features**:
    *   **Full Parity**: Offers the complete feature set including Vector Search, Text Search (BM25), and "Ride-on" Metadata.
    *   **LINQ-Friendly**: The API is designed to work naturally with C# collections and LINQ queries.
    *   **Struct Support**: Optimized for C# `struct` keys to minimize GC pressure.

## Unified API Design

One of SOP's greatest strengths is its **Unified API**. A developer moving from Go to Java to Python will find the concepts identical:

*   **Context**: The configuration and environment scope.
*   **StoreRepository**: The factory for opening/creating stores.
*   **Transaction**: The unit of work (Begin, Commit, Rollback).
*   **BTree**: The primary data structure.

**Go:**
```go
trans, _ := sop.NewTransaction(ctx)
trans.Begin()
store.Add(ctx, "key", "value")
trans.Commit(ctx)
```

**Java:**
```java
try (Transaction trans = db.beginTransaction(ctx)) {
    btree.add("key", "value");
    trans.commit();
}
```

**Python:**
```python
with db.transaction() as trans:
    store.add("key", "value")
    # Auto-commit on exit
```

## Zero Impedance Mismatch: POJOs with Raw Power

SOP bridges the gap between your application's objects and disk storage, eliminating the need for complex ORMs like **Entity Framework** or **JPA**. It offers a perfect **impedance match** for your language's native data structures:

*   **Java**: Persist **POJOs** directly using standard serialization.
*   **C#**: Store **Structs** and **Classes** with full type safety.
*   **Python**: Save **Dataclasses** and **Pydantic models** effortlessly.

Unlike simple Key-Value stores, SOP exposes the **full raw power** of the underlying B-Tree engine. You aren't just dumping blobs; you are managing sorted, indexed data with capabilities like **Range Queries**, **Prefix Scans**, and **Composite Keys**. You get the simplicity of working with native objects combined with the performance of a bare-metal storage engine.

## Why This Matters

1.  **Performance Consistency**: Whether you are writing a high-frequency trading bot in C# or a data ingestion script in Python, you get the **same** underlying B-Tree performance.
2.  **Operational Simplicity**: You only need to deploy and monitor one storage engine. The logs, metrics, and on-disk formats are identical regardless of the host language.
3.  **Team Flexibility**: A backend team can write the core services in Go or Java, while the data science team accesses the *exact same data* using Python, without needing complex ETL pipelines or API layers.

SOP isn't just a database; it's a **universal storage layer** that bridges the gap between your application's languages.
