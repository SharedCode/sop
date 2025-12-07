---
title: I built a Transactional NoSQL Database in Go (and now it has AI Agents!)
published: false
description: Introducing SOP V2: A high-performance, ACID-compliant storage engine library for Go and Python.
tags: go, database, opensource, ai
cover_image: https://raw.githubusercontent.com/SharedCode/sop/master/doc/logo_banner.png
---

I am thrilled to announce the General Public Availability (GPA) of **Scalable Objects Persistence (SOP) V2**, a native Golang code library that embeds a high-performance, ACID-compliant storage engine directly into your application.

SOP is not just another wrapper around an existing database. It is a **raw storage engine** built from the ground up, bringing the power of B-Tree indexing, transaction management, and distributed coordination directly to your fingertips.

## Why I Built SOP: The "Database-as-a-Library" Paradigm

In traditional architectures, the database is a monolithic external service. Your application talks to it over the wire, dealing with latency, serialization overhead, and "impedance mismatch."

SOP changes the game by turning your application cluster *into* the database cluster.

*   **No External Dependencies (Standalone Mode)**: Build powerful desktop apps, CLI tools, or local AI models that store data in robust B-Trees on the local filesystem. No Docker containers or DB processes required.
*   **Cluster Native**: In distributed mode, SOP uses Redis for lightweight coordination (locking) and the filesystem for storage. This turns your microservices into a masterless, horizontally scalable storage cluster.

## Key Features

### 1. Raw Performance & Control
SOP gives you direct access to the storage muscle.
*   **B-Tree Indexing**: A custom, modernized B-Tree implementation optimized for high node utilization (62%-75%+).
*   **Direct I/O**: Bypasses layers of abstraction to talk directly to the disk.
*   **"NoCheck" Mode**: For build-once-read-many scenarios, skip conflict checks entirely for raw, unbridled read speed.

### 2. Full ACID Transactions
Don't compromise on data integrity. SOP provides full **ACID** (Atomicity, Consistency, Isolation, Durability) support with **Two-Phase Commit (2PC)**. You can seamlessly integrate SOP transactions with other data sources in your ecosystem.

### 3. AI & Vector Search Ready 🤖
SOP V2 is uniquely positioned for the AI era.
*   **Partitioned Vector Search**: Store high-dimensional vectors directly in B-Trees. By partitioning data (e.g., by UserID or DocumentID), SOP outperforms specialized vector stores for hybrid search workloads.
*   **Streaming Data**: Handle multi-gigabyte objects (video, large models) with the `StreamingDataStore`, supporting chunked uploads, downloads, and partial updates within ACID transactions.

## Show Me The Code!

### Go Example: Storing Users
```go
import (
    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/database"
)

// 1. Initialize (Standalone or Clustered)
db := database.NewDatabase(sop.DatabaseOptions{
    Type:          sop.Standalone,
    StoresFolders: []string{"/var/lib/sop"},
})

// 2. Start a Transaction
tx, _ := db.BeginTransaction(ctx, sop.ForWriting)

// 3. Open a Store
users, _ := db.NewBtree(ctx, "users", tx)

// 4. Add Data
users.Add(ctx, "user1", User{Name: "John Doe", Email: "john@example.com"})

// 5. Commit
tx.Commit(ctx)
```

### Python Example: AI Vector Search
We recently released **sop4py**, allowing you to use SOP's power in Python for AI workflows.

```python
from sop.ai import Database, DatabaseType
from sop.database import DatabaseOptions

# 1. Initialize
db = Database(DatabaseOptions(stores_folders=["data/my_db"], type=DatabaseType.Standalone))

with db.begin_transaction(ctx) as tx:
    # 2. Open Vector Store
    vs = db.open_vector_store(ctx, tx, "products")
    
    # 3. Search for similar items
    results = vs.search(ctx, query_vector, limit=5)
    
    for item in results:
        print(f"Found: {item.id} (Score: {item.score})")
```

## The "Moat": Swarm Computing & Auto-Merge
SOP introduces a capability unmatched by traditional RDBMS: **Automatic Transaction Commit Merging**.

In a distributed cluster, multiple machines can commit transactions concurrently. SOP automatically detects non-conflicting changes and merges them into a single cohesive state. This makes parallel programming trivial—transactions are natively thread-safe and machine-safe.

## Get Involved!

SOP is open source and we are just getting started.

*   **GitHub Repo**: [github.com/SharedCode/sop](https://github.com/SharedCode/sop)
*   **Python Package**: `pip install sop4py`
*   **Discussions**: Join us on GitHub Discussions!

I'd love to hear your thoughts on this architecture. Does "Database-as-a-Library" fit your use case? Let me know in the comments! 👇
