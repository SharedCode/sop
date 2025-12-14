# SOP for Rust: Bare Metal Storage for the Systems Guru

**Rust developers demand three things: Control, Safety, and Blazing Speed.**

Today, we are bringing the power of **Scalable Objects Persistence (SOP)** to the Rust ecosystem. If you are building high-performance systems, embedded applications, or distributed services that need ACID-compliant storage without the overhead of a massive database server, SOP for Rust (`sop4rs`) is your new weapon of choice.

## The Promise: Bare Metal Performance

SOP is not just another key-value store. It is a sophisticated storage engine featuring:
- **B-Tree Indexing**: Optimized for range queries and massive datasets.
- **ACID Transactions**: Full commit/rollback support with Two-Phase Commit (2PC).
- **Swarm Computing**: Lock-free concurrent modifications across distributed nodes.
- **Hybrid Caching**: Built-in L1 (Memory) and L2 (Redis) caching for sub-millisecond latency.

With the new Rust bindings, you get direct access to this engine with **zero-cost abstractions**. We've designed the bindings to feel native to Rust—leveraging ownership, lifetimes, and `Result` types—while the battle-tested Go engine handles the heavy lifting of I/O and concurrency.

## Why Rust + SOP?

### 1. System-Level Control
You don't want a garbage collector pausing your critical path? Neither do we. The Rust bindings are a thin, efficient layer. You manage the memory, you control the threads, and SOP handles the data consistency.

### 2. "Swarm" Ready
Building a distributed microservice in Rust? SOP's "Clustered Mode" allows multiple Rust instances to operate on the same dataset simultaneously. No central coordinator, no complex locking logic. Just open a transaction, do your work, and commit. SOP handles the conflicts.

### 3. Idiomatic API
We didn't just port the code; we adapted the soul.
- **Generic Keys/Values**: Use any serializable struct as a key or value.
- **Clean Syntax**: No verbose boilerplate.

## Show Me The Code

Here is how simple it is to spin up a high-performance B-Tree in Rust:

```rust
use sop::{Context, Database, DatabaseOptions, Item};

fn main() {
    let ctx = Context::new();
    
    // Open a database (embedded or clustered)
    let mut options = DatabaseOptions::default();
    options.stores_folders = Some(vec!["./data".to_string()]);
    let db = Database::new(&ctx, options).unwrap();

    // Start a transaction
    let trans = db.begin_transaction(&ctx).unwrap();

    // Create a B-Tree
    let btree = db.new_btree::<String, String>(&ctx, "sys_config", &trans, None).unwrap();

    // Add data (Clean, idiomatic API)
    btree.add(&ctx, "max_connections".to_string(), "10000".to_string()).unwrap();
    btree.add(&ctx, "timeout_ms".to_string(), "500".to_string()).unwrap();

    // Commit
    trans.commit(&ctx).unwrap();
    
    println!("System configuration persisted safely.");
}
```

## The "Low Level" Advantage

For the systems guru, SOP offers knobs and dials that others hide:
- **Custom Indexing**: Define complex compound keys.
- **Streaming**: Handle values larger than memory (BLOBs) efficiently.
- **Pluggable Architecture**: Swap out the backend storage or caching layer.

## Get Started

SOP for Rust is available now. Whether you are writing a CLI tool, a game server, or a high-frequency trading bot, give your data the bare-metal treatment it deserves.

Check out the [examples](./bindings/rust/examples) and start building.

---
*SOP: Scalable Objects Persistence. Now speaking your language.*
