# SOP for Rust (sop4rs)

`sop4rs` provides safe, idiomatic Rust bindings for the Scalable Objects Persistence (SOP) engine. It allows Rust applications to leverage SOP's high-performance B-Tree storage, ACID transactions, and Swarm Computing capabilities.

## Features

*   **Safe Wrappers**: Idiomatic Rust structs and traits wrapping the raw C ABI.
*   **Zero-Cost**: Direct FFI calls to the optimized Go core.
*   **Serde Integration**: Seamlessly serialize/deserialize Rust structs to B-Tree values.
*   **RAII**: Automatic resource management for Contexts and Transactions.

## Prerequisites

*   Rust 1.56+
*   `libsop.so` / `libsop.dylib` / `libsop.dll` (built from `bindings/main`)

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
sop = { path = "path/to/sop/bindings/rust" }
```

## Usage

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

## Building and Running Examples

The `build.rs` script expects to find the `jsondb` library in `../main`. Ensure you have built the Go shared library first:

```bash
cd ../main
go build -buildmode=c-shared -o libjsondb.dylib jsondb.main.go
```

To run the basic B-Tree example:

```bash
cargo run --example btree_basic
```

To run the Vector Search AI example:

```bash
cargo run --example vector_search_ai
```

To run the Concurrent Transactions demo:

```bash
cargo run --example concurrent_demo
```

To run the Clustered Concurrent Transactions demo (requires Redis):

```bash
cargo run --example concurrent_demo_clustered
```

To run the B-Tree Paging example:

```bash
cargo run --example btree_paging
```
