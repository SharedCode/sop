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
