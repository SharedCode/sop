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
use sop::{SopContext, Database, DatabaseOptions};

fn main() -> Result<(), String> {
    let ctx = SopContext::new();
    
    let options = DatabaseOptions::default();
    let db = Database::new(&ctx, options)?;
    
    let trans = db.begin_transaction(&ctx)?;
    
    // ... perform operations ...
    
    trans.commit(&ctx)?;
    Ok(())
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
