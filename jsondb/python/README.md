# SOP for Python (sop4py)

**Scalable Objects Persistence (SOP)** is a high-performance, transactional storage engine for Python, powered by a robust Go backend. It combines the raw speed of direct disk I/O with the reliability of ACID transactions and the flexibility of modern AI data management.

## Key Features

*   **Transactional B-Tree Store**: Unlimited, persistent B-Tree storage for key-value data.
*   **Vector Database**: Built-in vector search (k-NN) for AI embeddings and similarity search.
*   **AI Model Store**: Versioned storage for machine learning models (B-Tree backed).
*   **ACID Compliance**: Full transaction support (Begin, Commit, Rollback) with isolation.
*   **High Performance**: Written in Go with a lightweight Python wrapper (ctypes).
*   **Caching**: Integrated Redis-backed L1/L2 caching for speed.
*   **Replication**: Optional Erasure Coding (EC) for fault-tolerant storage across drives.
*   **Flexible Deployment**: Supports both **Standalone** (local) and **Clustered** (distributed) modes.

## Documentation

*   **[API Cookbook](COOKBOOK.md)**: Common recipes and patterns (Key-Value, Transactions, AI).
*   **[Examples](examples/)**: Complete runnable scripts.

## Prerequisites

*   **Redis**: Required for caching and transaction coordination (especially in Clustered mode). **Note**: Redis is NOT used for data storage, just for coordination & to offer built-in caching.
*   **Storage**: Local disk space (supports multiple drives/folders).
*   **OS**: macOS (Darwin), Linux, or Windows (AMD64).

## Installation

```bash
pip install sop4py
```

*Note: Ensure the `libjsondb` shared library is available in your library path if building from source.*

## Quick Start Guide

### 1. Setup & Configuration

First, import the necessary modules and configure your storage environment.

```python
from sop import Context, Transaction, TransactionOptions, TransactionMode

# Initialize Context
ctx = Context()

# Configure Transaction Options
opts = TransactionOptions(
    mode=TransactionMode.ForWriting.value,
    max_time=15,  # 15 minutes timeout
    registry_hash_mod=250,
    stores_folders=["/tmp/sop_data"],  # Path to store data
    erasure_config={}  # Optional replication config
)
```

### 2. Transactional Key-Value Store (B-Tree)

SOP allows you to manage data using B-Trees within a transaction.

```python
from sop import btree

# Use the Transaction Context Manager
with Transaction(ctx, opts) as t:
    # Create or Open a B-Tree
    # "users" is the store name, True indicates native key type
    bo = btree.BtreeOptions("users", True, cache_config=btree.CacheConfig())
    store = btree.Btree.new(ctx, bo, t)

    # Add Items
    store.add(ctx, [
        btree.Item(key=1, value="Alice"),
        btree.Item(key=2, value="Bob")
    ])

    # Fetch Items
    item = store.find_one(ctx, 1, False)
    print(f"Found: {item.value}")

# Transaction is automatically committed here
```

### 3. Vector Database (AI)

Manage vector embeddings for semantic search. You can choose between **Standalone** (local) and **Clustered** (distributed) modes.

```python
from sop import Context
from sop.ai import VectorDatabase, UsageMode, Item, DBType

# Initialize Context
ctx = Context()

# Initialize Vector DB
# DBType.Standalone: Uses in-memory caching (good for single node).
# DBType.Clustered: Uses Redis for caching (good for distributed setups).
vdb = VectorDatabase(
    ctx,
    storage_path="/tmp/sop_vectors", 
    usage_mode=UsageMode.Dynamic,
    db_type=DBType.Standalone
)
vector_store = vdb.open(ctx, "products")

# Upsert Vectors
vector_store.upsert(ctx, Item(
    id="prod_101",
    vector=[0.1, 0.5, 0.9],
    payload={"name": "Laptop", "price": 999}
))

# Optimize (Optional but recommended after bulk loads)
# Rebalances the index for optimal search performance.
# Note: This performs a full index rebuild. Schedule periodically based on data drift.
vector_store.optimize(ctx)

# Semantic Search (k-NN)
hits = vector_store.query(
    ctx,
    vector=[0.1, 0.5, 0.8],
    k=5,
    filter={"name": "Laptop"}
)

for hit in hits:
    print(f"Match: {hit.id}, Score: {hit.score}")

### 4. Vector Store Usage Modes

SOP provides different modes to optimize for your specific use case:

*   **`UsageMode.BuildOnceQueryMany`**: Best for static datasets (e.g., Wikipedia dump).
    *   **Workflow**: Ingest Data -> Call `optimize()` -> Serve Queries.
    *   **Benefit**: Discards temporary build artifacts to save space and maximize read performance.
*   **`UsageMode.Dynamic`**: Best for general-purpose, changing datasets. Maintains structures for frequent updates.
*   **`UsageMode.DynamicWithVectorCountTracking`**: Advanced mode for Agents.
    *   **Benefit**: Tracks vector counts per cluster, allowing your application to calculate density skew and intelligently decide *when* to trigger optimization (e.g., "only if skew > 5x").

### 5. Explicit Centroid Management

You can bypass automatic clustering by providing a `centroid_id` during upsert. This is useful if your data already has logical groupings (e.g., User ID, Category).

```python
vector_store.upsert(ctx, Item(
    id="prod_102",
    vector=[0.2, 0.6, 0.1],
    payload={"name": "Phone"},
    centroid_id=500  # Explicitly assign to cluster 500
))
```

### 6. AI Model Store
```

### 4. AI Model Store

Version and manage your machine learning models using the B-Tree backend.

```python
from sop import Context
from sop.ai import Database, Model

# Initialize Context
ctx = Context()

# Initialize the Database
db = Database(ctx, storage_path="/tmp/sop_data")

# Open the Model Store
model_store = db.open_model_store(ctx, "my_models")

# Save a Model
model = Model(
    id="churn_v1",
    algorithm="random_forest",
    hyperparameters={"trees": 100},
    parameters=[0.5, 0.1, 0.9],  # Serialized weights
    metrics={"accuracy": 0.98},
    is_active=True
)
model_store.save(ctx, "classifiers", "churn_v1", model)

# Retrieve a Model
loaded_model = model_store.get(ctx, "classifiers", "churn_v1")
print(f"Loaded Model: {loaded_model['model']['id']}")
```

## Examples

The `examples/` directory contains complete, runnable demos showcasing various features of the library.

*   **`vector_demo.py`**: Basic usage of the Vector Store in Standalone mode. Demonstrates auto-commit vs. explicit transactions.
*   **`vector_clustered_demo.py`**: Usage of the Vector Store in Clustered mode with Redis caching.
*   **`modelstore_demo.py`**: Managing AI models (save, load, list, delete) using the B-Tree backend.
*   **`langchain_demo.py`**: Integration with LangChain workflows (mocked adapter example).
*   **`vector_replication_demo.py`**: Advanced usage showing how to configure Erasure Coding (RAID-like redundancy) for vector data.

To run an example (assuming you are in the root of the repo):

```bash
PYTHONPATH=jsondb/python python3 jsondb/python/examples/vector_demo.py
```

## AI Development Workflows

SOP is designed to support the full AI Software Development Life Cycle (SDLC), allowing you to transition seamlessly from local experimentation to distributed production.

### Phase 1: Local Training & Development (Standalone)
Data Scientists and ML Engineers can work in **Standalone Mode** on their local machines. This mode isolates the environment, requiring no external dependencies like Redis.

*   **Action**: Train models and populate vector stores locally.
*   **Configuration**: Use `DBType.Standalone`.
*   **Benefit**: Fast iteration, zero infrastructure overhead, full isolation.

```python
# Local Development
ctx = Context()
vdb = VectorDatabase(ctx, storage_path="./local_vectors", db_type=DBType.Standalone)
# ... train model, generate embeddings, upsert to vdb ...
```

### Phase 2: Production Deployment (Clustered)
Once the model and vector data are stabilized and validated, they can be "promoted" to the production environment.

*   **Action**: Ensure the storage directory (e.g., `./local_vectors`) is accessible to production nodes. You can copy it to a shared volume, or if it's already on a network share, just use it in-place.
*   **Configuration**: Switch the application to `DBType.Clustered`.
*   **Benefit**: The production cluster (using Redis for caching & coordination) now serves the pre-computed data with high availability. **Note**: Data remains on disk (B-Trees) for massive scalability; Redis is NOT used for primary storage.

```python
# Production
ctx = Context()
vdb = VectorDatabase(ctx, storage_path="/mnt/prod_vectors", db_type=DBType.Clustered)
# ... serve queries ...
```

This workflow allows you to treat your AI data (models and vector indices) as artifacts that are built locally and released to production, ensuring consistency and simplifying the deployment pipeline.

## Architecture

SOP uses a split architecture:
1.  **Core Engine (Go)**: Handles disk I/O, B-Tree algorithms, caching, and transactions. Compiled as a shared library (`.dylib`, `.so`, `.dll`).
2.  **Python Wrapper**: Uses `ctypes` to interface with the Go engine, providing a Pythonic API (`sop` package).

## Project Links

*   **Source Code**: [GitHub - sharedcode/sop](https://github.com/sharedcode/sop)
*   **PyPI**: [sop4py](https://pypi.org/project/sop4py)

## Contributing

Contributions are welcome! Please check the `CONTRIBUTING.md` file in the repository for guidelines.
