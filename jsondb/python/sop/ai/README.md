# SOP AI Python Wrapper

This package provides Python bindings for the SOP AI library, enabling you to build Vector Databases and manage AI models using Python.

## Installation

Ensure you have the shared library (`libjsondb.so`, `.dll`, or `.dylib`) built and available in your library path.

## Usage

### Vector Database

```python
from sop.ai.vector import VectorDatabase, VectorDBOptions, DBType, UsageMode
from sop.transaction import ErasureCodingConfig

# 1. Initialize Vector DB
# Standalone (Local, No Replication)
db = VectorDatabase(storage_path="./my_vector_db", db_type=DBType.Standalone)

# Clustered (Distributed, With Replication)
ec_config = ErasureCodingConfig(
    data_shards_count=2,
    parity_shards_count=1,
    base_folder_paths_across_drives=["/mnt/d1/sop", "/mnt/d2/sop"],
    repair_corrupted_shards=True
)

clustered_db = VectorDatabase(
    storage_path="./my_cluster_db", 
    db_type=DBType.Clustered,
    erasure_config=ec_config,
    stores_folders=["/mnt/d1/sop", "/mnt/d2/sop"]
)

# 2. Open a Store
store = db.open("documents")

# 3. Upsert Items
# ...
```

### Replication Support

Replication (Erasure Coding) is **optional** in all modes.

*   **Standalone Mode**: Can be configured with replication, but defaults to single-folder storage.
*   **Clustered Mode**: Can be configured with replication for high availability, or used without it for simple distributed access.
