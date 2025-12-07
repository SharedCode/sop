# SOP AI Python Wrapper

This package provides Python bindings for the SOP AI library, enabling you to build Vector Databases and manage AI models using Python.

## Installation

Ensure you have the shared library (`libjsondb.so`, `.dll`, or `.dylib`) built and available in your library path.

## Usage

### Vector Database

You can use the unified `sop.ai.Database` class to manage your Vector Stores.

```python
import sop
from sop.ai import Database
from sop.database import DatabaseOptions
from sop.transaction import ErasureCodingConfig, DatabaseType

# 1. Initialize Context
ctx = sop.Context()

# 2. Initialize Database
# Standalone (Local, No Replication)
db = Database(DatabaseOptions(stores_folders=["./my_vector_db"], type=DatabaseType.Standalone))

# Clustered (Distributed, With Replication)
ec_config = ErasureCodingConfig(
    data_shards_count=2,
    parity_shards_count=1,
    base_folder_paths_across_drives=["/mnt/d1/sop", "/mnt/d2/sop"],
    repair_corrupted_shards=True
)

clustered_db = Database(DatabaseOptions(
    stores_folders=["/mnt/d1/sop", "/mnt/d2/sop"],
    type=DatabaseType.Clustered,
    erasure_config={"default": ec_config}
))

# 3. Open a Store within a Transaction
with db.begin_transaction(ctx) as tx:
    store = db.open_vector_store(ctx, tx, "documents")

    # 4. Upsert Items
    # ...
```

### Replication Support

Replication (Erasure Coding) is **optional** in all modes.

*   **Standalone Mode**: Can be configured with replication, but defaults to single-folder storage.
*   **Clustered Mode**: Can be configured with replication for high availability, or used without it for simple distributed access.
