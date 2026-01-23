# SOP for Python (sop4py) Cookbook

Simple, copy-pasteable examples for common SOP scenarios in Python.

## 1. Storing Structured Data (Key-Value)

This example demonstrates how to store structured data (dictionaries) in a B-Tree using the unified Database API.

```python
import sop
from sop import btree
import json

# 1. Initialize Database
ctx = sop.Context()
db = sop.Database(sop.DatabaseOptions(stores_folders=["/tmp/sop_data"]))

# 2. Run Transaction
with db.begin_transaction(ctx) as t:
    # Open/Create Store "users"
    # BtreeOptions is optional if you just want default settings.
    # You can pass the name directly to new_btree.
    store = db.new_btree(ctx, "users", t)

    # Add Data
    for i in range(1000):
        user_id = f"user_{i}"
        profile = {
            "name": "John Doe",
            "email": f"john{i}@example.com",
            "active": True
        }
        # SOP stores values as strings (JSON recommended for complex objects)
        # Or use native types if supported by your wrapper version
        store.add(ctx, sop.btree.Item(key=user_id, value=json.dumps(profile)))

    print("Added 1000 users.")
```

## 2. Multi-Store Transaction (Bank Transfer)

Atomically update a "Bank Account" and a "Transaction Log" in the same transaction. If any part fails, the entire operation rolls back.

```python
import sop
from sop import btree
import json
import uuid

def transfer_funds(ctx, db, from_id, to_id, amount):
    # Start a transaction on the database
    with db.begin_transaction(ctx) as t:
        # Open both stores in the SAME transaction
        # BtreeOptions is optional, name can be passed directly
        accounts = db.new_btree(ctx, "accounts", t)
        logs = db.new_btree(ctx, "logs", t)

        # 1. Deduct from Sender
        if accounts.find(ctx, from_id):
            sender_item = accounts.get_current_item(ctx)
            sender = json.loads(sender_item.value)
            sender["balance"] -= amount
            accounts.update_current_value(ctx, json.dumps(sender))

        # 2. Add to Receiver
        if accounts.find(ctx, to_id):
            receiver_item = accounts.get_current_item(ctx)
            receiver = json.loads(receiver_item.value)
            receiver["balance"] += amount
            accounts.update_current_value(ctx, json.dumps(receiver))

        # 3. Log the Transfer
        log_entry = {
            "action": "transfer",
            "from": from_id,
            "to": to_id,
            "amount": amount
        }
        logs.add(ctx, sop.btree.Item(key=str(uuid.uuid4()), value=json.dumps(log_entry)))

    # Commit happens automatically on exit of 'with' block
    print("Transfer complete.")

# Usage
ctx = sop.Context()
db = sop.Database(sop.DatabaseOptions(stores_folders=["/tmp/sop_data"]))
transfer_funds(ctx, db, "user_1", "user_2", 100)
```

## 3. Complex Keys & Indexing

SOP supports complex keys (structs/dataclasses) and allows you to define how fields are indexed and sorted. This enables efficient multi-column sorting and prefix scanning.

```python
from dataclasses import dataclass
from sop import Context, BtreeOptions, Item, ValueDataSize, PagingInfo
from sop.btree import IndexSpecification, IndexFieldSpecification
from sop.ai import Database, DatabaseType
from sop.database import DatabaseOptions

# 1. Define Key Structure (Region -> Dept -> ID)
@dataclass
class UserKey:
    region: str
    department: str
    employee_id: int

# 2. Define Value Structure
@dataclass
class UserProfile:
    name: str
    role: str

ctx = Context()
db = Database(DatabaseOptions(stores_folders=["/tmp/sop_complex_db"], type=DatabaseType.Standalone))

with db.begin_transaction(ctx) as t:
    # Configure B-Tree for Complex Keys
    bo = BtreeOptions("employees", is_unique=True)
    bo.is_primitive_key = False 
    bo.set_value_data_size(ValueDataSize.Small)

    # Define Index Hierarchy: Region -> Department -> EmployeeID
    idx_spec = IndexSpecification(
        index_fields=(
            IndexFieldSpecification("region", ascending_sort_order=True),
            IndexFieldSpecification("department", ascending_sort_order=True),
            IndexFieldSpecification("employee_id", ascending_sort_order=True),
        )
    )

    store = db.new_btree(ctx, "employees", t, options=bo, index_spec=idx_spec)

    # Add Data
    k1 = UserKey(region="US", department="Eng", employee_id=101)
    store.add(ctx, Item(key=k1, value=UserProfile(name="Alice", role="Dev")))
    
    k2 = UserKey(region="US", department="Sales", employee_id=202)
    store.add(ctx, Item(key=k2, value=UserProfile(name="Bob", role="Manager")))

    print("Added users.")

# Querying
with db.begin_transaction(ctx) as t:
    store = db.open_btree(ctx, "employees", t)

    # Range Scan: Find all "US" employees
    # Since 'region' is the first index field, we can scan efficiently.
    if store.first(ctx):
        items = store.get_items(ctx, PagingInfo(page_size=100))
        for item in items:
            if item.key['region'] == "US":
                print(f"Found: {item.value['name']} in {item.key['department']}")
```

## 4. Metadata-Carrying Keys (Soft Delete)

Store metadata (like `is_deleted` flags or timestamps) directly in the Key. This allows you to filter records efficiently using `get_keys` without fetching the heavy Value payload from disk.

```python
from dataclasses import dataclass
import time
from sop import Context, BtreeOptions, Item, ValueDataSize, PagingInfo
from sop.btree import IndexSpecification, IndexFieldSpecification
from sop.ai import Database, DatabaseType
from sop.database import DatabaseOptions

# 1. Define Key with "Ride-On" Metadata
# Only 'doc_id' is indexed. 'is_deleted' rides along.
@dataclass
class DocKey:
    doc_id: int
    is_deleted: bool = False
    timestamp: int = 0

ctx = Context()
db = Database(DatabaseOptions(stores_folders=["/tmp/sop_meta_db"], type=DatabaseType.Standalone))

with db.begin_transaction(ctx) as t:
    bo = BtreeOptions("documents", is_unique=True)
    bo.is_primitive_key = False
    # Use Big values (implies we want to avoid fetching them unless necessary)
    bo.set_value_data_size(ValueDataSize.Big)

    # Index ONLY on doc_id
    idx_spec = IndexSpecification(
        index_fields=(
            IndexFieldSpecification("doc_id", ascending_sort_order=True),
        )
    )
    store = db.new_btree(ctx, "documents", t, options=bo, index_spec=idx_spec)

    # Add Data
    k1 = DocKey(doc_id=100, is_deleted=False, timestamp=int(time.time()))
    store.add(ctx, Item(key=k1, value="Large Content A..."))

    k2 = DocKey(doc_id=101, is_deleted=True, timestamp=int(time.time()))
    store.add(ctx, Item(key=k2, value="Large Content B..."))

## 5. Simplified Lookup (Dictionary Keys)

While the Producer application (which creates the store) should use strict Dataclasses and IndexSpecifications to ensure data integrity, Consumer applications can be loosely coupled. You can search for items using plain Python dictionaries as long as the keys match the structure defined in the B-Tree.

```python
import sop
from sop import Context, Item
from sop.ai import Database, DatabaseType
from sop.database import DatabaseOptions

# Initialize Context
ctx = Context()
db = Database(DatabaseOptions(stores_folders=["/tmp/sop_complex_db"], type=DatabaseType.Standalone))

with db.begin_transaction(ctx) as t:
    # Open existing B-Tree
    # Note: No IndexSpecification needed here; it's loaded from the store's metadata.
    store = db.open_btree(ctx, "employees", t)

    # Search using a plain dictionary
    # The dictionary keys must match the field names defined in the IndexSpecification.
    search_key = {
        "region": "US", 
        "department": "Sales", 
        "employee_id": 202
    }
    
    if store.find(ctx, search_key):
        # Fetch the value
        # We pass the same dict as the key to get_values
        items = store.get_values(ctx, Item(key=search_key))
        if items:
            # The value is returned as a dict (since we didn't provide a dataclass)
            user_profile = items[0].value
            print(f"Found: {user_profile['name']}")
```

# Querying Metadata Only
with db.begin_transaction(ctx) as t:
    store = db.open_btree(ctx, "documents", t)
    if store.first(ctx):
        # get_keys fetches ONLY the keys. Values remain on disk.
        keys = store.get_keys(ctx, PagingInfo(page_size=100))
        
        for item in keys:
            status = "[DELETED]" if item.key['is_deleted'] else "[ACTIVE]"
            print(f"Doc {item.key['doc_id']}: {status}")
```

## 5. Vector Database (AI)

Store and search vector embeddings using the unified Database.

```python
import sop
from sop.ai import Database as AIDatabase, Item

ctx = sop.Context()
db = AIDatabase(sop.DatabaseOptions(stores_folders=["/tmp/sop_vectors"]))

with db.begin_transaction(ctx) as tx:
    store = db.open_vector_store(ctx, tx, "products")

    # Upsert
    store.upsert(ctx, Item(
        id="prod_101",
        vector=[0.1, 0.5, 0.9],
        payload={"name": "Laptop", "price": 999}
    ))

    # Search
    hits = store.query(
        ctx,
        vector=[0.1, 0.5, 0.8],
        k=5,
        filter={"name": "Laptop"}
    )

    for hit in hits:
        print(f"Match: {hit.id}, Score: {hit.score}")
```

## 6. AI Model Store

Version and manage machine learning models.

```python
import sop
from sop.ai import Database as AIDatabase, Model

ctx = sop.Context()
db = AIDatabase(ctx, storage_path="/tmp/sop_data")

with db.begin_transaction(ctx) as tx:
    model_store = db.open_model_store(ctx, tx, "my_models")

    # Save Model
    model = Model(
        id="churn_v1",
        algorithm="random_forest",
        parameters=[0.5, 0.1, 0.9],
        metrics={"accuracy": 0.98}
    )
    model_store.save(ctx, "classifiers", "churn_v1", model)

    # Load Model
    loaded = model_store.get(ctx, "classifiers", "churn_v1")
    print(f"Loaded: {loaded['model']['id']}")
```

## 7. Multi-Tenancy (Redis & Cassandra)

Connect to different tenants using isolated Redis databases and Cassandra Keyspaces.

```python
import sop
from sop.database import Database, DatabaseOptions, DatabaseType, RedisCacheConfig

ctx = sop.Context()

# 1. Connect to Tenant A
# Uses Redis DB 0 and Cassandra Keyspace 'tenant_a'
db_a = Database(DatabaseOptions(
    keyspace="tenant_a",
    type=DatabaseType.Clustered,
    redis_config=RedisCacheConfig(url="redis://localhost:6379/0")
))

# 2. Connect to Tenant B
# Uses Redis DB 1 and Cassandra Keyspace 'tenant_b'
db_b = Database(DatabaseOptions(
    keyspace="tenant_b",
    type=DatabaseType.Clustered,
    redis_config=RedisCacheConfig(url="redis://localhost:6379/1")
))

# Operations on db_a are completely isolated from db_b
with db_a.begin_transaction(ctx) as t:
    store = db_a.new_btree(ctx, "config", t)
    store.add(ctx, sop.btree.Item(key="theme", value="dark"))
    # Commit happens automatically

# Note: You do NOT need to call Redis.initialize() globally when using per-database config.
```

## 8. Text Search

Index and search text documents transactionally.

```python
import sop
from sop import database

ctx = sop.Context()
db = database.Database(ctx, storage_path="/tmp/sop_data")

# 1. Index Documents
with db.begin_transaction(ctx) as t:
    idx = db.open_search(ctx, "articles", t)
    idx.add("doc1", "The quick brown fox")
    idx.add("doc2", "jumps over the lazy dog")

# 2. Search
with db.begin_transaction(ctx) as t:
    idx = db.open_search(ctx, "articles", t)
    results = idx.search("fox")
    for r in results:
        print(f"Doc: {r.DocID}, Score: {r.Score}")
```

## 4. Managing Stores (Remove Btree)

You can remove a B-Tree store from the database. This action is permanent and deletes all data associated with the store.

```python
import sop
from sop import btree

# 1. Initialize Database
ctx = sop.Context()
db = sop.Database(sop.DatabaseOptions(stores_folders=["/tmp/sop_data"]))

# 2. Create a store to delete
with db.begin_transaction(ctx) as t:
    bo = btree.BtreeOptions("temp_store")
    store = db.new_btree(ctx, "temp_store", t, bo)
    store.add(ctx, sop.btree.Item(key="foo", value="bar"))
    print("Created 'temp_store' and added data.")

# 3. Remove the store
# Note: RemoveBtree is a database-level operation and does not require an active transaction.
try:
    db.remove_btree(ctx, "temp_store")
    print("Successfully removed 'temp_store'.")
except Exception as e:
    print(f"Failed to remove store: {e}")

# 4. Verify removal
with db.begin_transaction(ctx) as t:
    try:
        store = db.open_btree(ctx, "temp_store", t)
        print("Store still exists (unexpected).")
    except Exception:
        print("Store not found (expected).")
```


## 4. Concurrent Transactions (Swarm Computing)

SOP supports "Swarm Computing" where multiple threads or processes can modify the same B-Tree concurrently without external locks. SOP handles conflict detection and merging automatically.

**Important**: You must pre-seed the B-Tree with at least one item in a separate transaction before launching concurrent workers. This establishes the root node and prevents race conditions during initialization.
> **Note:** This requirement is simply to have at least one item in the tree. It can be a real application item or a dummy seed item.

```python
import threading
import time
import random
from sop import Context, Database, DatabaseOptions, DatabaseType, Item

def worker(thread_id, db, ctx, items_per_thread):
    retry_count = 0
    committed = False
    while not committed and retry_count < 10:
        try:
            # Each thread starts its own transaction
            # No external locks needed!
            with db.begin_transaction(ctx) as t:
                btree = db.open_btree(ctx, "concurrent_tree", t)            
                for j in range(items_per_thread):
                    # Unique keys per thread -> No conflicts, SOP merges changes
                    key = (thread_id * items_per_thread) + j                
                    btree.add(ctx, Item(key=key, value=f"Thread {thread_id} - Item {j}"))
                # Commit happens here
            committed = True
            print(f"Thread {thread_id} committed.")
        except Exception as e:
            retry_count += 1
            time.sleep(random.random() * 0.5) # Backoff

# Usage
ctx = Context()

# Option A: Standalone (Local disk or shared Network drive, In-Memory Cache) - Good for single-node concurrency
db = Database(DatabaseOptions(stores_folders=["/tmp/sop_swarm"], type=DatabaseType.Standalone))

# Option B: Clustered (Redis Cache) - Required for multi-process/distributed swarm
# db = Database(DatabaseOptions(stores_folders=["/tmp/sop_swarm"], type=DatabaseType.Clustered))

# 1. Pre-seed (Required for Swarm)
with db.begin_transaction(ctx) as t:
    btree = db.new_btree(ctx, "concurrent_tree", t)
    btree.add(ctx, Item(key=-1, value="Root Seed"))

# 2. Launch Threads
threads = []
for i in range(5):
    t = threading.Thread(target=worker, args=(i, db, ctx, 100))
    threads.append(t)
    t.start()

for t in threads:
    t.join()
```

## 5. Modeling Relations

SOP uses **Relations Metadata** to define connections between stores. This metadata is sufficient for the Join Tool and AI Agents to navigate most relationships (One-to-One, One-to-Many, Many-to-One).

### 1. Logical Relations (Metadata)
You should always register relations in `BtreeOptions` when a store contains foreign keys. 

```python
from sop.btree import BtreeOptions, Relation

# Register: 'orders.user_id' -> 'users.id'
opts = BtreeOptions(
    relations=[
        Relation(
            source_fields=["user_id"],
            target_store="users",
            target_fields=["id"]
        )
    ]
)
order_store = db.new_btree(ctx, "orders", t, options=opts)
```

**Benefit:**
The metadata allows the system to resolve lookups automatically using the indexed fields on the records.

### 2. Link Stores (Many-to-Many)
For **Many-to-Many** relationships (e.g., Students <-> Classes), you need a dedicated **Link Store** because neither entity holds a unique pointer to the other.

*   **Store: Students**: Key=`StudentID`
*   **Store: Classes**: Key=`ClassID`
*   **Store: Student_Classes**: Key=`StudentID:ClassID`

```python
# Create the Link Store for M:N mapping
with db.begin_transaction(ctx) as t:
    link_store = db.new_btree(ctx, "student_classes", t)
    
    # Add a link (Student 123 -> Class ABC)
    link_store.add(ctx, sop.Item(key="s123:cABC", value=""))
```
