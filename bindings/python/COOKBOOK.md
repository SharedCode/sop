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
db = sop.Database(sop.DatabaseOptions(storage_path="/tmp/sop_data"))

# 2. Run Transaction
with db.begin_transaction(ctx) as t:
    # Open/Create Store "users"
    bo = btree.BtreeOptions("users")
    store = db.new_btree(ctx, "users", t, bo)

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
        acct_opts = btree.BtreeOptions("accounts")
        accounts = db.new_btree(ctx, "accounts", t, acct_opts)

        log_opts = btree.BtreeOptions("logs")
        logs = db.new_btree(ctx, "logs", t, log_opts)

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
db = sop.Database(sop.DatabaseOptions(storage_path="/tmp/sop_data"))
transfer_funds(ctx, db, "user_1", "user_2", 100)
```

## 3. Vector Database (AI)

Store and search vector embeddings using the unified Database.

```python
import sop
from sop.ai import Database as AIDatabase, Item

ctx = sop.Context()
db = AIDatabase(sop.DatabaseOptions(storage_path="/tmp/sop_vectors"))

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

## 4. AI Model Store

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

## 5. Multi-Tenancy (Cassandra Keyspaces)

Connect to different tenants (Keyspaces) on the same Cassandra cluster using the `CassandraDatabase` helper. This example also initializes Redis for distributed locking.

```python
import sop
from sop import Redis
from sop.database import CassandraDatabase

# 1. Initialize Global Connections (Once per app)
Redis.initialize("redis://localhost:6379/0")
CassandraDatabase.initialize({
    "cluster_hosts": ["127.0.0.1"],
    "keyspace": "sop_global"
})

ctx = sop.Context()

# 2. Connect to Tenant A (Keyspace: 'tenant_a')
db_a = CassandraDatabase(keyspace="tenant_a")

# 3. Connect to Tenant B (Keyspace: 'tenant_b')
db_b = CassandraDatabase(keyspace="tenant_b")

# Operations on db_a are completely isolated from db_b
with db_a.begin_transaction(ctx) as t:
    store = db_a.new_btree(t, "config")
    store.add(ctx, "theme", "dark")
    # Commit happens automatically

# 4. Cleanup on shutdown
Redis.close()
CassandraDatabase.close()
```

## 6. Text Search

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
