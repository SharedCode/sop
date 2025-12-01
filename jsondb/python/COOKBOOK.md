# SOP for Python (sop4py) Cookbook

Simple, copy-pasteable examples for common SOP scenarios in Python.

## 1. Storing Structured Data (Key-Value)

This example demonstrates how to store structured data (dictionaries) in a B-Tree.

```python
from sop import Context, Transaction, TransactionOptions, TransactionMode, btree
import json

# 1. Initialize Context & Options
ctx = Context()
opts = TransactionOptions(
    mode=TransactionMode.ForWriting.value,
    stores_folders=["/tmp/sop_data"]
)

# 2. Run Transaction
with Transaction(ctx, opts) as t:
    # Open/Create Store "users"
    # is_unique=True, is_value_data_globally_unique=False
    bo = btree.BtreeOptions("users", True, cache_config=btree.CacheConfig())
    store = btree.Btree.new(ctx, bo, t)

    # Add Data
    for i in range(1000):
        user_id = f"user_{i}"
        profile = {
            "name": "John Doe",
            "email": f"john{i}@example.com",
            "active": True
        }
        # SOP stores values as strings (JSON recommended for complex objects)
        store.add(ctx, user_id, json.dumps(profile))

    print("Added 1000 users.")
```

## 2. Multi-Store Transaction (Bank Transfer)

Atomically update a "Bank Account" and a "Transaction Log" in the same transaction. If any part fails, the entire operation rolls back.

```python
from sop import Context, Transaction, TransactionOptions, TransactionMode, btree
import json
import uuid

def transfer_funds(ctx, from_id, to_id, amount):
    opts = TransactionOptions(
        mode=TransactionMode.ForWriting.value,
        stores_folders=["/tmp/sop_data"]
    )

    with Transaction(ctx, opts) as t:
        # Open both stores in the SAME transaction
        acct_opts = btree.BtreeOptions("accounts", True, cache_config=btree.CacheConfig())
        accounts = btree.Btree.new(ctx, acct_opts, t)

        log_opts = btree.BtreeOptions("logs", True, cache_config=btree.CacheConfig())
        logs = btree.Btree.new(ctx, log_opts, t)

        # 1. Deduct from Sender
        if accounts.find_one(ctx, from_id, False):
            sender_json = accounts.get_current_value(ctx)
            sender = json.loads(sender_json)
            sender["balance"] -= amount
            accounts.update_current_item(ctx, json.dumps(sender))

        # 2. Add to Receiver
        if accounts.find_one(ctx, to_id, False):
            receiver_json = accounts.get_current_value(ctx)
            receiver = json.loads(receiver_json)
            receiver["balance"] += amount
            accounts.update_current_item(ctx, json.dumps(receiver))

        # 3. Log the Transfer
        log_entry = {
            "action": "transfer",
            "from": from_id,
            "to": to_id,
            "amount": amount
        }
        logs.add(ctx, str(uuid.uuid4()), json.dumps(log_entry))

    # Commit happens automatically on exit of 'with' block
    print("Transfer complete.")

```

## 3. Vector Database (AI)

Store and search vector embeddings.

```python
from sop import Context
from sop.ai import VectorDatabase, UsageMode, Item, DBType

ctx = Context()

# Initialize Vector DB (Standalone for local, Clustered for Redis-backed)
vdb = VectorDatabase(
    ctx,
    storage_path="/tmp/sop_vectors", 
    usage_mode=UsageMode.Dynamic,
    db_type=DBType.Standalone
)
store = vdb.open(ctx, "products")

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
from sop import Context
from sop.ai import Database, Model

ctx = Context()
db = Database(ctx, storage_path="/tmp/sop_data")
model_store = db.open_model_store(ctx, "my_models")

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
