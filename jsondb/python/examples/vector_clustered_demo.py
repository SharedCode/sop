import os
import shutil
import sys
import uuid
from dataclasses import asdict

# Add the parent directory to sys.path to import sop
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sop.ai import VectorDatabase, Item, UsageMode, DBType
from sop.transaction import Transaction, TransactionOptions, TransactionMode
from sop.redis import Redis, RedisOptions

def main():
    db_path = "vector_clustered_demo_db"
    
    # Clean up previous run
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    print("Attempting to connect to Redis (localhost:6379)...")
    # opts = RedisOptions()
    try:
        Redis.open_connection("redis://localhost:6379")
    except Exception as e:
        print(f"Skipping Clustered Demo: Could not connect to Redis. Error: {e}")
        return

    try:
        print(f"Initializing SOP Vector Database (Clustered) at '{db_path}'...")
        # Note: In Clustered mode, we typically use a shared storage path or distributed file system,
        # but for this demo we use a local path.
        db = VectorDatabase(storage_path=db_path, usage_mode=UsageMode.Dynamic, db_type=DBType.Clustered)
        store = db.open("demo_store_clustered")

        # --- 1. Auto-Commit ---
        print("\n--- 1. Auto-Commit (Implicit Transaction) ---")
        item1 = Item(id=str(uuid.uuid4()), vector=[0.1, 0.2, 0.3], payload={"name": "Item 1 (Clustered)"})
        print(f"Adding Item 1: {item1.payload['name']}")
        store.upsert(item1)
        
        fetched = store.get(item1.id)
        print(f"Verified Item 1: {fetched.payload['name']}")

        # --- 2. Explicit Transaction ---
        print("\n--- 2. Explicit Transaction ---")
        # For Clustered, we usually need to specify the stores folders if we want replication,
        # but here we just want to test the Redis caching integration.
        # We use the same path for simplicity.
        trans_opts = TransactionOptions(
            mode=TransactionMode.ForWriting.value,
            max_time=15,
            registry_hash_mod=250,
            stores_folders=[db_path], # Standalone-like folder structure but with Redis cache
            erasure_config={}
        )

        with Transaction(store.ctx, trans_opts) as trans:
            print("Transaction Started.")
            tx_store = store.with_transaction(trans)
            
            item2 = Item(id=str(uuid.uuid4()), vector=[0.4, 0.5, 0.6], payload={"name": "Item 2 (Tx Clustered)"})
            print(f"Adding Item 2: {item2.payload['name']}")
            tx_store.upsert(item2)
            
            print("Committing...")
        
        fetched2 = store.get(item2.id)
        print(f"Verified Item 2: {fetched2.payload['name']}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Closing Redis connection...")
        Redis.close_connection()
        
        # Clean up
        if os.path.exists(db_path):
            shutil.rmtree(db_path)
        print("Demo completed.")

if __name__ == "__main__":
    main()
