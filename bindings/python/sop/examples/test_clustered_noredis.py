import os
import shutil
import sys
import uuid

# Add the parent directory to sys.path to import sop
# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sop import Context
from sop.ai import Database, Item, DatabaseType
from sop.transaction import TransactionMode, DatabaseOptions
from sop.redis import Redis

def main():
    db_path = os.path.abspath("vector_clustered_demo_db")
    
    # Clean up previous run
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    # print("Attempting to connect to Redis (localhost:6379)...")
    # # opts = RedisOptions()
    # try:
    #     Redis.open_connection("redis://localhost:6379")
    # except Exception as e:
    #     print(f"Skipping Clustered Demo: Could not connect to Redis. Error: {e}")
    #     return

    try:
        print(f"Initializing SOP Vector Database (Clustered) at '{db_path}'...")
        ctx = Context()
        # Note: In Clustered mode, we typically use a shared storage path or distributed file system,
        # but for this demo we use a local path.
        db = Database(DatabaseOptions(stores_folders=[db_path], type=DatabaseType.Clustered))

        # --- 1. Explicit Transaction ---
        print("\n--- 1. Explicit Transaction ---")
        # For Clustered, we usually need to specify the stores folders if we want replication,
        # but here we just want to test the Redis caching integration.
        # We use the same path for simplicity.

        with db.begin_transaction(ctx) as trans:
            print("Transaction Started.")
            store = db.open_vector_store(ctx, trans, "demo_store_clustered")
            
            item1 = Item(id=str(uuid.uuid4()), vector=[0.1, 0.2, 0.3], payload={"name": "Item 1 (Clustered)"})
            print(f"Adding Item 1: {item1.payload['name']}")
            store.upsert(ctx, item1)
            
            item2 = Item(id=str(uuid.uuid4()), vector=[0.4, 0.5, 0.6], payload={"name": "Item 2 (Tx Clustered)"})
            print(f"Adding Item 2: {item2.payload['name']}")
            store.upsert(ctx, item2)
            
            print("Committing...")
        
        # Verify in new transaction
        with db.begin_transaction(ctx, mode=TransactionMode.ForReading.value) as trans_read:
            store_read = db.open_vector_store(ctx, trans_read, "demo_store_clustered")
            fetched1 = store_read.get(ctx, item1.id)
            print(f"Verified Item 1: {fetched1.payload['name']}")
            fetched2 = store_read.get(ctx, item2.id)
            print(f"Verified Item 2: {fetched2.payload['name']}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Closing Redis connection...")
        Redis.close()
        
        # Clean up
        if os.path.exists(db_path):
            shutil.rmtree(db_path)
        print("Demo completed.")

if __name__ == "__main__":
    main()
