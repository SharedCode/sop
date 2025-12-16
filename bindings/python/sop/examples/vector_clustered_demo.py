import os
import shutil
import sys
import uuid

# Add the parent directory to sys.path to import sop
# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sop import Context
from sop.ai import Database, Item, DatabaseType
from sop.database import DatabaseOptions
from sop.transaction import TransactionMode
from sop.redis import Redis

def main():
    db_path = os.path.abspath("vector_clustered_demo_db")
    
    # Clean up previous run
    if os.path.exists(db_path):
        try:
            # Try to clean up properly using SOP API to ensure Redis is also cleaned
            ctx = Context()
            # We need to initialize Redis first for RemoveBtree to work in Clustered mode
            Redis.initialize("redis://localhost:6379")
            db = Database(DatabaseOptions(stores_folders=[db_path], type=DatabaseType.Clustered))
            db.remove_btree(ctx, "demo_store_clustered")
        except:
            # If it fails (e.g. corrupted, or Redis not available yet), ignore and proceed to force delete
            pass
        shutil.rmtree(db_path)
    os.makedirs(db_path, exist_ok=True)

    # Clean up Redis to ensure metadata is consistent with filesystem
    try:
        import subprocess
        subprocess.run(["redis-cli", "flushall"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except:
        print("Warning: Could not flush Redis. If you have stale data, the demo might fail.")

    print("Attempting to connect to Redis (localhost:6379)...")
    # opts = RedisOptions()
    try:
        # Re-initialize if needed (idempotent usually, or check if already init)
        # Redis.initialize("redis://localhost:6379") 
        # Since we might have initialized it above, we can skip or just let it be.
        # But the original code initialized it here.
        pass
    except Exception as e:
        print(f"Skipping Clustered Demo: Could not connect to Redis. Error: {e}")
        return

    # Ensure Redis is initialized for the main logic
    try:
         Redis.initialize("redis://localhost:6379")
    except:
         pass

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
