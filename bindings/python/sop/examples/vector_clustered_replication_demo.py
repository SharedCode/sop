import os
import shutil
import sys
import uuid

# Add the parent directory to sys.path to import sop
# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sop import Context
from sop.ai import Database, Item, DatabaseType
from sop.database import DatabaseOptions
from sop.transaction import TransactionMode, ErasureCodingConfig
from sop.redis import Redis

def main():
    print("Attempting to connect to Redis (localhost:6379)...")
    try:
        Redis.initialize("redis://localhost:6379")
    except Exception as e:
        print(f"Skipping Clustered Replication Demo: Could not connect to Redis. Error: {e}")
        return

    # Define paths
    active_path = os.path.abspath("vec_clus_repl_active")
    passive_path = os.path.abspath("vec_clus_repl_passive")
    
    # Create 4 shard paths for EC
    shard_paths = [os.path.abspath(f"vec_clus_repl_shard_{i}") for i in range(4)]

    # Clean up previous run
    # Try to remove btree properly first if active path exists
    if os.path.exists(active_path):
        try:
            ctx = Context()
            # Redis is already initialized above
            db = Database(DatabaseOptions(stores_folders=[active_path], type=DatabaseType.Clustered))
            db.remove_btree(ctx, "demo_store_clus_repl")
        except:
            pass

    for p in [active_path, passive_path] + shard_paths:
        if os.path.exists(p):
            shutil.rmtree(p)
        os.makedirs(p, exist_ok=True)

    # Clean up Redis to ensure metadata is consistent with filesystem
    try:
        import subprocess
        subprocess.run(["redis-cli", "flushall"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except:
        print("Warning: Could not flush Redis. If you have stale data, the demo might fail.")

    print("Initializing SOP Vector Database (Clustered + Replicated + EC)...")
    
    # Configure Erasure Coding (2 Data + 2 Parity = 4 Shards)
    ec_config = ErasureCodingConfig(
        data_shards_count=2,
        parity_shards_count=2,
        base_folder_paths_across_drives=shard_paths,
        repair_corrupted_shards=False
    )

    ctx = Context()
    
    # Initialize Database with Clustered Type AND Replication Config
    db = Database(DatabaseOptions(
        type=DatabaseType.Clustered, # <--- Clustered Mode
        erasure_config={"": ec_config},
        stores_folders=[active_path, passive_path]
    ))
    
    print("\n--- Explicit Transaction (Clustered + Replicated) ---")
    
    try:
        with db.begin_transaction(ctx, max_time=15) as trans:
            print("Transaction Started.")
            store = db.open_vector_store(ctx, trans, "demo_store_clus_repl")
            
            item1 = Item(id=str(uuid.uuid4()), vector=[0.9, 0.8, 0.7], payload={"name": "Item 1 (Clustered+Repl)"})
            print(f"Adding Item 1: {item1.payload['name']}")
            store.upsert(ctx, item1)
            
            print("Committing...")
        
        # Verify
        with db.begin_transaction(ctx, mode=TransactionMode.ForReading.value, max_time=15) as trans_read:
            store_read = db.open_vector_store(ctx, trans_read, "demo_store_clus_repl")
            fetched = store_read.get(ctx, item1.id)
            print(f"Verified Item 1: {fetched.payload['name']}")
        
        # Check if files exist in both Active and Passive folders
        if os.path.exists(active_path) and os.path.exists(passive_path):
            print("Success: Active and Passive folders created.")
        else:
            print("Error: Replication folders missing.")

        # Check if shards exist
        shards_found = 0
        for p in shard_paths:
            if os.path.exists(p):
                shards_found += 1
        
        if shards_found == 4:
            print("Success: All 4 EC shards created.")
        else:
            print(f"Error: Only {shards_found}/4 shards found.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Clean up
        for p in [active_path, passive_path] + shard_paths:
            if os.path.exists(p):
                shutil.rmtree(p)
        Redis.close()
        print("Demo completed.")

if __name__ == "__main__":
    main()
