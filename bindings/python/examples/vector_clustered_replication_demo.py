import os
import shutil
import sys
import uuid
from dataclasses import asdict

# Add the parent directory to sys.path to import sop
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sop import call_go, Context
from sop.ai import Database, Item, UsageMode, DBType
from sop.transaction import Transaction, TransactionOptions, TransactionMode, ErasureCodingConfig
from sop.redis import Redis

def main():
    print("Attempting to connect to Redis (localhost:6379)...")
    try:
        Redis.open_connection("redis://localhost:6379")
    except Exception as e:
        print(f"Skipping Clustered Replication Demo: Could not connect to Redis. Error: {e}")
        return

    # Define paths
    active_path = "vec_clus_repl_active"
    passive_path = "vec_clus_repl_passive"
    
    # Define paths for Erasure Coding shards
    shard_paths = [
        "vec_clus_repl_shard_1",
        "vec_clus_repl_shard_2",
        "vec_clus_repl_shard_3",
        "vec_clus_repl_shard_4"
    ]

    # Clean up previous run
    for p in [active_path, passive_path] + shard_paths:
        if os.path.exists(p):
            shutil.rmtree(p)

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
    db = Database(
        ctx,
        storage_path=active_path, 
        db_type=DBType.Clustered, # <--- Clustered Mode
        erasure_config={"": ec_config},
        stores_folders=[active_path, passive_path]
    )
    
    print("\n--- Explicit Transaction (Clustered + Replicated) ---")
    
    trans_opts = TransactionOptions(
        mode=TransactionMode.ForWriting.value,
        max_time=15,
        registry_hash_mod=250,
        stores_folders=[active_path, passive_path],
        erasure_config={"": ec_config}
    )

    try:
        with db.begin_transaction(ctx, options=trans_opts) as trans:
            print("Transaction Started.")
            store = db.open_vector_store(ctx, trans, "demo_store_clus_repl")
            
            item1 = Item(id=str(uuid.uuid4()), vector=[0.9, 0.8, 0.7], payload={"name": "Item 1 (Clustered+Repl)"})
            print(f"Adding Item 1: {item1.payload['name']}")
            store.upsert(ctx, item1)
            
            print("Committing...")
        
        # Verify
        with db.begin_transaction(ctx, options=trans_opts) as trans_read:
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
        Redis.close_connection()
        print("Demo completed.")

if __name__ == "__main__":
    main()
