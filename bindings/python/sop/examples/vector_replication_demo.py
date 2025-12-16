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
    # Open Redis Connection (Required for Replication/Locking)
    print("Attempting to connect to Redis (localhost:6379)...")
    try:
        Redis.initialize("redis://localhost:6379")
    except Exception as e:
        print(f"Skipping Replication Demo: Could not connect to Redis. Error: {e}")
        return

    # Define paths for Active/Passive replication
    active_path = "vector_repl_active_db"
    passive_path = "vector_repl_passive_db"
    
    # Define paths for Erasure Coding shards (simulated on same drive)
    shard_paths = [
        "vector_repl_shard_1",
        "vector_repl_shard_2",
        "vector_repl_shard_3",
        "vector_repl_shard_4"
    ]

    # Clean up previous run
    for p in [active_path, passive_path] + shard_paths:
        if os.path.exists(p):
            shutil.rmtree(p)

    print("Initializing SOP Vector Database (Replicated + EC)...")
    
    # Configure Erasure Coding (2 Data + 2 Parity = 4 Shards)
    ec_config = ErasureCodingConfig(
        data_shards_count=2,
        parity_shards_count=2,
        base_folder_paths_across_drives=shard_paths,
        repair_corrupted_shards=False
    )

    ctx = Context()
    # Initialize Database with Replication Config
    # Note: We pass the Active path as the primary storage path, but also provide the full config.
    db = Database(DatabaseOptions(
        type=DatabaseType.Standalone,
        erasure_config={"": ec_config}, # Default config
        stores_folders=[active_path, passive_path]
    ))
    
    # --- 1. Explicit Transaction with Replication ---
    print("\n--- 1. Explicit Transaction (Replicated) ---")
    
    # Transaction options are now inherited from DatabaseOptions
    with db.begin_transaction(ctx) as trans:
        print("Transaction Started.")
        store = db.open_vector_store(ctx, trans, "demo_store_repl")
        
        item1 = Item(id=str(uuid.uuid4()), vector=[0.1, 0.2, 0.3], payload={"name": "Item 1 (Replicated)"})
        print(f"Adding Item 1: {item1.payload['name']}")
        store.upsert(ctx, item1)
        
        print("Committing...")
    
    # Verify
    with db.begin_transaction(ctx, mode=TransactionMode.ForReading.value) as trans_read:
        store_read = db.open_vector_store(ctx, trans_read, "demo_store_repl")
        fetched = store_read.get(ctx, item1.id)
        print(f"Verified Item 1: {fetched.payload['name']}")
    
    # Check if files exist in both Active and Passive folders (Basic verification)
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

    # Clean up
    for p in [active_path, passive_path] + shard_paths:
        if os.path.exists(p):
            shutil.rmtree(p)
    print("Demo completed.")

if __name__ == "__main__":
    main()
