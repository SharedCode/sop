import os
import shutil
import sys
import uuid
from dataclasses import asdict

# Add the parent directory to sys.path to import sop
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sop import call_go
from sop.ai import VectorDatabase, Item, UsageMode, DBType
from sop.transaction import Transaction, TransactionOptions, TransactionMode, ErasureCodingConfig

def main():
    # Open Redis Connection (Required for Replication/Locking)
    call_go.open_redis_connection("redis://localhost:6379")
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

    # Initialize Database with Replication Config
    # Note: We pass the Active path as the primary storage path, but also provide the full config.
    db = VectorDatabase(
        storage_path=active_path, 
        usage_mode=UsageMode.Dynamic, 
        db_type=DBType.Standalone,
        erasure_config=ec_config,
        stores_folders=[active_path, passive_path]
    )
    
    store = db.open("demo_store_repl")

    # --- 1. Explicit Transaction with Replication ---
    print("\n--- 1. Explicit Transaction (Replicated) ---")
    
    trans_opts = TransactionOptions(
        mode=TransactionMode.ForWriting.value,
        max_time=15,
        registry_hash_mod=250,
        stores_folders=[active_path, passive_path],
        erasure_config={"": ec_config} # Default config
    )

    with Transaction(store.ctx, trans_opts) as trans:
        print("Transaction Started.")
        tx_store = store.with_transaction(trans)
        
        item1 = Item(id=str(uuid.uuid4()), vector=[0.1, 0.2, 0.3], payload={"name": "Item 1 (Replicated)"})
        print(f"Adding Item 1: {item1.payload['name']}")
        tx_store.upsert(item1)
        
        print("Committing...")
    
    # Verify
    fetched = store.get(item1.id)
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
