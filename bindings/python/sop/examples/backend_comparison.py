import sys
import os

# Add parent directory to path to import sop
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sop.context import Context
from sop.database import Database, DatabaseType, DatabaseOptions
from sop.transaction import TransactionMode
from sop.btree import Item
from sop.cassandra import Cassandra
from sop.redis import Redis

def main():
    ctx = Context()
    
    # ---------------------------------------------------------
    # Scenario 1: Standalone (infs)
    # ---------------------------------------------------------
    # Uses local filesystem for EVERYTHING (Registry, Nodes, Blobs).
    # No Redis, No Cassandra required.
    # Data is stored in the specified storage_path.
    
    print("--- Scenario 1: Standalone (infs) ---")
    path_standalone = "/tmp/sop_data/standalone"
    
    # Clean up previous run
    import shutil
    if os.path.exists(path_standalone):
        shutil.rmtree(path_standalone)
    
    print(f"Initializing Standalone Database at {path_standalone}...")
    # Note: keyspace is NOT provided.
    db_standalone = Database(DatabaseOptions(stores_folders=[path_standalone], type=DatabaseType.Standalone))
    
    print("Starting transaction...")
    t1 = db_standalone.begin_transaction(ctx)
    
    print("Creating B-Tree 'local_store'...")
    store1 = db_standalone.new_btree(ctx, "local_store", t1)
    store1.add(ctx, Item(key="key1", value="value_local"))
    
    t1.commit(ctx)
    print("Standalone transaction committed.")
    print(f"Check {path_standalone} to see the created files (Registry, Nodes, Blobs).")
    print("")

    # ---------------------------------------------------------
    # Scenario 2: Clustered (incfs) with Keyspace
    # ---------------------------------------------------------
    # Uses Cassandra for Registry.
    # Uses Redis for Caching/Locking.
    # Uses local filesystem (or shared mount) for Blobs.
    # Data is partitioned by 'keyspace' in Cassandra.
    
    print("--- Scenario 2: Clustered (incfs) with Keyspace ---")
    path_clustered = "/tmp/sop_data/clustered_blobs"
    keyspace_name = "db1"
    
    # Clean up previous run
    if os.path.exists(path_clustered):
        shutil.rmtree(path_clustered)

    print(f"Initializing Clustered Database (Keyspace: {keyspace_name})...")
    print(f"Blobs will be stored at {path_clustered}...")
    
    # Note: keyspace IS provided.
    # Requires Cassandra and Redis running.
    try:
        # Initialize connections (assuming localhost)
        Cassandra.initialize({"cluster_hosts": ["localhost"], "consistency": 1})
        Redis.initialize("redis://localhost:6379")

        # We can omit type=DatabaseType.Clustered because keyspace implies it.
        db_clustered = Database(DatabaseOptions(
            keyspace=keyspace_name, 
            stores_folders=[path_clustered],
            type=DatabaseType.Clustered
        ))
        
        print("Starting transaction...")
        t2 = db_clustered.begin_transaction(ctx)
        
        print(f"Creating B-Tree 'cluster_store' in keyspace '{keyspace_name}'...")
        store2 = db_clustered.new_btree(ctx, "cluster_store", t2)
        store2.add(ctx, Item(key="key1", value="value_cluster"))
        
        t2.commit(ctx)
        print("Clustered transaction committed.")
        print(f"Check {path_clustered} to see the Blob files.")
        print(f"Check Cassandra keyspace '{keyspace_name}' to see the Registry tables.")
        
        Cassandra.close()
        Redis.close()
        
    except Exception as e:
        print(f"Skipping Clustered test (requires Cassandra/Redis): {e}")
        try:
            Cassandra.close()
            Redis.close()
        except:
            pass

if __name__ == "__main__":
    main()
