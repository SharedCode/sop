import os
import sys

# Add the parent directory to sys.path to import sop
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sop import Context, Logger, LogLevel
from sop.database import Database, DatabaseOptions, DatabaseType
from sop.transaction import ErasureCodingConfig, RedisCacheConfig
from sop.btree import Item

def main():
    print("--- Erasure Coding Config Demo ---")
    print("Note: This demo requires running Redis instance on localhost.")

    # Tell SOP to turn on logging to std err(console).
    Logger.configure(LogLevel.Info)

    try:
        # Create Clustered Database
        ctx = Context()
        db_path = "data/ec_demo"

        ec = {
            "": ErasureCodingConfig(
                data_shards_count=1,
                parity_shards_count=1,
                base_folder_paths_across_drives=[db_path + "/d1", db_path + "/d2"]
            )
        }

        print(f"Creating Clustered Database at {db_path}...")
        db = Database(DatabaseOptions(
            stores_folders=[db_path],
            type=DatabaseType.Clustered,
            redis_config=RedisCacheConfig(address="localhost:6379"),
            erasure_config=ec
        ))

        print("Starting Transaction...")
        with db.begin_transaction(ctx) as trans:
            print("Creating B-Tree 'cluster_btree'...")
            btree = db.new_btree(ctx, "cluster_btree", trans)
            
            print("Adding item...")
            btree.add(ctx, Item(key="key1", value="value1"))
            
            print("Committing...")
            # Commit happens automatically on exit of context manager
        
        print("Clustered operation successful.")
        # Remove the Btree store to cleanup getting ready for next run.
        db.remove_btree(ctx, "cluster_btree")
        
    except Exception as e:
        print(f"Clustered demo failed (expected if services are not running): {e}")
    
    print("--- End of Erasure Coding Demo ---")

if __name__ == "__main__":
    main()
