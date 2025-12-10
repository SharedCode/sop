import sys
import os
import shutil

# Add parent directory to path to import sop
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sop.context import Context
from sop.database import Database, DatabaseOptions, DatabaseType
from sop.transaction import TransactionMode
from sop.btree import Item
from sop.cassandra import Cassandra
from sop.redis import Redis

def main():
    print("--- Cassandra & Redis Demo ---")
    print("Note: This demo requires running Cassandra and Redis instances on localhost.")
    print("Ensure you have created the keyspace in Cassandra:")
    print("CREATE KEYSPACE sop_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")

    # Configuration for a local Cassandra instance
    config = {
        "cluster_hosts": ["localhost"],
        "consistency": 1, # LocalQuorum
        "connection_timeout": 5000,
    }

    try:
        print("Initializing Cassandra connection...")
        Cassandra.initialize(config)
        print("Cassandra initialized successfully.")

        print("Initializing Redis connection...")
        Redis.initialize("redis://localhost:6379")
        print("Redis initialized successfully.")

        # Create Cassandra-backed Database
        ctx = Context()
        db_path = "data/cassandra_demo"
        
        if os.path.exists(db_path):
            shutil.rmtree(db_path)

        print(f"Creating Cassandra-backed Database at {db_path}...")
        # Note: We don't need to specify DatabaseType.Clustered if keyspace is provided, 
        # but explicit is fine too. The C# example removed it, so we can rely on keyspace.
        db = Database(DatabaseOptions(
            stores_folders=[db_path],
            keyspace="sop_test"
        ))

        # 1. Insert
        print("Starting Write Transaction...")
        t1 = db.begin_transaction(ctx)
        
        # Use a unique name or ensure it doesn't exist. 
        # Since we can't easily drop the table in this demo without more code, we'll use a unique name.
        import uuid
        btree_name = f"cassandra_btree_{uuid.uuid4().hex}"
        
        # Optional: Try to remove it first if it exists from a previous run (best effort)
        # But RemoveBtree requires a transaction, and we are in one.
        # Let's just use NewBtree. If it fails, we catch it.
        
        btree = db.new_btree(ctx, btree_name, t1)
        print(f"Adding item 'key1' to {btree_name}...")
        btree.add(ctx, Item(key="key1", value="value1"))
        
        t1.commit(ctx)
        print("Committed.")

        # 2. Read
        print("Starting Read Transaction...")
        t2 = db.begin_transaction(ctx, TransactionMode.ForReading.value)
        
        btree = db.open_btree(ctx, btree_name, t2)
        
        if btree.find(ctx, "key1"):
            items = btree.get_values(ctx, Item(key="key1"))
            if items:
                print(f"Found item: Key={items[0].key}, Value={items[0].value}")
            else:
                print("Item found but value retrieval failed.")
        else:
            print("Item not found!")
        
        t2.commit(ctx)

    except Exception as e:
        print(f"Operation failed: {e}")
    finally:
        print("Closing connections...")
        try:
            Redis.close()
        except:
            pass
        try:
            Cassandra.close()
        except:
            pass
    
    print("--- End of Cassandra Demo ---")

if __name__ == "__main__":
    main()
