import sys
import os
import shutil
import uuid

# Add parent directory to path to import sop
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sop.context import Context
from sop.database import Database, DatabaseOptions, DatabaseType
from sop.cassandra import Cassandra
from sop.transaction import TransactionMode, RedisCacheConfig
from sop import Item

def main():
    # Initialize Global Connections
    # print("Initializing Redis Connection...")
    # Redis.initialize("redis://localhost:6379/0")

    print("Initializing Cassandra Connection...")
    Cassandra.initialize({
        "cluster_hosts": ["localhost"],
        "consistency": 1
    })

    store_name = f"shared_store_{uuid.uuid4().hex}"
    print(f"Using Store Name: {store_name}")

    try:
        ctx = Context()
        
        # Tenant 1
        print("Connecting to Tenant 1 (Keyspace: db1)...")
        # Note: Ensure you have a Redis instance running and Cassandra keyspaces 'db1' and 'db2' created.
        # CREATE KEYSPACE db1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        # CREATE KEYSPACE db2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        
        path1 = "data/tenant1"
        if os.path.exists(path1):
            shutil.rmtree(path1)

        db1 = Database(DatabaseOptions(
            type=DatabaseType.Clustered,
            keyspace="db1",
            redis_config=RedisCacheConfig(url="redis://localhost:6379/0"),
            stores_folders=[path1]
        ))
        
        print("Tenant 1: Starting transaction...")
        t1 = db1.begin_transaction(ctx)
        store1 = db1.new_btree(ctx, store_name, t1)
        store1.add(ctx, Item(key="key1", value="value_for_tenant_1"))
        t1.commit(ctx)
        print("Tenant 1: Data committed.")

        # Tenant 2
        print("Connecting to Tenant 2 (Keyspace: db2)...")
        path2 = "data/tenant2"
        if os.path.exists(path2):
            shutil.rmtree(path2)

        db2 = Database(DatabaseOptions(
            type=DatabaseType.Clustered,
            keyspace="db2",
            redis_config=RedisCacheConfig(url="redis://localhost:6379/0"),
            stores_folders=[path2]
        ))
        
        print("Tenant 2: Starting transaction...")
        t2 = db2.begin_transaction(ctx)
        store2 = db2.new_btree(ctx, store_name, t2)
        store2.add(ctx, Item(key="key1", value="value_for_tenant_2"))
        t2.commit(ctx)
        print("Tenant 2: Data committed.")

        # Verify Isolation
        print("Verifying Tenant 1 data...")
        t1b = db1.begin_transaction(ctx, TransactionMode.ForReading.value)
        store1b = db1.open_btree(ctx, store_name, t1b)
        found = store1b.find(ctx, "key1")
        if found:
            items = store1b.get_values(ctx, Item(key="key1"))
            val1 = items[0].value if items else None
            if val1 == "value_for_tenant_1":
                print(f"Success: Tenant 1 sees '{val1}'")
            else:
                print(f"Failure: Tenant 1 saw '{val1}'")
        else:
            print("Failure: Tenant 1 key not found")
        t1b.commit(ctx)

        print("Verifying Tenant 2 data...")
        t2b = db2.begin_transaction(ctx, TransactionMode.ForReading.value)
        store2b = db2.open_btree(ctx, store_name, t2b)
        found = store2b.find(ctx, "key1")
        if found:
            items = store2b.get_values(ctx, Item(key="key1"))
            val2 = items[0].value if items else None
            if val2 == "value_for_tenant_2":
                print(f"Success: Tenant 2 sees '{val2}'")
            else:
                print(f"Failure: Tenant 2 saw '{val2}'")
        else:
            print("Failure: Tenant 2 key not found")
        t2b.commit(ctx)

    finally:
        print("Closing Cassandra Connection...")
        Cassandra.close()

if __name__ == "__main__":
    main()
