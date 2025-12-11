import threading
import time
import uuid
import sys
import os

# Ensure we can import the sop package
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import sop
from sop.transaction import DatabaseType
from sop.btree import Item

def worker(thread_id, num_items, db, btree_name):
    print(f"Thread {thread_id}: Starting")
    ctx = sop.Context()
    
    try:
        # Begin Transaction
        trans = db.begin_transaction(ctx, sop.TransactionMode.ForWriting.value)
        
        # Open Btree (assuming it's already created)
        store = db.open_btree(ctx, btree_name, trans)
        
        # Add Items
        for i in range(num_items):
            key = f"key_{thread_id}_{i}"
            value = f"value_{thread_id}_{i}"
            store.add(ctx, Item(key=key, value=value))
            
        # Commit
        trans.commit(ctx)
        print(f"Thread {thread_id}: Committed {num_items} items")
        
    except Exception as e:
        print(f"Thread {thread_id}: Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    num_threads = 5
    num_items = 100
    btree_name = "shared_btree"
    
    print("Initializing Database...")
    db_opts = sop.DatabaseOptions(
        type=DatabaseType.Standalone,
        stores_folders=["/tmp/sop_python_concurrency_test"]
    )
    db = sop.Database(db_opts)
    
    # Setup: Create the Btree first
    ctx = sop.Context()
    trans = db.begin_transaction(ctx, sop.TransactionMode.ForWriting.value)
    db.new_btree(ctx, btree_name, trans)
    trans.commit(ctx)
    print(f"Created shared Btree: {btree_name}")
    
    threads = []
    start_time = time.time()
    
    for i in range(num_threads):
        t = threading.Thread(target=worker, args=(i, num_items, db, btree_name))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    end_time = time.time()
    print(f"All threads completed in {end_time - start_time:.2f} seconds")
    
    # Verification
    print("Verifying data...")
    trans = db.begin_transaction(ctx, sop.TransactionMode.ForReading.value)
    store = db.open_btree(ctx, btree_name, trans)
    
    missing_count = 0
    for i in range(num_threads):
        for j in range(num_items):
            key = f"key_{i}_{j}"
            if not store.find(ctx, key):
                print(f"Missing key: {key}")
                missing_count += 1
    
    trans.commit(ctx)
    
    if missing_count == 0:
        print("SUCCESS: All items found.")
    else:
        print(f"FAILURE: {missing_count} items missing.")

if __name__ == "__main__":
    main()
