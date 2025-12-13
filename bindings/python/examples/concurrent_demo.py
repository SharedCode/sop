import sys
import os
import threading
import time
import random
import logging

# Add parent directory to path to import sop
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sop.context import Context
from sop.database import Database, DatabaseOptions, DatabaseType
from sop.btree import Item
from sop.redis import Redis
from sop import Logger, LogLevel

def worker(ctx, db, store_name, thread_id, items_per_thread):
    # NOTE: No threading.Lock used here.
    # SOP handles transaction isolation and merging.
    
    retry_count = 0
    committed = False
    
    while not committed and retry_count < 10:
        try:
            # Each thread starts its own transaction
            t = db.begin_transaction(ctx)
            btree = db.open_btree(ctx, store_name, t)            
            
            batch = []
            for j in range(items_per_thread):
                # Unique keys per thread -> No conflicts, SOP merges changes
                key = (thread_id * items_per_thread) + j                
                batch.append(Item(key=key, value=f"Thread {thread_id} - Item {j}"))
            
            if not btree.add(ctx, batch):
                raise Exception(f"Thread {thread_id} failed to write batch (add returned False)")

            t.commit(ctx)
            committed = True
            print(f"Thread {thread_id} committed. Btree ID: {btree.id}")
        except Exception as e:
            retry_count += 1
            delay = random.randint(100, 500) / 1000.0 * retry_count
            print(f"Thread {thread_id} conflict detected (Retry {retry_count}): {e}")
            time.sleep(delay)
            
    if not committed:
        print(f"Thread {thread_id} failed after retries.")

def main():
    logging.basicConfig(level=logging.DEBUG)
    print("--- Concurrent Transactions Demo ---")
    print("Demonstrating multi-threaded access without client-side locks.")
    print("SOP handles ACID transactions, conflict detection, and merging.")
    print("This applies to both local threads and distributed processes (Clustered mode).")
    
    db_path = "data/concurrent_demo_py"
    
    store_name = "concurrent_tree"
    Redis.initialize("redis://localhost:6379")
    
    Logger.configure(LogLevel.Warn)
    
    ctx = Context()
    db = Database(DatabaseOptions(
        stores_folders=[db_path],
        type=DatabaseType.Clustered
    ))
    
    # 1. Setup
    # IMPORTANT: Pre-seed the B-Tree with one item to establish the root node.
    # This prevents race conditions on the very first commit when multiple threads 
    # try to initialize an empty tree simultaneously.
    # NOTE: This requirement is simply to have at least one item in the tree.
    # It can be a real application item or a dummy seed item.
    t_setup = db.begin_transaction(ctx)
    btree = db.new_btree(ctx, store_name, t_setup)
    btree.add(ctx, Item(key=-1, value="Root Seed Item"))
    t_setup.commit(ctx)
    
    threads = []
    thread_count = 30
    items_per_thread = 300
    
    print(f"Launching {thread_count} threads, {items_per_thread} items each...")
    
    for i in range(thread_count):
        t = threading.Thread(target=worker, args=(ctx, db, store_name, i, items_per_thread))
        threads.append(t)
        t.start()
        # Jitter to simulate more realistic real life scenario on transactions running in the cluster.
        time.sleep(random.randint(20, 500) / 1000.0)
        
    for t in threads:
        t.join()
        
    print("done")
        
    # Verify
    t_read = db.begin_transaction(ctx)
    btree = db.open_btree(ctx, store_name, t_read)
    print(f"Verify Btree ID: {btree.id}")
    
    count = 0
    if btree.first(ctx):
        while True:
            count += 1
            if not btree.next(ctx):
                break
            
    # Expected count includes the seed item
    expected_count = (thread_count * items_per_thread) + 1
    print(f"Final Count: {count} (Expected: {expected_count})")
    
    if count == expected_count:
        print("SUCCESS: All transactions merged correctly.")
    else:
        print("FAILURE: Count mismatch.")
        
    t_read.commit(ctx)

    db.remove_btree(ctx, store_name)
    Redis.close()

if __name__ == "__main__":
    main()
