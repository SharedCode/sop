import os
import shutil
import sys
import uuid

# Add the parent directory to sys.path to import sop
# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sop import Context
from sop.ai import Database, Item, DatabaseType
from sop.database import DatabaseOptions
from sop.transaction import TransactionMode

def main():
    db_path = "vector_demo_db"
    
    # Clean up previous run
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    print(f"Initializing SOP Vector Database at '{db_path}'...")
    ctx = Context()
    db = Database(DatabaseOptions(stores_folders=[db_path], type=DatabaseType.Standalone))

    # --- 1. Explicit Transaction (Commit) ---
    print("\n--- 1. Explicit Transaction (Commit) ---")
    
    # Create Transaction Options
    # Start Transaction
    with db.begin_transaction(ctx) as trans:
        print("Transaction Started.")
        
        # Open store within transaction
        store = db.open_vector_store(ctx, trans, "demo_store")
        
        item1 = Item(id=str(uuid.uuid4()), vector=[0.1, 0.2, 0.3], payload={"name": "Item 1"})
        print(f"Adding Item 1: {item1.payload['name']}")
        store.upsert(ctx, item1)
        
        item2 = Item(id=str(uuid.uuid4()), vector=[0.4, 0.5, 0.6], payload={"name": "Item 2"})
        print(f"Adding Item 2: {item2.payload['name']}")
        store.upsert(ctx, item2)

        # Verify visibility inside transaction
        fetched = store.get(ctx, item1.id)
        print(f"Inside Tx: Found {fetched.payload['name']}")

        print("Committing Transaction...")
    
    # Verify after commit (Need new transaction to read)
    print("Verifying after commit...")
    with db.begin_transaction(ctx, mode=TransactionMode.ForReading.value, max_time=15) as trans_read:
        store_read = db.open_vector_store(ctx, trans_read, "demo_store")
        try:
            after = store_read.get(ctx, item2.id)
            print(f"After Commit: Found {after.payload['name']}")
        except:
            print("After Commit: Not Found (Error)")


    # --- 2. Explicit Transaction (Rollback) ---
    print("\n--- 2. Explicit Transaction (Rollback) ---")
    
    try:
        with db.begin_transaction(ctx, max_time=15) as trans:
            print("Transaction Started.")
            store_rb = db.open_vector_store(ctx, trans, "demo_store")
            
            item3 = Item(id=str(uuid.uuid4()), vector=[0.7, 0.8, 0.9], payload={"name": "Item 3 (Rollback)"})
            print(f"Adding Item 3 inside transaction: {item3.payload['name']}")
            store_rb.upsert(ctx, item3)
            
            print("Simulating Error to trigger Rollback...")
            raise Exception("Something went wrong!")
            
    except Exception as e:
        print(f"Caught Exception: {e}")
        print("Transaction should have rolled back.")

    # Verify after rollback
    with db.begin_transaction(ctx, mode=TransactionMode.ForReading.value, max_time=15) as trans_check:
        store_check = db.open_vector_store(ctx, trans_check, "demo_store")
        try:
            store_check.get(ctx, item3.id)
            print("After Rollback: Found Item 3 (Error - Should be gone)")
        except:
            print("After Rollback: Item 3 Not Found (Success)")

    # Clean up
    if os.path.exists(db_path):
        shutil.rmtree(db_path)
    print("\nDemo completed successfully.")

if __name__ == "__main__":
    main()
