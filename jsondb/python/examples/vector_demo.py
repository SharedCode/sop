import os
import shutil
import sys
import uuid
from dataclasses import asdict

# Add the parent directory to sys.path to import sop
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sop.ai import VectorDatabase, Item, UsageMode, DBType
from sop.transaction import Transaction, TransactionOptions, TransactionMode

def main():
    db_path = "vector_demo_db"
    
    # Clean up previous run
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    print(f"Initializing SOP Vector Database at '{db_path}'...")
    db = VectorDatabase(storage_path=db_path, usage_mode=UsageMode.Dynamic, db_type=DBType.Standalone)
    store = db.open("demo_store")

    # --- 1. Auto-Commit (Implicit Transaction) ---
    print("\n--- 1. Auto-Commit (Implicit Transaction) ---")
    item1 = Item(id=str(uuid.uuid4()), vector=[0.1, 0.2, 0.3], payload={"name": "Item 1 (Auto)"})
    print(f"Adding Item 1: {item1.payload['name']}")
    store.upsert(item1)
    
    # Verify
    fetched = store.get(item1.id)
    print(f"Verified Item 1: {fetched.payload['name']}")


    # --- 2. Explicit Transaction (Commit) ---
    print("\n--- 2. Explicit Transaction (Commit) ---")
    
    # Create Transaction Options
    trans_opts = TransactionOptions(
        mode=TransactionMode.ForWriting.value,
        max_time=15,
        registry_hash_mod=250,
        stores_folders=[db_path],
        erasure_config={}
    )

    # Start Transaction
    with Transaction(store.ctx, trans_opts) as trans:
        print("Transaction Started.")
        
        # Get a transactional view of the store
        tx_store = store.with_transaction(trans)
        
        item2 = Item(id=str(uuid.uuid4()), vector=[0.4, 0.5, 0.6], payload={"name": "Item 2 (Tx)"})
        print(f"Adding Item 2 inside transaction: {item2.payload['name']}")
        tx_store.upsert(item2)

        # Verify visibility outside transaction (Isolation)
        # Note: In Read Committed, this should NOT be visible yet.
        try:
            outside = store.get(item2.id)
            print(f"Outside Tx: Found {outside.payload['name']} (Unexpected!)")
        except:
            print("Outside Tx: Not Found (Expected - Isolation)")

        print("Committing Transaction...")
    
    # Verify after commit
    try:
        after = store.get(item2.id)
        print(f"After Commit: Found {after.payload['name']}")
    except:
        print("After Commit: Not Found (Error)")


    # --- 3. Explicit Transaction (Rollback) ---
    print("\n--- 3. Explicit Transaction (Rollback) ---")
    
    try:
        with Transaction(store.ctx, trans_opts) as trans:
            print("Transaction Started.")
            tx_store = store.with_transaction(trans)
            
            item3 = Item(id=str(uuid.uuid4()), vector=[0.7, 0.8, 0.9], payload={"name": "Item 3 (Rollback)"})
            print(f"Adding Item 3 inside transaction: {item3.payload['name']}")
            tx_store.upsert(item3)
            
            print("Simulating Error to trigger Rollback...")
            raise Exception("Something went wrong!")
            
    except Exception as e:
        print(f"Caught Exception: {e}")
        print("Transaction should have rolled back.")

    # Verify after rollback
    try:
        store.get(item3.id)
        print("After Rollback: Found Item 3 (Error - Should be gone)")
    except:
        print("After Rollback: Item 3 Not Found (Success)")

    # Clean up
    if os.path.exists(db_path):
        shutil.rmtree(db_path)
    print("\nDemo completed successfully.")

if __name__ == "__main__":
    main()
