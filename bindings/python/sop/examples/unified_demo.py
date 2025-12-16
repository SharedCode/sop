import os
import shutil
import sys
import uuid

# Add the parent directory to sys.path to import sop
# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sop import Context, TransactionMode, BtreeOptions, Item
from sop.ai import Database, DatabaseType, Item as VectorItem
from sop.database import DatabaseOptions

def main():
    # Define paths
    general_db_path = "unified_general_db"
    vector_db_path = "unified_vector_db"
    
    # Clean up
    for p in [general_db_path, vector_db_path]:
        if os.path.exists(p):
            shutil.rmtree(p)

    print("Initializing Unified Demo...")
    ctx = Context()

    # 1. Initialize AI Database handle
    # We don't need to "create" the folder structure explicitly for AI DB, 
    # but we need the Database object to open stores.
    ai_db = Database(DatabaseOptions(stores_folders=[vector_db_path], type=DatabaseType.Standalone))

    # 2. Create a Transaction
    # We use a single storage path for simplicity in this demo.
    # To use multiple paths (replication), see vector_replication_demo.py

    print("\n--- Starting Unified Transaction ---")
    with ai_db.begin_transaction(ctx) as trans:
        print("Transaction Started.")

        # --- A. General Purpose B-Tree Operation ---
        print("Opening General B-Tree 'users'...")
        # Note: db.new_btree creates a new store. db.open_btree opens existing.
        # For demo, we use new.
        btree_opts = BtreeOptions(name="users")
        users_store = ai_db.new_btree(ctx, "users", trans, options=btree_opts)
        
        user_id = uuid.uuid4()
        print(f"Adding User to B-Tree: {user_id}")
        users_store.add(ctx, Item(key=str(user_id), value={"name": "Alice", "role": "Engineer"}))

        # --- B. AI Vector Store Operation ---
        print("Opening Vector Store 'user_vectors'...")
        vec_store = ai_db.open_vector_store(ctx, trans, "user_vectors")
        
        print(f"Adding Vector for User: {user_id}")
        vec_item = VectorItem(
            id=str(user_id), 
            vector=[0.1, 0.9, 0.5], 
            payload={"user_id": str(user_id)}
        )
        vec_store.upsert(ctx, vec_item)

        print("Committing Unified Transaction...")

    # 3. Verify Data (Read back)
    print("\n--- Verifying Data ---")
    with ai_db.begin_transaction(ctx, mode=TransactionMode.ForReading.value) as trans:
        # Open B-Tree
        users_store = ai_db.open_btree(ctx, "users", trans)
        found = users_store.find(ctx, str(user_id))
        if found:
            # Fetch value
            # Btree.get_values takes an Item with key populated
            kv = users_store.get_values(ctx, Item(key=str(user_id)))
            # get_values returns a list of Items
            if kv and kv[0].value:
                print(f"Found User in B-Tree: {kv[0].value}")
            else:
                print("User found but value missing!")
        else:
            print("User NOT found in B-Tree!")

        # Open Vector Store
        vec_store = ai_db.open_vector_store(ctx, trans, "user_vectors")
        try:
            vec = vec_store.get(ctx, str(user_id))
            print(f"Found Vector in AI Store: {vec.payload}")
        except Exception as e:
            print(f"Vector NOT found: {e}")

    # Clean up
    for p in [general_db_path, vector_db_path]:
        if os.path.exists(p):
            shutil.rmtree(p)
    print("Unified Demo completed successfully.")

if __name__ == "__main__":
    main()
