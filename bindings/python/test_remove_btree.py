import sop
from sop import btree
import os
import shutil

def test_remove_btree():
    # Setup
    data_dir = "/tmp/sop_test_remove_btree"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir)

    try:
        # 1. Initialize Database
        ctx = sop.Context()
        db = sop.Database(sop.DatabaseOptions(stores_folders=[data_dir]))

        # 2. Create a store to delete
        print("Creating 'temp_store'...")
        with db.begin_transaction(ctx) as t:
            bo = btree.BtreeOptions("temp_store")
            store = db.new_btree(ctx, "temp_store", t, bo)
            store.add(ctx, sop.btree.Item(key="foo", value="bar"))
        print("Created 'temp_store' and added data.")

        # 3. Remove the store
        print("Removing 'temp_store'...")
        db.remove_btree(ctx, "temp_store")
        print("Successfully removed 'temp_store'.")

        # 4. Verify removal
        print("Verifying removal...")
        with db.begin_transaction(ctx) as t:
            try:
                store = db.open_btree(ctx, "temp_store", t)
                # If open_btree doesn't raise, we check if it's actually usable or empty?
                # Usually open_btree might succeed if it just checks registry, but if registry entry is gone it should fail.
                # Or it might return a valid object but operations fail.
                # Let's assume open_btree should fail if the store is removed from registry.
                # If it returns, let's try to find the item.
                if store.find(ctx, "foo"):
                    print("Store still exists and data found (unexpected).")
                    exit(1)
                else:
                     print("Store opened but data not found (ambiguous).")
            except Exception as e:
                print(f"Store not found (expected): {e}")

    finally:
        # Cleanup
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)

if __name__ == "__main__":
    test_remove_btree()
