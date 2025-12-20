import os
import shutil
import sys
from sop import Context, Database, DatabaseOptions, DatabaseType, Item

def main():
    # Use a local data directory relative to where the script is run
    db_path = "data/large_simple_db"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)
    
    print(f"Generating Large Simple DB at {db_path}...")
    
    ctx = Context()
    db = Database(DatabaseOptions(stores_folders=[db_path], type=DatabaseType.Standalone))
    
    store_name = "large_simple_tree"
    
    t = db.begin_transaction(ctx)
    store = db.new_btree(ctx, store_name, t)
    
    count = 1000
    print(f"Adding {count} items with Integer keys...")
    
    batch = []
    for i in range(count):
        # Integer Key
        batch.append(Item(key=i, value=f"Value for {i}"))
        
        if len(batch) >= 100:
            store.add(ctx, batch)
            batch = []
            print(f"  Added {i+1}/{count}")
    
    if batch:
        store.add(ctx, batch)
        
    t.commit(ctx)
    
    print("Done. You can now browse this data using 'sop-httpserver'.")
    print(f"Registry Path: {os.path.abspath(db_path)}")

if __name__ == "__main__":
    main()
