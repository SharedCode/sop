import os
import sys
import shutil
import json

# Add the parent directory to sys.path to import sop
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from sop import Context
from sop.ai import Database, DatabaseType
from sop.database import DatabaseOptions

def main():
    db_path = "data/get_options_demo_db"
    
    # 1. Clean up
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    print(f"Initializing SOP Database Demo at '{db_path}'...")
    ctx = Context()

    # 2. Setup Database
    print("Step 1: Setting up database...")
    options = DatabaseOptions(
        stores_folders=[db_path],
        type=DatabaseType.Standalone
    )
    Database.setup(ctx, options)
    
    # 3. Retrieve Options
    print("Step 2: Retrieving database options...")
    retrieved_opts = Database.get_options(ctx, db_path)
    
    if retrieved_opts:
        print("Success! Retrieved Options:")
        print(f"  Type: {retrieved_opts.type}")
        print(f"  Stores Folders: {retrieved_opts.stores_folders}")
    else:
        print("Failed to retrieve options.")
        sys.exit(1)

    # 4. Use Retrieved Options to Open Database
    # This demonstrates the 'Explicit' pattern where we read config before opening.
    print("Step 3: Opening Database using retrieved options...")
    db = Database(retrieved_opts)
    
    with db.begin_transaction(ctx) as t:
        print("Transaction started successfully!")

if __name__ == "__main__":
    main()
