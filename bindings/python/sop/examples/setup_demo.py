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
    db_path = "data/setup_demo_db"
    
    # 1. Clean up for demo purposes
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    print(f"Initializing SOP Database Demo at '{db_path}'...")
    ctx = Context()

    # 2. Prepare Constraints/Configuration
    # In a real scenario, this might be a complex configuration with Erasure Coding,
    # Replication, etc.
    # Here we simulate starting with just a path, or a partial config.
    print("Step 1: Create Initial Options (just path)")
    options = DatabaseOptions(
        stores_folders=[db_path],
        type=DatabaseType.Standalone
    )
    
    # 3. Call Setup
    # This persists the configuration to 'dboptions.json' in the db_path.
    # If the database already provided options (like Erasure Config), they are saved.
    # If the database existed, this loads them? No, specific behavior depends on Go impl.
    # But importantly, it RETURNS the full configuration as understood by the backend.
    print("Step 2: Calling Database.setup() ...")
    
    # Saving database options for discoverability in DataManager
    Database.setup(ctx, options)
    
    # 3. Use the Options
    # Now valid options are populated.
    print("Step 3: Creating Database instance with setup options...")
    db = Database(options)

    # 5. Verify functionality
    print("Step 4: Starting Transaction to verify...")
    with db.begin_transaction(ctx) as t:
        print("Transaction started successfully!")
        
    print("Done. Database created and verified.")

if __name__ == "__main__":
    main()
