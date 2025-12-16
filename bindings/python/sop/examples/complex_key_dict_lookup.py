import os
import sys
import json
from dataclasses import dataclass

# Add the parent directory to sys.path to import sop
# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sop import Context, Item
from sop.ai import Database, DatabaseType
from sop.database import DatabaseOptions

def main():
    # Path to the DB created by complex_key_demo.py
    db_path = "complex_key_demo_db"
    
    if not os.path.exists(db_path):
        print(f"Error: Database '{db_path}' not found. Please run complex_key_demo.py first.")
        return

    print("Initializing Complex Key Consumer (No Dataclass Definition)...")
    ctx = Context()
    
    # Open the existing database
    db = Database(DatabaseOptions(stores_folders=[db_path], type=DatabaseType.Standalone))

    print("\n--- Searching with Plain Dictionary ---")
    with db.begin_transaction(ctx) as tx:
        # Open the existing B-Tree. 
        # Note: We do NOT pass IndexSpecification here. The backend already knows it.
        employees = db.open_btree(ctx, "employees", tx)

        # Search using a plain dictionary.
        # The keys must match the field names defined in the IndexSpecification 
        # (region, department, employee_id).
        search_key = {
            "region": "US", 
            "department": "Engineering", 
            "employee_id": 101
        }
        
        print(f"Searching for: {search_key}")
        
        if employees.find(ctx, search_key):
            # Fetch the value
            # We pass the same dict as the key to get_values
            items = employees.get_values(ctx, Item(key=search_key))
            
            if items:
                # The value is returned as a dict (since we didn't provide a dataclass to deserialize into)
                user_profile = items[0].value
                print(f"Found Match: {user_profile}")
                print(f"Role: {user_profile.get('role')}")
            else:
                print("Item found but value could not be retrieved.")
        else:
            print("No match found.")

if __name__ == "__main__":
    main()
