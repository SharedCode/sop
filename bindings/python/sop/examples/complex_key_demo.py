import os
import sys
import shutil
from dataclasses import dataclass
import json

# Add the parent directory to sys.path to import sop
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from sop import Context, BtreeOptions, Item, ValueDataSize, PagingInfo
from sop.btree import IndexSpecification, IndexFieldSpecification
from sop.ai import Database, DatabaseType
from sop.database import DatabaseOptions

# 1. Define your Complex Key Structure
# This will be the Primary Key of the B-Tree.
@dataclass
class UserKey:
    region: str
    department: str
    employee_id: int

# 2. Define your Complex Value Structure
# This is the data payload.
@dataclass
class UserProfile:
    first_name: str
    last_name: str
    role: str
    active: bool

def main():
    # Setup paths
    db_path = "complex_key_demo_db"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    print("Initializing Complex Key Demo...")
    ctx = Context()
    
    # Initialize Database (Standalone for simplicity)
    db = Database(DatabaseOptions(stores_folders=[db_path], type=DatabaseType.Standalone))

    # --- Step 1: Create the B-Tree with Index Specification ---
    print("\n--- Creating B-Tree with Composite Key ---")
    with db.begin_transaction(ctx) as t:
        # Configure B-Tree Options
        # is_primitive_key=False tells SOP that the key is a complex object (struct/dataclass), not a simple string/int.
        bo = BtreeOptions("employees", is_unique=True)
        bo.is_primitive_key = False 
        bo.set_value_data_size(ValueDataSize.Small)

        # Define how the Key fields should be indexed and sorted.
        # Order matters! This defines the hierarchy of the B-Tree.
        # Here: Region -> Department -> EmployeeID
        idx_spec = IndexSpecification(
            index_fields=(
                IndexFieldSpecification("region", ascending_sort_order=True),
                IndexFieldSpecification("department", ascending_sort_order=True),
                IndexFieldSpecification("employee_id", ascending_sort_order=True),
            )
        )

        # Create the B-Tree
        store = db.new_btree(ctx, "employees", t, options=bo, index_spec=idx_spec)
        
        # Add Data
        # We create Item objects where 'key' is an instance of UserKey and 'value' is UserProfile
        
        # User 1: US / Engineering / 101
        k1 = UserKey(region="US", department="Engineering", employee_id=101)
        v1 = UserProfile(first_name="Alice", last_name="Smith", role="Dev", active=True)
        store.add(ctx, Item(key=k1, value=v1))

        # User 2: US / Sales / 202
        k2 = UserKey(region="US", department="Sales", employee_id=202)
        v2 = UserProfile(first_name="Bob", last_name="Jones", role="Manager", active=True)
        store.add(ctx, Item(key=k2, value=v2))

        # User 3: EU / Engineering / 102
        k3 = UserKey(region="EU", department="Engineering", employee_id=102)
        v3 = UserProfile(first_name="Charlie", last_name="Brown", role="Dev", active=True)
        store.add(ctx, Item(key=k3, value=v3))

        print("Added 3 users with complex keys.")

    # --- Step 2: Querying with Partial Keys (Prefix Search) ---
    print("\n--- Querying Data ---")
    with db.begin_transaction(ctx) as t:
        store = db.open_btree(ctx, "employees", t)

        # A. Exact Match
        target_key = UserKey(region="US", department="Engineering", employee_id=101)
        if store.find(ctx, target_key):
            # Fetch the current item (page_size=1)
            items = store.get_items(ctx, PagingInfo(page_size=1))
            if items:
                item = items[0]
                # Note: The Python wrapper returns the value as a dictionary for dataclasses
                print(f"Found Exact Match: {item.key} -> {item.value}")

        # B. Range Query / Prefix Scan
        # Since the key is (Region, Department, ID), we can efficiently scan all "US" employees.
        
        print("\nScanning all 'US' employees:")
        
        # Move to the first record
        if store.first(ctx):
            # Fetch a batch of items (e.g., 100)
            # In a real app, you would loop and fetch pages until no more items.
            items = store.get_items(ctx, PagingInfo(page_size=100))
            
            for item in items:
                # item.key is a dictionary representation of the UserKey
                # We filter for "US" region
                if item.key['region'] == "US":
                     print(f" - {item.value['first_name']} {item.value['last_name']} ({item.key['department']})")


if __name__ == "__main__":
    main()
