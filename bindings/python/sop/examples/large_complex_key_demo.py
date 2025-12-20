import os
import sys
import shutil
import random
from dataclasses import dataclass
import time

# Add parent directory to path to import sop
# We need to go up two levels: examples -> sop -> python_root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from sop import Context, BtreeOptions, Item, ValueDataSize, PagingInfo
from sop.btree import IndexSpecification, IndexFieldSpecification
from sop.ai import Database, DatabaseType
from sop.database import DatabaseOptions

# 1. Define Complex Key
@dataclass
class EmployeeKey:
    region: str
    department: str
    employee_id: int

# 2. Define Value
@dataclass
class EmployeeProfile:
    name: str
    title: str
    salary: int
    active: bool

def main():
    db_path = "large_complex_key_db"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    print(f"Initializing Large Complex Key Demo in '{db_path}'...")
    ctx = Context()
    db = Database(DatabaseOptions(stores_folders=[db_path], type=DatabaseType.Standalone))

    regions = ["US", "EU", "APAC", "LATAM"]
    departments = ["Engineering", "Sales", "HR", "Marketing", "Finance"]
    titles = ["Associate", "Senior", "Lead", "Manager", "Director"]

    total_records = 2000
    batch_size = 500

    print(f"Generating {total_records} records...")

    with db.begin_transaction(ctx) as t:
        # Configure B-Tree
        bo = BtreeOptions("employees", is_unique=True)
        bo.is_primitive_key = False 
        bo.set_value_data_size(ValueDataSize.Small)

        # Index: Region -> Department -> EmployeeID
        idx_spec = IndexSpecification(
            index_fields=(
                IndexFieldSpecification("region", ascending_sort_order=True),
                IndexFieldSpecification("department", ascending_sort_order=True),
                IndexFieldSpecification("employee_id", ascending_sort_order=True),
            )
        )

        store = db.new_btree(ctx, "employees", t, options=bo, index_spec=idx_spec)
        
        count = 0
        for i in range(total_records):
            region = random.choice(regions)
            dept = random.choice(departments)
            # Ensure unique ID per region/dept combo for simplicity in this demo, 
            # or just use a global counter for the ID part to ensure uniqueness of the full key.
            # Here we use the loop index 'i' as the ID to guarantee uniqueness of the tuple.
            emp_id = i + 1000 

            key = EmployeeKey(region, dept, emp_id)
            val = EmployeeProfile(
                name=f"Employee {i}", 
                title=random.choice(titles),
                salary=random.randint(50000, 150000),
                active=random.choice([True, False])
            )
            
            store.add(ctx, Item(key=key, value=val))
            count += 1
            
            if count % batch_size == 0:
                print(f"  Prepared {count} records...")

        print("Committing transaction...")
    
    print("Done! Database created.")
    print(f"You can now browse '{db_path}' using sop-httpserver.")

if __name__ == "__main__":
    main()
