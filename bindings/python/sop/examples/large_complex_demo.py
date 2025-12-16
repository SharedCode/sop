import os
import shutil
import sys
import random
from dataclasses import dataclass
from sop import Context, Database, DatabaseOptions, DatabaseType, Item, BtreeOptions, ValueDataSize
from sop.btree import IndexSpecification, IndexFieldSpecification

@dataclass
class PersonKey:
    country: str
    city: str
    ssn: int

@dataclass
class Person:
    name: str
    age: int
    email: str

def main():
    db_path = "data/large_complex_db"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)
        
    print(f"Generating Large Complex DB at {db_path}...")
    
    ctx = Context()
    db = Database(DatabaseOptions(stores_folders=[db_path], type=DatabaseType.Standalone))
    
    store_name = "people"
    
    t = db.begin_transaction(ctx)
    bo = BtreeOptions(store_name, is_unique=True)
    bo.is_primitive_key = False
    bo.set_value_data_size(ValueDataSize.Small)
    
    idx = IndexSpecification(
        index_fields=(
            IndexFieldSpecification("country", ascending_sort_order=True),
            IndexFieldSpecification("city", ascending_sort_order=True),
            IndexFieldSpecification("ssn", ascending_sort_order=True),
        )
    )
    
    store = db.new_btree(ctx, store_name, t, options=bo, index_spec=idx)
    
    countries = ["US", "UK", "FR", "DE", "JP"]
    cities = ["CityA", "CityB", "CityC", "CityD"]
    
    count = 1000
    print(f"Adding {count} items with Complex keys...")
    
    batch = []
    for i in range(count):
        c = random.choice(countries)
        ct = random.choice(cities)
        k = PersonKey(country=c, city=ct, ssn=100000 + i)
        v = Person(name=f"Person {i}", age=random.randint(20, 80), email=f"p{i}@example.com")
        
        batch.append(Item(key=k, value=v))
        
        if len(batch) >= 100:
            store.add(ctx, batch)
            batch = []
            print(f"  Added {i+1}/{count}")
            
    if batch:
        store.add(ctx, batch)
        
    t.commit(ctx)

    print("Done. You can now browse this data using 'sop-browser'.")
    print(f"Registry Path: {os.path.abspath(db_path)}")

if __name__ == "__main__":
    main()
