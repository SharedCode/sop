import os
import shutil
import sys
import random
from dataclasses import dataclass

# Add parent directory to path to import sop
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from sop import Context, Item, ValueDataSize
from sop.database import Database, DatabaseOptions, DatabaseType
from sop.btree import BtreeOptions, IndexSpecification, IndexFieldSpecification

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

@dataclass
class ProductKey:
    category: str
    sku: str

@dataclass
class Product:
    name: str
    price: float
    available: bool

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

    # Store 2: Products
    store_name2 = "products"
    bo2 = BtreeOptions(store_name2, is_unique=True)
    bo2.is_primitive_key = False
    bo2.set_value_data_size(ValueDataSize.Small)

    idx2 = IndexSpecification(
        index_fields=(
            IndexFieldSpecification("category", ascending_sort_order=True),
            IndexFieldSpecification("sku", ascending_sort_order=True),
        )
    )

    store2 = db.new_btree(ctx, store_name2, t, options=bo2, index_spec=idx2)

    categories = ["Electronics", "Books", "Clothing", "Home"]
    print(f"Adding items to {store_name2}...")
    
    batch2 = []
    for i in range(500):
        cat = random.choice(categories)
        sku = f"SKU-{i:05d}"
        k = ProductKey(category=cat, sku=sku)
        v = Product(name=f"Product {i}", price=round(random.uniform(10.0, 500.0), 2), available=random.choice([True, False]))
        
        batch2.append(Item(key=k, value=v))
        
        if len(batch2) >= 100:
            store2.add(ctx, batch2)
            batch2 = []
            
    if batch2:
        store2.add(ctx, batch2)
        
    t.commit(ctx)

    print("Done. You can now browse this data using 'sop-httpserver'.")
    print(f"Registry Path: {os.path.abspath(db_path)}")

if __name__ == "__main__":
    main()
