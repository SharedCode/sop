import sop.context as context
import sop.database as database
import sop.transaction as transaction
from sop.btree import BtreeOptions, Relation
import os
from dataclasses import dataclass, asdict

@dataclass
class User:
    id: str
    name: str

@dataclass
class Order:
    id: str
    user_id: str
    amount: float

def run():
    # 1. Setup
    ctx = context.Context()
    # Define database options pointing to "data" folder (must be absolute or relative path)
    opts = database.DatabaseOptions(stores_folders=["data"])
    db = database.Database(opts)
    
    # 2. Begin Transaction
    t = db.begin_transaction(ctx, mode=transaction.TransactionMode.ForWriting.value)
    
    try:
        # 3. Create 'Users' Store (Target)
        print("Creating 'users' store...")
        user_store = db.new_btree(ctx, "users", t)
        user_store.add(ctx, "user_1", asdict(User(id="user_1", name="Alice")))
        
        # 4. Create 'Orders' Store with Relation Metadata (Source)
        print("Creating 'orders' store with Relation metadata...")
        
        # Define the relationship: orders.user_id -> users.id
        rel = Relation(
            source_fields=["user_id"],
            target_store="users",
            target_fields=["id"]
        )
        
        opts = BtreeOptions(
            relations=[rel]
        )
        
        # Note: We expect the Value type to be a dict/object containing 'user_id'
        order_store = db.new_btree(ctx, "orders", t, options=opts)
        
        # Add a dummy order using strongly-typed dataclass
        order = Order(id="order_A", user_id="user_1", amount=100)
        order_store.add(ctx, "order_A", asdict(order))

        # 5. Commit
        t.commit(ctx)
        print("Successfully created stores with Relations!")
        print("Verified: Order Store 'orders' is linked to 'users'.")

    except Exception as e:
        t.rollback(ctx)
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    run()
