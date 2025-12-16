import os
import sys
import shutil
from dataclasses import dataclass
import time
import json

# Add the parent directory to sys.path to import sop
# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sop import Context, BtreeOptions, Item, ValueDataSize, PagingInfo
from sop.btree import IndexSpecification, IndexFieldSpecification
from sop.ai import Database, DatabaseType
from sop.database import DatabaseOptions

# 1. Define Key with "Ride-On" Metadata
# Only 'doc_id' will be indexed. 'is_deleted' and 'timestamp' ride along.
@dataclass
class DocKey:
    doc_id: int
    is_deleted: bool = False
    timestamp: int = 0

# 2. Define Value (The heavy payload)
@dataclass
class DocContent:
    title: str
    body: str

def main():
    db_path = "metadata_key_demo_db"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    print("Initializing Metadata Key Demo...")
    ctx = Context()
    db = Database(DatabaseOptions(stores_folders=[db_path], type=DatabaseType.Standalone))

    with db.begin_transaction(ctx) as t:
        # Configure B-Tree
        bo = BtreeOptions("documents", is_unique=True)
        bo.is_primitive_key = False
        # We set ValueDataSize.Big to imply values might be stored separately/efficiently
        bo.set_value_data_size(ValueDataSize.Big)

        # Index ONLY on doc_id
        # This tells SOP to ignore 'is_deleted' and 'timestamp' for sorting/uniqueness
        idx_spec = IndexSpecification(
            index_fields=(
                IndexFieldSpecification("doc_id", ascending_sort_order=True),
            )
        )

        store = db.new_btree(ctx, "documents", t, options=bo, index_spec=idx_spec)

        # Add Data
        # Doc 1: Active
        k1 = DocKey(doc_id=100, is_deleted=False, timestamp=int(time.time()))
        v1 = DocContent(title="SOP Intro", body="..." * 1000) # Large body
        store.add(ctx, Item(key=k1, value=v1))

        # Doc 2: Soft Deleted
        k2 = DocKey(doc_id=101, is_deleted=True, timestamp=int(time.time()))
        v2 = DocContent(title="Old Draft", body="..." * 1000)
        store.add(ctx, Item(key=k2, value=v2))

        # Doc 3: Active
        k3 = DocKey(doc_id=102, is_deleted=False, timestamp=int(time.time()))
        v3 = DocContent(title="Advanced SOP", body="..." * 1000)
        store.add(ctx, Item(key=k3, value=v3))

        print("Added 3 documents (one soft-deleted).")

    # --- Querying Metadata without Fetching Values ---
    print("\n--- Scanning Keys (Metadata Only) ---")
    with db.begin_transaction(ctx) as t:
        store = db.open_btree(ctx, "documents", t)

        if store.first(ctx):
            # Use get_keys to fetch ONLY keys (metadata). 
            # This is much faster than get_items if values are large.
            keys = store.get_keys(ctx, PagingInfo(page_size=100))
            
            print(f"Fetched {len(keys)} keys. Checking metadata...")
            
            for item in keys:
                # item.key is the DocKey dict
                # item.value is None (because we used get_keys)
                
                doc_id = item.key['doc_id']
                deleted = item.key['is_deleted']
                ts = item.key['timestamp']
                
                status = "[DELETED]" if deleted else "[ACTIVE]"
                print(f"Doc {doc_id}: {status} (Timestamp: {ts})")
                
                if not deleted:
                    # In a real app, you would fetch the value here if needed
                    # values = store.get_values(ctx, item)
                    pass

if __name__ == "__main__":
    main()
