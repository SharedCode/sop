import sys
import os
import uuid
import random
import json
import logging

# Ensure we can import the sop package
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sop import context, transaction, btree
from sop.ai import vector, model, database

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Sanity Check...")

    # 1. Initialize Context
    ctx = context.Context()
    logger.info(f"Context initialized: {ctx.id}")

    # 2. Initialize Transaction
    # We need a directory for the stores
    store_path = "/tmp/sop_sanity_check"
    if not os.path.exists(store_path):
        os.makedirs(store_path)

    trans_options = transaction.TransactionOptions(
        mode=transaction.TransactionMode.ForWriting.value,
        max_time=15,
        registry_hash_mod=250,
        stores_folders=[store_path],
        erasure_config=None,
        db_type=0 # Standalone
    )

    logger.info("Initializing Transaction...")
    try:
        with transaction.Transaction(ctx, trans_options) as trans:
            logger.info(f"Transaction started: {trans.transaction_id}")

            # 3. Create B-Trees
            # 3a. Generic B-Trees (e.g. User Data)
            logger.info("Creating Generic B-Trees...")
            user_btree_opts = btree.BtreeOptions(name="Users", slot_length=100)
            user_btree = btree.Btree[str, str].new(ctx, user_btree_opts, trans)
            
            log_btree_opts = btree.BtreeOptions(name="Logs", slot_length=100)
            log_btree = btree.Btree[str, str].new(ctx, log_btree_opts, trans)

            # 3b. Model Stores
            logger.info("Creating Model Stores...")
            # ModelStore.open_btree_store creates a store that uses the transaction
            # We use one store instance for all categories as they map to the same underlying B-Tree
            model_store = model.ModelStore.open_btree_store(trans)

            # 3c. Vector Store
            logger.info("Creating Vector Store...")
            # Vector DB needs to be created first
            vec_db = vector.VectorDatabase(storage_path=store_path)
            # Open a store (this is non-transactional initially)
            vec_store_base = vec_db.open("Embeddings")
            # Wrap in transaction
            vec_store = vec_store_base.with_transaction(trans)

            # 4. Perform CRUD Operations (50-100 items)
            num_items = 60
            logger.info(f"Generating {num_items} items for each store...")

            # Users
            for i in range(num_items):
                user_id = f"user_{i}"
                user_data = json.dumps({"name": f"User {i}", "age": 20 + (i % 50)})
                user_btree.add(ctx, btree.Item(key=user_id, value=user_data))
            
            # Logs
            for i in range(num_items):
                log_id = f"log_{i}"
                log_data = f"Log entry {i} at {uuid.uuid4()}"
                log_btree.add(ctx, btree.Item(key=log_id, value=log_data))

            # Models
            for i in range(num_items):
                model_name = f"model_{i}"
                model_data = {"weights": [random.random() for _ in range(10)], "bias": random.random()}
                model_store.save("recommendation", model_name, model_data)
                model_store.save("classification", model_name, model_data)

            # Vectors
            vec_items = []
            for i in range(num_items):
                vec_id = uuid.uuid4()
                vec_vector = [random.random() for _ in range(128)]
                vec_payload = {"source": f"doc_{i}", "tag": "test"}
                vec_items.append(vector.Item(id=str(vec_id), vector=vec_vector, payload=vec_payload))
            
            vec_store.upsert_batch(vec_items)

            logger.info("All items added. Committing transaction...")
        
        # Transaction commit happens on exit of context manager
        logger.info("Transaction committed successfully.")

    except Exception as e:
        logger.error(f"Transaction failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # 5. Verify Data (Read back)
    logger.info("Verifying data...")
    
    # We need a new transaction for reading (or NoCheck mode)
    read_opts = transaction.TransactionOptions(
        mode=transaction.TransactionMode.ForReading.value,
        max_time=15,
        registry_hash_mod=250,
        stores_folders=[store_path],
        erasure_config=None
    )

    with transaction.Transaction(ctx, read_opts) as trans:
        # Open existing B-Trees
        user_btree = btree.Btree[str, str].open(ctx, "Users", trans)
        count = user_btree.count()
        logger.info(f"Users B-Tree count: {count}")
        if count != num_items:
            logger.error(f"Expected {num_items} users, got {count}")

        # Check a random user
        if user_btree.find(ctx, "user_10"):
            item = user_btree.get_items(ctx, btree.PagingInfo(fetch_count=1))
            logger.info(f"Found user_10: {item[0].value}")
        else:
            logger.error("Could not find user_10")

        # Vector Store Verification
        # We need to re-open the vector store and wrap it in the new transaction
        vec_db = vector.VectorDatabase(storage_path=store_path)
        vec_store_base = vec_db.open("Embeddings")
        vec_store = vec_store_base.with_transaction(trans)
        
        vec_count = vec_store.count()
        logger.info(f"Vector Store count: {vec_count}")
        if vec_count != num_items:
             logger.error(f"Expected {num_items} vectors, got {vec_count}")

        # 6. Optimize Vector Store
        # Optimize might not need a transaction, or it might. 
        # Usually optimize is a maintenance task.
        # Let's call it on the base store (non-transactional) or the transactional one?
        # The implementation in Go uses the store lookup.
        logger.info("Optimizing Vector Store...")
        vec_store.optimize()
        logger.info("Vector Store optimized.")

    logger.info("Sanity Check Complete!")

if __name__ == "__main__":
    main()
