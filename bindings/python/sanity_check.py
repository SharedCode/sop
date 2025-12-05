import sys
import os
import uuid
import random
import json
import logging

# Ensure we can import the sop package
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sop import context, transaction, btree
from sop.ai import Database, DBType, Item as VectorItem
from sop.redis import Redis

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Sanity Check...")

    # Initialize Redis (required for SOP)
    Redis.open_connection("redis://localhost:6379")

    # 1. Initialize Context
    ctx = context.Context()
    logger.info(f"Context initialized: {ctx.id}")

    # 2. Initialize Database
    store_path = "/tmp/sop_sanity_check"
    if os.path.exists(store_path):
        import shutil
        shutil.rmtree(store_path)
    if not os.path.exists(store_path):
        os.makedirs(store_path)
    
    db = Database(ctx, storage_path=store_path, db_type=DBType.Standalone)

    trans_options = transaction.TransactionOptions(
        mode=transaction.TransactionMode.ForWriting.value,
        max_time=15,
        registry_hash_mod=250,
        stores_folders=[store_path],
        erasure_config={}
    )

    logger.info("Initializing Transaction...")
    try:
        with db.begin_transaction(ctx, options=trans_options) as trans:
            logger.info(f"Transaction started: {trans.transaction_id}")

            # 3. Create B-Trees
            # 3a. Generic B-Trees (e.g. User Data)
            logger.info("Creating Generic B-Trees...")
            user_btree_opts = btree.BtreeOptions(name="Users", slot_length=100)
            user_btree = db.new_btree(ctx, "Users", trans, options=user_btree_opts)
            
            log_btree_opts = btree.BtreeOptions(name="Logs", slot_length=100)
            log_btree = db.new_btree(ctx, "Logs", trans, options=log_btree_opts)

            # 3b. Model Stores
            logger.info("Creating Model Stores...")
            model_store = db.open_model_store(ctx, trans, "default")

            # 3c. Vector Store
            logger.info("Creating Vector Store...")
            vec_store = db.open_vector_store(ctx, trans, "Embeddings")

            # 4. Perform CRUD Operations (50-100 items)
            num_items = 60
            logger.info(f"Generating {num_items} items for each store...")

            # Users
            for i in range(num_items):
                user_id = f"user_{i}"
                user_data = json.dumps({"name": f"User {i}", "age": 20 + (i % 50)})
                user_btree.add(ctx, [btree.Item(key=user_id, value=user_data)])
            
            # Logs
            for i in range(num_items):
                log_id = f"log_{i}"
                log_data = f"Log entry {i} at {uuid.uuid4()}"
                log_btree.add(ctx, [btree.Item(key=log_id, value=log_data)])

            # Models
            for i in range(num_items):
                model_name = f"model_{i}"
                model_data = {"weights": [random.random() for _ in range(10)], "bias": random.random()}
                model_store.save(ctx, "recommendation", model_name, model_data)
                model_store.save(ctx, "classification", model_name, model_data)

            # Vectors
            vec_items = []
            for i in range(num_items):
                vec_id = uuid.uuid4()
                vec_vector = [random.random() for _ in range(128)]
                vec_payload = {"source": f"doc_{i}", "tag": "test"}
                vec_items.append(VectorItem(id=str(vec_id), vector=vec_vector, payload=vec_payload))
            
            vec_store.upsert_batch(ctx, vec_items)

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
        erasure_config={}
    )

    with db.begin_transaction(ctx, options=read_opts) as trans:
        # Open existing B-Trees
        user_btree = db.open_btree(ctx, "Users", trans)
        count = user_btree.count()
        logger.info(f"Users B-Tree count: {count}")
        if count != num_items:
            logger.error(f"Expected {num_items} users, got {count}")

        # Check a random user
        if user_btree.find(ctx, "user_10"):
            # get_items takes PagingInfo
            # But get_values takes keys.
            # Let's use get_values for specific key
            items = user_btree.get_values(ctx, [btree.Item(key="user_10")])
            logger.info(f"Found user_10: {items[0].value}")
        else:
            logger.error("Could not find user_10")

        # Vector Store Verification
        vec_store = db.open_vector_store(ctx, trans, "Embeddings")
        
        vec_count = vec_store.count(ctx)
        logger.info(f"Vector Store count: {vec_count}")
        if vec_count != num_items:
             logger.error(f"Expected {num_items} vectors, got {vec_count}")

    # 6. Optimize Vector Store
    logger.info("Optimizing Vector Store...")
    
    optimize_opts = transaction.TransactionOptions(
        mode=transaction.TransactionMode.ForWriting.value,
        max_time=15,
        registry_hash_mod=250,
        stores_folders=[store_path],
        erasure_config={}
    )
    
    # Optimize commits the transaction internally, so we don't use 'with' block which tries to commit again
    trans = db.begin_transaction(ctx, options=optimize_opts)
    vec_store = db.open_vector_store(ctx, trans, "Embeddings")
    vec_store.optimize(ctx)
    logger.info("Vector Store optimized.")

    logger.info("Sanity Check Complete!")

if __name__ == "__main__":
    main()
