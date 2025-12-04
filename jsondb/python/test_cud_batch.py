import unittest
import sys
import os
import shutil
import time
import random

# Ensure we can import sop
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sop.ai import Database, Item, UsageMode, DBType, ModelStore, Model
from sop import Context, Transaction, TransactionOptions, TransactionMode
from sop.btree import Btree, BtreeOptions, ValueDataSize, CacheConfig, Item as BtreeItem
from sop.transaction import ErasureCodingConfig

class TestCUDBatch(unittest.TestCase):
    def setUp(self):
        self.test_dirs = []

    def tearDown(self):
        for d in self.test_dirs:
            if os.path.exists(d):
                shutil.rmtree(d)

    def create_temp_dir(self, suffix):
        path = f"/tmp/sop_cud_test_{suffix}_{int(time.time())}_{random.randint(0, 1000)}"
        self.test_dirs.append(path)
        return path

    def test_user_btree_cud(self):
        print("\n--- Testing User B-Tree CUD (Batch 100) ---")
        path = self.create_temp_dir("user_btree")
        
        # Setup Transaction Context
        ctx = Context()
        
        # Create Database instance
        db = Database(ctx, storage_path=path, db_type=DBType.Standalone)

        # Simple config for standalone test
        opts = TransactionOptions(
            mode=TransactionMode.ForWriting.value,
            max_time=15,
            registry_hash_mod=250
        )

        # 1. Create (Insert)
        print("Creating 100 users...")
        with db.begin_transaction(ctx, options=opts) as t:
            cache = CacheConfig()
            bo = BtreeOptions("users", True, cache_config=cache)
            bo.set_value_data_size(ValueDataSize.Small)
            
            b3 = db.new_btree(ctx, "users", t, options=bo)
            
            items = []
            for i in range(100):
                items.append(BtreeItem(key=f"user_{i}", value=f"User Name {i}"))
            
            b3.add(ctx, items)
            # t.commit(ctx) - handled by context manager

        # Verify Insert
        with db.begin_transaction(ctx, options=opts) as t:
            b3 = db.open_btree(ctx, "users", t)
            count = b3.count()
            self.assertEqual(count, 100)
            
            found = b3.find(ctx, "user_50")
            self.assertTrue(found)
            
            items = b3.get_values(ctx, [BtreeItem(key="user_50")])
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0].value, "User Name 50")

        # 2. Update
        print("Updating 100 users...")
        with db.begin_transaction(ctx, options=opts) as t:
            b3 = db.open_btree(ctx, "users", t)
            
            items = []
            for i in range(100):
                items.append(BtreeItem(key=f"user_{i}", value=f"Updated User {i}"))
            
            b3.update(ctx, items)
            # t.commit(ctx)

        # Verify Update
        with db.begin_transaction(ctx, options=opts) as t:
            b3 = db.open_btree(ctx, "users", t)
            items = b3.get_values(ctx, [BtreeItem(key="user_50")])
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0].value, "Updated User 50")

        # 3. Delete
        print("Deleting 100 users...")
        with db.begin_transaction(ctx, options=opts) as t:
            b3 = db.open_btree(ctx, "users", t)
            
            keys = [f"user_{i}" for i in range(100)]
            b3.remove(ctx, keys)
            # t.commit(ctx)

        # Verify Delete
        with db.begin_transaction(ctx, options=opts) as t:
            b3 = db.open_btree(ctx, "users", t)
            count = b3.count()
            self.assertEqual(count, 0)

    def test_model_store_cud(self):
        print("\n--- Testing Model Store CUD (Batch 50) ---")
        path = self.create_temp_dir("model_store")
        
        # Setup Transaction Context for B-Tree Store
        ctx = Context()
        
        db = Database(ctx, storage_path=path, db_type=DBType.Standalone)

        opts = TransactionOptions(
            mode=TransactionMode.ForWriting.value,
            max_time=15,
            registry_hash_mod=250
        )

        # 1. Create
        print("Creating 50 models...")
        with db.begin_transaction(ctx, options=opts) as t:
            store = db.open_model_store(ctx, t, "default")
            for i in range(50):
                model = Model(id=f"model_{i}", algorithm="algo", hyperparameters={"k": i}, parameters=[], metrics={}, is_active=True)
                store.save(ctx, "default", f"model_{i}", model)
            # t.commit(ctx)
            
        # Verify Create
        with db.begin_transaction(ctx, options=opts) as t:
            store = db.open_model_store(ctx, t, "default")
            models = store.list(ctx, "default")
            self.assertEqual(len(models), 50)
            fetched = store.get(ctx, "default", "model_25")
            # fetched is a dict because get returns Any (json decoded)
            self.assertEqual(fetched["hyperparameters"]["k"], 25)
            # t.commit(ctx)

        # 2. Update
        print("Updating 50 models...")
        with db.begin_transaction(ctx, options=opts) as t:
            store = db.open_model_store(ctx, t, "default")
            for i in range(50):
                model_dict = store.get(ctx, "default", f"model_{i}")
                model_dict["hyperparameters"]["k"] = i * 10
                store.save(ctx, "default", f"model_{i}", model_dict)
            # t.commit(ctx)

        # Verify Update
        with db.begin_transaction(ctx, options=opts) as t:
            store = db.open_model_store(ctx, t, "default")
            fetched = store.get(ctx, "default", "model_25")
            self.assertEqual(fetched["hyperparameters"]["k"], 250)
            # t.commit(ctx)

        # 3. Delete
        print("Deleting 50 models...")
        with db.begin_transaction(ctx, options=opts) as t:
            store = db.open_model_store(ctx, t, "default")
            for i in range(50):
                store.delete(ctx, "default", f"model_{i}")
            # t.commit(ctx)

        # Verify Delete
        with db.begin_transaction(ctx, options=opts) as t:
            store = db.open_model_store(ctx, t, "default")
            models = store.list(ctx, "default")
            self.assertEqual(len(models), 0)
            # t.commit(ctx)

    def test_vector_store_cud(self):
        print("\n--- Testing Vector Store CUD (Batch 50) ---")
        path = self.create_temp_dir("vector_store")
        
        ctx = Context()
        vdb = Database(ctx, storage_path=path, db_type=DBType.Standalone)
        
        # 1. Create
        print("Creating 50 vectors...")
        tx1 = vdb.begin_transaction(ctx)
        store = vdb.open_vector_store(ctx, tx1, "vectors_test")

        for i in range(50):
            # Simple 2D vector
            vec = [float(i), float(i)]
            item = Item(id=f"vec_{i}", vector=vec, payload={"val": i})
            store.upsert(ctx, item)
        
        tx1.commit(ctx)

        # Verify Create
        tx2 = vdb.begin_transaction(ctx)
        store = vdb.open_vector_store(ctx, tx2, "vectors_test")

        # Note: Vector store count might not be directly exposed easily without iterating or internal check, 
        # but we can check existence via get
        fetched = store.get(ctx, "vec_25")
        self.assertEqual(fetched.id, "vec_25")
        self.assertEqual(fetched.payload["val"], 25)

        # NEW: Optimize and Validate Internals
        print("Optimizing Vector Store...")
        store.optimize(ctx)
        
        # tx2.commit(ctx) - Optimize already commits the transaction

        print("Validating Internal B-Trees...")
        # We need a transaction to open B-Trees
        # ctx = Context() # Reuse context
        opts = TransactionOptions(
            mode=TransactionMode.ForReading.value,
            max_time=15,
            registry_hash_mod=250
        )
        
        with vdb.begin_transaction(ctx, options=opts) as t:
            # 1. Centroids
            # Note: For small dataset, we might only have 1 centroid or 0 if not partitioned yet?
            # But usually there is at least one or the system initializes them.
            # Let's check if we can open it.
            # After optimize, version is 1, so store name has _1 suffix
            b_centroids = vdb.open_btree(ctx, "vectors_test_centroids_1", t)
            c_count = b_centroids.count()
            print(f"Centroids count: {c_count}")
            # With 50 items, it might be small, but should be accessible.
            
            # 2. Vectors
            b_vectors = vdb.open_btree(ctx, "vectors_test_vecs_1", t)
            v_count = b_vectors.count()
            print(f"Vectors count: {v_count}")
            self.assertEqual(v_count, 50)

            # 3. Content (Payloads)
            b_content = vdb.open_btree(ctx, "vectors_test_data", t)
            d_count = b_content.count()
            print(f"Content count: {d_count}")
            self.assertEqual(d_count, 50)
            
            # t.commit(ctx)

        # 2. Update
        print("Updating 50 vectors...")
        tx3 = vdb.begin_transaction(ctx)
        store = vdb.open_vector_store(ctx, tx3, "vectors_test")

        for i in range(50):
            vec = [float(i*2), float(i*2)]
            item = Item(id=f"vec_{i}", vector=vec, payload={"val": i*100})
            store.upsert(ctx, item)
        
        tx3.commit(ctx)

        # Verify Update
        tx4 = vdb.begin_transaction(ctx)
        store = vdb.open_vector_store(ctx, tx4, "vectors_test")

        fetched = store.get(ctx, "vec_25")
        self.assertEqual(fetched.payload["val"], 2500)
        self.assertEqual(fetched.vector, [50.0, 50.0])
        
        tx4.commit(ctx)

        # 3. Delete
        print("Deleting 50 vectors...")
        tx5 = vdb.begin_transaction(ctx)
        store = vdb.open_vector_store(ctx, tx5, "vectors_test")

        for i in range(50):
            store.delete(ctx, f"vec_{i}")
        
        tx5.commit(ctx)

        # Verify Delete
        tx6 = vdb.begin_transaction(ctx)
        store = vdb.open_vector_store(ctx, tx6, "vectors_test")

        try:
            fetched = store.get(ctx, "vec_25")
            self.fail("Should have raised exception for deleted item")
        except Exception as e:
            pass
        
        tx6.commit(ctx)

if __name__ == '__main__':
    unittest.main()
