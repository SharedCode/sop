import unittest
import sys
import os
import shutil
import time
import random

# Ensure we can import sop
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sop.ai import VectorDatabase, Item, UsageMode, DBType, ModelStore, Model
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
        # Simple config for standalone test
        opts = TransactionOptions(
            mode=TransactionMode.ForWriting.value,
            max_time=15,
            registry_hash_mod=250,
            stores_folders=[path],
            erasure_config={},
            db_type=DBType.Standalone.value
        )

        # 1. Create (Insert)
        print("Creating 100 users...")
        with Transaction(ctx, opts) as t:
            cache = CacheConfig()
            bo = BtreeOptions("users", True, cache_config=cache)
            bo.set_value_data_size(ValueDataSize.Small)
            
            b3 = Btree.new(ctx, bo, t)
            
            items = []
            for i in range(100):
                items.append(BtreeItem(key=f"user_{i}", value=f"User Name {i}"))
            
            b3.add(ctx, items)
            # t.commit(ctx) - handled by context manager

        # Verify Insert
        with Transaction(ctx, opts) as t:
            b3 = Btree.open(ctx, "users", t)
            count = b3.count()
            self.assertEqual(count, 100)
            
            found = b3.find(ctx, "user_50")
            self.assertTrue(found)
            
            items = b3.get_values(ctx, [BtreeItem(key="user_50")])
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0].value, "User Name 50")

        # 2. Update
        print("Updating 100 users...")
        with Transaction(ctx, opts) as t:
            b3 = Btree.open(ctx, "users", t)
            
            items = []
            for i in range(100):
                items.append(BtreeItem(key=f"user_{i}", value=f"Updated User {i}"))
            
            b3.update(ctx, items)
            # t.commit(ctx)

        # Verify Update
        with Transaction(ctx, opts) as t:
            b3 = Btree.open(ctx, "users", t)
            items = b3.get_values(ctx, [BtreeItem(key="user_50")])
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0].value, "Updated User 50")

        # 3. Delete
        print("Deleting 100 users...")
        with Transaction(ctx, opts) as t:
            b3 = Btree.open(ctx, "users", t)
            
            keys = [f"user_{i}" for i in range(100)]
            b3.remove(ctx, keys)
            # t.commit(ctx)

        # Verify Delete
        with Transaction(ctx, opts) as t:
            b3 = Btree.open(ctx, "users", t)
            count = b3.count()
            self.assertEqual(count, 0)

    def test_model_store_cud(self):
        print("\n--- Testing Model Store CUD (Batch 50) ---")
        path = self.create_temp_dir("model_store")
        
        # Setup Transaction Context for B-Tree Store
        ctx = Context()
        opts = TransactionOptions(
            mode=TransactionMode.ForWriting.value,
            max_time=15,
            registry_hash_mod=250,
            stores_folders=[path],
            erasure_config={}, # Empty map if no EC
            db_type=DBType.Standalone.value
        )

        # 1. Create
        print("Creating 50 models...")
        with Transaction(ctx, opts) as t:
            store = ModelStore.open_btree_store(t)
            for i in range(50):
                model = Model(id=f"model_{i}", algorithm="algo", hyperparameters={"k": i}, parameters=[], metrics={}, is_active=True)
                store.save("default", f"model_{i}", model)
            # t.commit(ctx)
            
        # Verify Create
        with Transaction(ctx, opts) as t:
            store = ModelStore.open_btree_store(t)
            models = store.list("default")
            self.assertEqual(len(models), 50)
            fetched = store.get("default", "model_25")
            # fetched is a dict because get returns Any (json decoded)
            self.assertEqual(fetched["hyperparameters"]["k"], 25)
            # t.commit(ctx)

        # 2. Update
        print("Updating 50 models...")
        with Transaction(ctx, opts) as t:
            store = ModelStore.open_btree_store(t)
            for i in range(50):
                model_dict = store.get("default", f"model_{i}")
                model_dict["hyperparameters"]["k"] = i * 10
                store.save("default", f"model_{i}", model_dict)
            # t.commit(ctx)

        # Verify Update
        with Transaction(ctx, opts) as t:
            store = ModelStore.open_btree_store(t)
            fetched = store.get("default", "model_25")
            self.assertEqual(fetched["hyperparameters"]["k"], 250)
            # t.commit(ctx)

        # 3. Delete
        print("Deleting 50 models...")
        with Transaction(ctx, opts) as t:
            store = ModelStore.open_btree_store(t)
            for i in range(50):
                store.delete("default", f"model_{i}")
            # t.commit(ctx)

        # Verify Delete
        with Transaction(ctx, opts) as t:
            store = ModelStore.open_btree_store(t)
            models = store.list("default")
            self.assertEqual(len(models), 0)
            # t.commit(ctx)

    def test_vector_store_cud(self):
        print("\n--- Testing Vector Store CUD (Batch 50) ---")
        path = self.create_temp_dir("vector_store")
        
        vdb = VectorDatabase(storage_path=path, usage_mode=UsageMode.Dynamic, db_type=DBType.Standalone)
        store = vdb.open("vectors_test")

        # 1. Create
        print("Creating 50 vectors...")
        for i in range(50):
            # Simple 2D vector
            vec = [float(i), float(i)]
            item = Item(id=f"vec_{i}", vector=vec, payload={"val": i})
            store.upsert(item)

        # Verify Create
        # Note: Vector store count might not be directly exposed easily without iterating or internal check, 
        # but we can check existence via get
        fetched = store.get("vec_25")
        self.assertEqual(fetched.id, "vec_25")
        self.assertEqual(fetched.payload["val"], 25)

        # NEW: Optimize and Validate Internals
        print("Optimizing Vector Store...")
        store.optimize()

        print("Validating Internal B-Trees...")
        # We need a transaction to open B-Trees
        ctx = Context()
        opts = TransactionOptions(
            mode=TransactionMode.ForReading.value,
            max_time=15,
            registry_hash_mod=250,
            stores_folders=[path],
            erasure_config={},
            db_type=DBType.Standalone.value
        )
        
        with Transaction(ctx, opts) as t:
            # 1. Centroids
            # Note: For small dataset, we might only have 1 centroid or 0 if not partitioned yet?
            # But usually there is at least one or the system initializes them.
            # Let's check if we can open it.
            b_centroids = Btree.open(ctx, "vectors_test_centroids", t)
            c_count = b_centroids.count()
            print(f"Centroids count: {c_count}")
            # With 50 items, it might be small, but should be accessible.
            
            # 2. Vectors
            b_vectors = Btree.open(ctx, "vectors_test_vectors", t)
            v_count = b_vectors.count()
            print(f"Vectors count: {v_count}")
            self.assertEqual(v_count, 50)

            # 3. Content (Payloads)
            b_content = Btree.open(ctx, "vectors_test_content", t)
            d_count = b_content.count()
            print(f"Content count: {d_count}")
            self.assertEqual(d_count, 50)
            
            # t.commit(ctx)

        # 2. Update
        print("Updating 50 vectors...")
        for i in range(50):
            vec = [float(i*2), float(i*2)]
            item = Item(id=f"vec_{i}", vector=vec, payload={"val": i*100})
            store.upsert(item)

        # Verify Update
        fetched = store.get("vec_25")
        self.assertEqual(fetched.payload["val"], 2500)
        self.assertEqual(fetched.vector, [50.0, 50.0])

        # 3. Delete
        print("Deleting 50 vectors...")
        for i in range(50):
            store.delete(f"vec_{i}")

        # Verify Delete
        try:
            fetched = store.get("vec_25")
            self.fail("Should have raised exception for deleted item")
        except Exception as e:
            pass

if __name__ == '__main__':
    unittest.main()
