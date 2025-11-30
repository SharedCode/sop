import unittest
import sys
import os
import shutil
import time
from sop.ai import VectorDatabase, Item, UsageMode, DBType, ModelStore, Model
from sop import Context, Transaction, TransactionOptions, TransactionMode
from sop.transaction import ErasureCodingConfig
from sop.redis import Redis, RedisOptions

# Ensure we can import sop
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

class TestSOPAI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Open Redis connection globally for the tests
        try:
            ro = RedisOptions()
            Redis.open_connection(ro)
        except Exception as e:
            print(f"Warning: Failed to connect to Redis: {e}")
            # We continue, but clustered tests might fail

    @classmethod
    def tearDownClass(cls):
        try:
            Redis.close_connection()
        except:
            pass

    def setUp(self):
        self.test_dirs = []

    def tearDown(self):
        for d in self.test_dirs:
            if os.path.exists(d):
                shutil.rmtree(d)

    def create_temp_dir(self, suffix):
        path = f"/tmp/sop_test_{suffix}_{int(time.time())}"
        self.test_dirs.append(path)
        return path

    def test_vector_db_standalone(self):
        print("\n--- Testing Vector DB (Standalone) ---")
        path = self.create_temp_dir("vec_standalone")
        
        vdb = VectorDatabase(storage_path=path, usage_mode=UsageMode.Dynamic, db_type=DBType.Standalone)
        store = vdb.open("products")

        # Upsert
        item = Item(id="p1", vector=[0.1, 0.1, 0.1], payload={"cat": "A"})
        store.upsert(item)

        # Get
        fetched = store.get("p1")
        self.assertEqual(fetched.id, "p1")
        self.assertEqual(fetched.payload["cat"], "A")

        # Query
        hits = store.query(vector=[0.1, 0.1, 0.1], k=1)
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0].id, "p1")

    def test_vector_db_clustered(self):
        print("\n--- Testing Vector DB (Clustered) ---")
        # This requires Redis. We assume it's running based on previous check.
        path = self.create_temp_dir("vec_clustered")
        
        vdb = VectorDatabase(storage_path=path, usage_mode=UsageMode.Dynamic, db_type=DBType.Clustered)
        store = vdb.open("products_cluster")

        # Upsert
        item = Item(id="c1", vector=[0.9, 0.9, 0.9], payload={"cat": "C"})
        store.upsert(item)

        # Get
        fetched = store.get("c1")
        self.assertEqual(fetched.id, "c1")

        # Query
        hits = store.query(vector=[0.9, 0.9, 0.9], k=1)
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0].id, "c1")

    def test_model_store_file(self):
        print("\n--- Testing Model Store (File) ---")
        path = self.create_temp_dir("model_file")
        
        store = ModelStore.open_file_store(path)
        
        model = Model(id="m1", algorithm="test", hyperparameters={}, parameters=[], metrics={}, is_active=True)
        store.save("m1", model)
        
        fetched = store.get("m1")
        self.assertEqual(fetched.id, "m1")
        
        names = store.list()
        self.assertIn("m1", names)

    def test_model_store_btree(self):
        print("\n--- Testing Model Store (B-Tree) ---")
        # B-Tree store requires a Transaction Context
        path = self.create_temp_dir("model_btree")
        path_passive = self.create_temp_dir("model_btree_passive")
        
        # Create 2 "drives" for EC (1 data + 1 parity)
        drive1 = self.create_temp_dir("drive1")
        drive2 = self.create_temp_dir("drive2")

        ec_config = ErasureCodingConfig(
            data_shards_count=1,
            parity_shards_count=1,
            base_folder_paths_across_drives=[drive1, drive2],
            repair_corrupted_shards=False
        )
        
        ctx = Context()
        opts = TransactionOptions(
            mode=TransactionMode.ForWriting.value,
            max_time=15,
            registry_hash_mod=250,
            stores_folders=[path, path_passive],
            erasure_config={"": ec_config}
        )

        with Transaction(ctx, opts) as t:
            store = ModelStore.open_btree_store(t)
            
            model = Model(id="bm1", algorithm="tree", hyperparameters={}, parameters=[], metrics={}, is_active=True)
            store.save("bm1", model)
            
            fetched = store.get("bm1")
            self.assertEqual(fetched.id, "bm1")
            
            names = store.list()
            self.assertIn("bm1", names)

if __name__ == '__main__':
    unittest.main()
