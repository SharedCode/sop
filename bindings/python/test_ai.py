import unittest
import sys
import os
import shutil
import time
from sop.ai import Database, Item, DatabaseType, Model
from sop.database import DatabaseOptions
from sop import Context, TransactionMode
from sop.transaction import ErasureCodingConfig, RedisCacheConfig
from sop.redis import Redis

# Ensure we can import sop
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

class TestSOPAI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.redis_available = True
        # Open Redis connection globally for the tests
        try:
            # We skip explicit initialization check and let tests fail if Redis is down
            # or rely on local setup
             pass
        except Exception as e:
            print(f"Warning: Failed to connect to Redis: {e}")
            # We continue, but clustered tests might fail

    @classmethod
    def tearDownClass(cls):
        try:
            Redis.close()
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
        
        ctx = Context()
        vdb = Database(DatabaseOptions(stores_folders=[path], type=DatabaseType.Standalone))
        
        # Transaction 1: Upsert
        tx1 = vdb.begin_transaction(ctx)
        store = vdb.open_vector_store(ctx, tx1, "products")

        # Upsert
        item = Item(id="p1", vector=[0.1, 0.1, 0.1], payload={"cat": "A"})
        store.upsert(ctx, item)
        
        tx1.commit(ctx)

        # Transaction 2: Read
        tx2 = vdb.begin_transaction(ctx)
        store = vdb.open_vector_store(ctx, tx2, "products")

        # Get
        fetched = store.get(ctx, "p1")
        self.assertEqual(fetched.id, "p1")
        self.assertEqual(fetched.payload["cat"], "A")

        # Query
        hits = store.query(ctx, vector=[0.1, 0.1, 0.1], k=1)
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0].id, "p1")
        
        tx2.commit(ctx)

    def test_vector_db_clustered(self):
        if not self.redis_available:
            self.skipTest("Redis not available")
        print("\n--- Testing Vector DB (Clustered) ---")
        # This requires Redis. We assume it's running based on previous check.
        path = self.create_temp_dir("vec_clustered")
        
        ctx = Context()
        red_conf = RedisCacheConfig(url="redis://localhost:6379")
        vdb = Database(DatabaseOptions(stores_folders=[path], type=DatabaseType.Clustered, redis_config=red_conf))
        
        # Transaction 1: Upsert
        tx1 = vdb.begin_transaction(ctx)
        store = vdb.open_vector_store(ctx, tx1, "products_cluster")

        # Upsert
        item = Item(id="c1", vector=[0.9, 0.9, 0.9], payload={"cat": "C"})
        store.upsert(ctx, item)
        
        tx1.commit(ctx)

        # Transaction 2: Read
        tx2 = vdb.begin_transaction(ctx)
        store = vdb.open_vector_store(ctx, tx2, "products_cluster")

        # Get
        fetched = store.get(ctx, "c1")
        self.assertEqual(fetched.id, "c1")

        # Query
        hits = store.query(ctx, vector=[0.9, 0.9, 0.9], k=1)
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0].id, "c1")
        
        tx2.commit(ctx)

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
        
        db = Database(DatabaseOptions(
            type=DatabaseType.Standalone, 
            erasure_config={"": ec_config},
            stores_folders=[path, path_passive]
        ))

        with db.begin_transaction(ctx) as t:
            store = db.open_model_store(ctx, t, "default")
            
            model = Model(id="bm1", algorithm="tree", hyperparameters={}, parameters=[], metrics={}, is_active=True)
            store.save(ctx, "default", "bm1", model)
            
            fetched = store.get(ctx, "default", "bm1")
            # fetched is a dict, not Model object because get returns Any (JSON)
            self.assertEqual(fetched["id"], "bm1")
            
            names = store.list(ctx, "default")
            self.assertIn("bm1", names)

    def test_vector_db_clustered_replication(self):
        if not self.redis_available:
            self.skipTest("Redis not available")
        print("\n--- Testing Vector DB (Clustered + Replication) ---")
        # This requires Redis.
        path = self.create_temp_dir("vec_clus_repl_active")
        path_passive = self.create_temp_dir("vec_clus_repl_passive")
        
        # Create 2 "drives" for EC
        drive1 = self.create_temp_dir("drive1")
        drive2 = self.create_temp_dir("drive2")

        ec_config = ErasureCodingConfig(
            data_shards_count=1,
            parity_shards_count=1,
            base_folder_paths_across_drives=[drive1, drive2],
            repair_corrupted_shards=False
        )
        
        ctx = Context()
        vdb = Database(DatabaseOptions(
            type=DatabaseType.Clustered,
            erasure_config={"": ec_config},
            stores_folders=[path, path_passive],
            redis_config=RedisCacheConfig(url="redis://localhost:6379")
        ))
        
        # Transaction 1: Upsert
        with vdb.begin_transaction(ctx) as tx1:
            store = vdb.open_vector_store(ctx, tx1, "products_cluster_repl")
            item = Item(id="cr1", vector=[0.5, 0.5, 0.5], payload={"cat": "CR"})
            store.upsert(ctx, item)
        
        # Transaction 2: Read
        with vdb.begin_transaction(ctx, mode=TransactionMode.ForReading.value) as tx2:
            store = vdb.open_vector_store(ctx, tx2, "products_cluster_repl")
            fetched = store.get(ctx, "cr1")
            self.assertEqual(fetched.id, "cr1")
            
            hits = store.query(ctx, vector=[0.5, 0.5, 0.5], k=1)
            self.assertEqual(len(hits), 1)
            self.assertEqual(hits[0].id, "cr1")

if __name__ == '__main__':
    unittest.main()
