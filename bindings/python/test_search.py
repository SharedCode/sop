import unittest
import os
import shutil
from sop import context, database

class TestSearch(unittest.TestCase):
    def setUp(self):
        if os.path.exists("data/search_test"):
            shutil.rmtree("data/search_test")

    def test_search_basic(self):
        ctx = context.Context()
        db = database.Database(database.DatabaseOptions(stores_folders=["data/search_test"], db_type=database.DBType.Standalone))
        t = db.begin_transaction(ctx)
        
        idx = db.open_search(ctx, "my_index", t)
        idx.add("doc1", "hello world")
        idx.add("doc2", "hello python")
        
        t.commit(ctx)
        
        # Search in new transaction
        t = db.begin_transaction(ctx)
        idx = db.open_search(ctx, "my_index", t)
        
        results = idx.search("hello")
        self.assertEqual(len(results), 2)
        
        results = idx.search("python")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].DocID, "doc2")
        
        t.commit(ctx)

if __name__ == '__main__':
    unittest.main()
