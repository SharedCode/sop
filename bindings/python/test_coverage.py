import unittest
import uuid
from unittest.mock import patch
from sop import context, redis
from sop.database import DatabaseOptions
from sop.transaction import ErasureCodingConfig, Transaction, TransactionError

class TestCoverage(unittest.TestCase):

    def test_erasure_coding_config_equality(self):
        ec1 = ErasureCodingConfig(2, 1, ["/tmp/d1", "/tmp/d2", "/tmp/d3"], True)
        ec2 = ErasureCodingConfig(2, 1, ["/tmp/d1", "/tmp/d2", "/tmp/d3"], True)
        ec3 = ErasureCodingConfig(2, 1, ["/tmp/d1", "/tmp/d2", "/tmp/d4"], True)
        
        self.assertEqual(ec1, ec2)
        self.assertNotEqual(ec1, ec3)
        self.assertEqual(hash(ec1), hash(ec2))

    def test_transaction_init_error(self):
        ctx = context.Context()
        # Test deprecated init
        with self.assertRaises(TransactionError):
            Transaction(ctx)

    def test_transaction_begin_error(self):
        ctx = context.Context()
        # Mock call_go to return error
        with patch('sop.call_go.manage_transaction', return_value="Some Error"):
            # We need to bypass init check to test begin error, or use a valid transaction object
            # But we can't create one easily without mocking.
            # Let's mock a transaction object
            t = Transaction(ctx, id=uuid.uuid4())
            with self.assertRaises(TransactionError):
                t.begin()

    def test_transaction_commit_error(self):
        ctx = context.Context()
        t = Transaction(ctx, id=uuid.uuid4(), begun=True)
        with patch('sop.call_go.manage_transaction', return_value="Commit Error"):
            with self.assertRaises(TransactionError):
                t.commit(ctx)

    def test_transaction_rollback_error(self):
        ctx = context.Context()
        t = Transaction(ctx, id=uuid.uuid4(), begun=True)
        with patch('sop.call_go.manage_transaction', return_value="Rollback Error"):
            with self.assertRaises(TransactionError):
                t.rollback(ctx)

    def test_transaction_context_manager_rollback_on_exception(self):
        ctx = context.Context()
        t = Transaction(ctx, id=uuid.uuid4(), begun=True)
        
        # Mock rollback to succeed
        with patch('sop.call_go.manage_transaction', return_value=None) as mock_manage:
            try:
                with t:
                    raise ValueError("Test Exception")
            except ValueError:
                pass
            
            # Verify rollback was called (Action 4)
            # manage_transaction(ctx.id, 4, str(t.transaction_id))
            # But wait, begin is called first (Action 2)
            # So calls: begin (2), rollback (4)
            # We need to check if rollback was called.
            # The mock is called multiple times.
            # We can check if any call was with action 4.
            found_rollback = False
            for call in mock_manage.call_args_list:
                if call[0][1] == 4:
                    found_rollback = True
                    break
            self.assertTrue(found_rollback)

    def test_redis_connection_error(self):
        with patch('sop.call_go.open_redis_connection', return_value="Connection Error"):
            with self.assertRaises(Exception):
                redis.Redis.initialize("redis://invalid")

    def test_redis_close_error(self):
        with patch('sop.call_go.close_redis_connection', return_value="Close Error"):
            with self.assertRaises(Exception):
                redis.Redis.close()

    def test_context_methods(self):
        ctx = context.Context()
        self.assertTrue(ctx.is_valid())
        self.assertIsNone(ctx.error())
        
        ctx.cancel()
        # After cancel, is_valid might still be true if error is not set immediately or if cancel just sets a flag on Go side.
        # But we can check if cancel was called.
        
        # Test destructor
        del ctx

    def test_model_store_open_error(self):
        ctx = context.Context()
        t = Transaction(ctx, id=uuid.uuid4(), begun=True, database_id=uuid.uuid4())
        with patch('sop.call_go.manage_database', side_effect=[str(uuid.uuid4()), "Open Error"]):
            with self.assertRaises(Exception):
                from sop.database import Database
                # We need a database instance to call open_model_store
                # But we can mock the database instance or just call the method if we can instantiate it.
                # Easier to just mock the call inside Database.open_model_store
                db = Database()
                db.open_model_store(ctx, t, "store")

    def test_model_store_save_error(self):
        ctx = context.Context()
        ms = self._create_mock_model_store()
        with patch('sop.call_go.manage_model_store', return_value="Save Error"):
            with self.assertRaises(Exception):
                ms.save(ctx, "cat", "name", {"data": 1})

    def test_model_store_get_error(self):
        ctx = context.Context()
        ms = self._create_mock_model_store()
        with patch('sop.call_go.manage_model_store', return_value=None):
            with self.assertRaises(Exception):
                ms.get(ctx, "cat", "name")
        
        with patch('sop.call_go.manage_model_store', return_value="Invalid JSON"):
            with self.assertRaises(Exception):
                ms.get(ctx, "cat", "name")

    def test_model_store_delete_error(self):
        ctx = context.Context()
        ms = self._create_mock_model_store()
        with patch('sop.call_go.manage_model_store', return_value="Delete Error"):
            with self.assertRaises(Exception):
                ms.delete(ctx, "cat", "name")

    def test_model_store_list_error(self):
        ctx = context.Context()
        ms = self._create_mock_model_store()
        with patch('sop.call_go.manage_model_store', return_value="Invalid JSON"):
            with self.assertRaises(Exception):
                ms.list(ctx, "cat")

    def _create_mock_model_store(self):
        from sop.ai.model import ModelStore
        return ModelStore(uuid.uuid4(), uuid.uuid4())

    def test_database_init_error(self):
        ctx = context.Context()
        with patch('sop.call_go.manage_database', return_value="Init Error"):
            with self.assertRaises(Exception):
                from sop.database import Database
                db = Database(DatabaseOptions())
                db._ensure_database_created(ctx)

    def test_database_begin_transaction_error(self):
        ctx = context.Context()
        # Mock successful init
        with patch('sop.call_go.manage_database', side_effect=[str(uuid.uuid4()), "Begin Error"]):
            from sop.database import Database
            db = Database(DatabaseOptions())
            
            with self.assertRaises(Exception):
                db.begin_transaction(ctx)

    def test_database_open_model_store_error(self):
        ctx = context.Context()
        t = Transaction(ctx, id=uuid.uuid4(), begun=True, database_id=uuid.uuid4())
        with patch('sop.call_go.manage_database', side_effect=[str(uuid.uuid4()), "Open Error"]):
            from sop.database import Database
            db = Database(DatabaseOptions())
            with self.assertRaises(Exception):
                db.open_model_store(ctx, t, "store")

    def test_database_open_vector_store_error(self):
        ctx = context.Context()
        t = Transaction(ctx, id=uuid.uuid4(), begun=True, database_id=uuid.uuid4())
        with patch('sop.call_go.manage_database', side_effect=[str(uuid.uuid4()), "Open Error"]):
            from sop.database import Database
            db = Database(DatabaseOptions())
            with self.assertRaises(Exception):
                db.open_vector_store(ctx, t, "store")

if __name__ == '__main__':
    unittest.main()
