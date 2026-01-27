import unittest
import json
import uuid
from unittest.mock import patch, MagicMock
from sop.context import Context
from sop.database import Database, DatabaseOptions, DatabaseAction
from sop.transaction import RedisCacheConfig, DatabaseType

class TestMultiDbRedisConfig(unittest.TestCase):
    def test_multi_db_redis_isolation(self):
        """
        Verify that multiple Database instances with different Redis configurations
        pass distinct configuration payloads to the underlying Go layer.
        """
        ctx = Context()
        
        # Configuration 1
        url1 = "redis://host1:6379/0"
        db_opts1 = DatabaseOptions(
            keyspace="db1",
            type=DatabaseType.Clustered, 
            redis_config=RedisCacheConfig(url=url1)
        )
        db1 = Database(db_opts1)

        # Configuration 2
        url2 = "redis://host2:6379/0"
        db_opts2 = DatabaseOptions(
            keyspace="db2",
            type=DatabaseType.Clustered,
            redis_config=RedisCacheConfig(url=url2)
        )
        db2 = Database(db_opts2)

        # We mock manage_database. 
        # When db.begin_transaction() is called, it triggers _ensure_database_created,
        # which calls NewDatabase (Action 1) with the JSON payload.
        # We want to capture that payload.
        
        with patch('sop.call_go.manage_database') as mock_manage:
            # Mock return values for NewDatabase (UUID) and BeginTransaction (UUID)
            def side_effect(ctx_id, action, db_id, payload):
                if action == DatabaseAction.NewDatabase.value:
                    return str(uuid.uuid4())
                if action == DatabaseAction.BeginTransaction.value:
                    return str(uuid.uuid4())
                return None
            
            mock_manage.side_effect = side_effect

            # Trigger DB1
            db1.begin_transaction(ctx)
            
            # Find the call for NewDatabase for DB1
            # We expect manage_database(ctx.id, 1, None, payload)
            # Filter calls where action == 1
            calls = mock_manage.mock_calls
            new_db_calls = [c for c in calls if c.args[1] == DatabaseAction.NewDatabase.value]
            
            self.assertGreaterEqual(len(new_db_calls), 1)
            
            # Get the payload from the first call (DB1)
            # args: (ctx_id, action, db_id, payload)
            # payload is args[3]
            payload1_json = new_db_calls[0].args[3]
            payload1 = json.loads(payload1_json)
            
            # Verify DB1 Config
            self.assertIn("redis_config", payload1)
            self.assertEqual(payload1["redis_config"]["url"], url1)
            # Verify keyspace to be sure it's DB1
            self.assertEqual(payload1["keyspace"], "db1")

            # Reset mock to clear history (optional, but cleaner)
            mock_manage.reset_mock()
            mock_manage.side_effect = side_effect

            # Trigger DB2
            db2.begin_transaction(ctx)
            
            # Find the call for NewDatabase for DB2
            calls = mock_manage.mock_calls
            new_db_calls = [c for c in calls if c.args[1] == DatabaseAction.NewDatabase.value]
            
            self.assertGreaterEqual(len(new_db_calls), 1)
            
            payload2_json = new_db_calls[0].args[3]
            payload2 = json.loads(payload2_json)
            
            # Verify DB2 Config
            self.assertIn("redis_config", payload2)
            self.assertEqual(payload2["redis_config"]["url"], url2)
             # Verify keyspace
            self.assertEqual(payload2["keyspace"], "db2")
            
            # Explicitly assert they are different
            self.assertNotEqual(payload1["redis_config"]["url"], payload2["redis_config"]["url"])

if __name__ == '__main__':
    unittest.main()
