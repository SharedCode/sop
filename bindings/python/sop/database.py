import json
import uuid
from dataclasses import asdict
from typing import Dict, Optional
from enum import Enum

from . import call_go
from . import context
from . import transaction
from .btree import Btree, BtreeOptions, IndexSpecification
from .search import Index
from .transaction import DatabaseType, DatabaseOptions, RedisCacheConfig


class DatabaseAction(Enum):
    NewDatabase = 1
    BeginTransaction = 2
    NewBtree = 3
    OpenBtree = 4
    OpenModelStore = 5
    OpenVectorStore = 6
    OpenSearch = 7
    RemoveBtree = 8
    RemoveModelStore = 9
    RemoveVectorStore = 10
    RemoveSearch = 11
    SetupDatabase = 12
    GetDatabaseOptions = 13

class Database:
    """
    Represents a database in the SOP library.
    """
    def __init__(self, options: DatabaseOptions):
        """
        Initializes a new Database instance.

        Args:
            options (DatabaseOptions): The options for the database.
        """
        self.options = options
        self.id = None

    @staticmethod
    def _deserialize_opts(json_str: str) -> Optional[DatabaseOptions]:
        try:
            if not json_str.strip().startswith('{'):
                return None
            
            # Since Go JSON tags match Python snake_case arguments, we can use the dict directly.
            # We just need to filter out unknown keys (like cache_type) and hydrate objects/enums.
            data = json.loads(json_str)

            # 1. Transform Nested Objects (Dict -> Object)
            if "redis_config" in data and isinstance(data["redis_config"], dict):
                # Only pass known fields to constructor to be safe
                valid_rc = RedisCacheConfig.__dataclass_fields__.keys()
                rc_args = {k: v for k, v in data["redis_config"].items() if k in valid_rc}
                data["redis_config"] = RedisCacheConfig(**rc_args)

            if "erasure_config" in data and isinstance(data["erasure_config"], dict):
                valid_ec = ErasureCodingConfig.__dataclass_fields__.keys()
                ec_map = {}
                for k, v in data["erasure_config"].items():
                    if isinstance(v, dict):
                        ec_args = {ik: iv for ik, iv in v.items() if ik in valid_ec}
                        ec_map[k] = ErasureCodingConfig(**ec_args)
                data["erasure_config"] = ec_map

            # 2. Transform Enum (Int -> Enum)
            if "type" in data:
                data["type"] = DatabaseType(data["type"])

            # 3. Filter top-level keys (remove 'cache_type' etc. that Python doesn't have)
            valid_db_keys = DatabaseOptions.__dataclass_fields__.keys()
            input_db_kwargs = {k: v for k, v in data.items() if k in valid_db_keys}
            
            return DatabaseOptions(**input_db_kwargs)
        except:
            return None

    @staticmethod
    def setup(ctx: context.Context, options: DatabaseOptions):
        """
        Persists the database options to the stores folders.
        This is a one-time setup operation for the database.
        
        It parses the returned JSON options (if any) from the Go Setup implementation,
        and updates the passed options object with the updated values.

        Args:
            ctx (context.Context): The context.
            options (DatabaseOptions): The options for the database.
        """
        opts = Database._serialize_options(options)
        payload = json.dumps(opts)
        
        res = call_go.manage_database(ctx.id, DatabaseAction.SetupDatabase.value, None, payload)
        if res:
            updated = Database._deserialize_opts(res)
            if updated:
                for field in DatabaseOptions.__dataclass_fields__:
                    val = getattr(updated, field)
                    if val is not None:
                        setattr(options, field, val)
                return

            if not res.strip().startswith('{'):
                raise Exception(res)

    @staticmethod
    def _serialize_options(options: DatabaseOptions) -> Dict:
        opts = {
            "type": options.type.value
        }
        
        if options.erasure_config:
            opts["erasure_config"] = {k: asdict(v) for k, v in options.erasure_config.items()}
            
        if options.stores_folders:
            opts["stores_folders"] = options.stores_folders

        if options.keyspace:
            opts["keyspace"] = options.keyspace

        if options.redis_config:
            opts["redis_config"] = asdict(options.redis_config)
            opts["cache_type"] = 2 # Redis enum value in Go
            
        return opts

    @staticmethod
    def get_options(ctx: context.Context, store_path: str) -> Optional[DatabaseOptions]:
        """
        Retrieves the database options from the given store path.

        Args:
            ctx (context.Context): The context.
            store_path (str): The path to the store.

        Returns:
            Optional[DatabaseOptions]: The database options if found, otherwise None.
        """
        res = call_go.manage_database(ctx.id, DatabaseAction.GetDatabaseOptions.value, None, store_path)
        if res:
            try:
                if res.strip().startswith('{'):
                    raw_opts = json.loads(res)
                    
                    # Convert Go PascalCase keys to Python snake_case
                    def to_snake(k):
                        return ''.join(['_'+c.lower() if c.isupper() else c for c in k]).lstrip('_')

                    data = {to_snake(k): v for k, v in raw_opts.items()}
                    
                    # Fixup Enums and Nested Objects
                    if "type" in data:
                        data["type"] = DatabaseType(data["type"])
                    
                    if "redis_config" in data and data["redis_config"]:
                        rc_data = {to_snake(k): v for k, v in data["redis_config"].items()}
                        # Filter keys to match RedisCacheConfig fields
                        valid_keys = RedisCacheConfig.__dataclass_fields__.keys()
                        input_kwargs = {k: v for k, v in rc_data.items() if k in valid_keys}
                        data["redis_config"] = RedisCacheConfig(**input_kwargs)
                        
                    if "erasure_config" in data and data["erasure_config"]:
                        ec_map = {}
                        valid_keys = ErasureCodingConfig.__dataclass_fields__.keys()
                        for k, v in data["erasure_config"].items():
                            ec_data = {to_snake(ik): iv for ik, iv in v.items()}
                            input_kwargs = {ik: iv for ik, iv in ec_data.items() if ik in valid_keys}
                            ec_map[k] = ErasureCodingConfig(**input_kwargs)
                        data["erasure_config"] = ec_map

                    # Safe Instantiation
                    # Filter top-level keys against DatabaseOptions fields
                    valid_db_keys = DatabaseOptions.__dataclass_fields__.keys()
                    input_db_kwargs = {k: v for k, v in data.items() if k in valid_db_keys}
                    
                    return DatabaseOptions(**input_db_kwargs)
            except Exception as e:
                # Fallthrough to error handling or re-raise if it was a JSON parse success but logic error?
                # For now, treat any exception as a failure to satisfy "options found" logic or explicit error.
                pass
            
            # If we received an error string that is not JSON
            if not res.strip().startswith('{'):
                raise Exception(res)
        return None

    def _ensure_database_created(self, ctx: context.Context):
        if self.id:
            return

        opts = Database._serialize_options(self.options)
        payload = json.dumps(opts)
        
        # Action NewDatabase
        res = call_go.manage_database(ctx.id, DatabaseAction.NewDatabase.value, None, payload)
        try:
            self.id = uuid.UUID(res)
        except:
            raise Exception(res)

    def begin_transaction(self, ctx: context.Context, mode: int = 1, max_time: int = 15) -> transaction.Transaction:
        """
        Begins a new transaction.

        Args:
            ctx (context.Context): The context.
            mode (int, optional): The transaction mode. Defaults to 1.
            max_time (int, optional): The maximum duration of the transaction in minutes. Defaults to 15.

        Returns:
            transaction.Transaction: The new transaction.

        Raises:
            Exception: If the transaction cannot be begun.
        """
        self._ensure_database_created(ctx)
        
        # We only need to send mode and max_time
        opts = {
            "mode": mode,
            "max_time": max_time
        }
        payload = json.dumps(opts)
            
        res = call_go.manage_database(ctx.id, DatabaseAction.BeginTransaction.value, str(self.id), payload)
        try:
            tid = uuid.UUID(res)
            return transaction.Transaction(ctx, id=tid, begun=True, database_id=self.id)
        except:
            raise Exception(res)


    def new_btree(self, ctx: context.Context, name: str, trans: transaction.Transaction, options: Optional[BtreeOptions] = None, index_spec: IndexSpecification = None) -> Btree:
        """
        Creates a new B-Tree.

        Args:
            ctx (context.Context): The context.
            name (str): The name of the B-Tree.
            trans (transaction.Transaction): The transaction.
            options (Optional[BtreeOptions], optional): The B-Tree options. Defaults to None.
            index_spec (IndexSpecification, optional): The index specification. Defaults to None.

        Returns:
            Btree: The new B-Tree.
        """
        self._ensure_database_created(ctx)
        return Btree.new(ctx, name, trans, options, index_spec)

    def open_btree(self, ctx: context.Context, name: str, trans: transaction.Transaction) -> Btree:
        """
        Opens an existing B-Tree.

        Args:
            ctx (context.Context): The context.
            name (str): The name of the B-Tree.
            trans (transaction.Transaction): The transaction.

        Returns:
            Btree: The opened B-Tree.
        """
        self._ensure_database_created(ctx)
        return Btree.open(ctx, name, trans)

    def open_search(self, ctx: context.Context, name: str, trans: transaction.Transaction) -> Index:
        """
        Opens a search index.

        Args:
            ctx (context.Context): The context.
            name (str): The name of the search index.
            trans (transaction.Transaction): The transaction.

        Returns:
            Index: The opened search index.

        Raises:
            Exception: If the search index cannot be opened.
        """
        self._ensure_database_created(ctx)
        opts = {
            "transaction_id": str(trans.transaction_id),
            "name": name
        }
        payload = json.dumps(opts)
        res = call_go.manage_database(ctx.id, DatabaseAction.OpenSearch.value, str(self.id), payload)
        try:
            # res is the UUID of the opened store
            return Index(ctx, res, str(trans.transaction_id))
        except:
            raise Exception(res)

    def remove_btree(self, ctx: context.Context, name: str) -> None:
        """
        Removes a B-Tree.

        Args:
            ctx (context.Context): The context.
            name (str): The name of the B-Tree.

        Raises:
            Exception: If the B-Tree cannot be removed.
        """
        self._ensure_database_created(ctx)
        # Action RemoveBtree (Action 8)
        # Payload is just the name
        res = call_go.manage_database(ctx.id, DatabaseAction.RemoveBtree.value, str(self.id), name)
        if res is not None:
            raise Exception(res)

