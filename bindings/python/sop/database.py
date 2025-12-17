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
from .transaction import DatabaseType, DatabaseOptions


class DatabaseAction(Enum):
    NewDatabase = 1
    BeginTransaction = 2
    NewBtree = 3
    OpenBtree = 4
    OpenModelStore = 5
    OpenVectorStore = 6
    OpenSearch = 7
    RemoveBtree = 8

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

    def _ensure_database_created(self, ctx: context.Context):
        if self.id:
            return

        # We reuse ModelDBOptions structure on Go side which expects storage_path and type
        opts = {
            "type": self.options.type.value
        }
        
        if self.options.erasure_config:
            # Convert ErasureCodingConfig objects to dicts if needed, or rely on asdict/json serialization
            opts["erasure_config"] = {k: asdict(v) for k, v in self.options.erasure_config.items()}
            
        if self.options.stores_folders:
            opts["stores_folders"] = self.options.stores_folders

        if self.options.keyspace:
            opts["keyspace"] = self.options.keyspace

        if self.options.redis_config:
            opts["redis_config"] = asdict(self.options.redis_config)
            # Ensure cache_type is set to Redis if config is present
            opts["cache_type"] = 2 # Redis enum value in Go

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

