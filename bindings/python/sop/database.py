import json
import uuid
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
from enum import Enum

from . import call_go
from . import context
from . import transaction
from .btree import Btree, BtreeOptions, IndexSpecification
from .search import Index
from .transaction import DBType


class DatabaseAction(Enum):
    NewDatabase = 1
    BeginTransaction = 2
    NewBtree = 3
    OpenBtree = 4
    OpenModelStore = 5
    OpenVectorStore = 6
    OpenSearch = 7

class Database:
    def __init__(self, ctx: context.Context, storage_path: str = "", db_type: DBType = DBType.Standalone, 
                 erasure_config: Optional[Dict[str, transaction.ErasureCodingConfig]] = None,
                 stores_folders: Optional[List[str]] = None):
        
        self.storage_path = storage_path # Store for reference
        
        # We reuse ModelDBOptions structure on Go side which expects storage_path and db_type
        opts = {
            "storage_path": storage_path,
            "db_type": db_type.value
        }
        
        if erasure_config:
            # Convert ErasureCodingConfig objects to dicts if needed, or rely on asdict/json serialization
            # Assuming transaction.ErasureCodingConfig is a dataclass
            opts["erasure_config"] = {k: asdict(v) for k, v in erasure_config.items()}
            
        if stores_folders:
            opts["stores_folders"] = stores_folders

        payload = json.dumps(opts)
        
        # Action NewDatabase
        res = call_go.manage_database(ctx.id, DatabaseAction.NewDatabase.value, None, payload)
        try:
            self.id = uuid.UUID(res)
        except:
            raise Exception(res)

    def begin_transaction(self, ctx: context.Context, mode: int = 1, options: Optional[transaction.TransactionOptions] = None) -> transaction.Transaction:
        # We use manage_database BeginTransaction
        if options:
            payload = json.dumps(asdict(options))
        else:
            payload = str(mode)
            
        res = call_go.manage_database(ctx.id, DatabaseAction.BeginTransaction.value, str(self.id), payload)
        try:
            tid = uuid.UUID(res)
            return transaction.Transaction(ctx, id=tid, begun=True, database_id=self.id)
        except:
            raise Exception(res)


    def new_btree(self, ctx: context.Context, name: str, trans: transaction.Transaction, options: Optional[BtreeOptions] = None, index_spec: IndexSpecification = None) -> Btree:
        return Btree.new(ctx, name, trans, options, index_spec)

    def open_btree(self, ctx: context.Context, name: str, trans: transaction.Transaction) -> Btree:
        return Btree.open(ctx, name, trans)

    def open_search(self, ctx: context.Context, name: str, trans: transaction.Transaction) -> Index:
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
