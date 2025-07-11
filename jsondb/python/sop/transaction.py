import json
from . import call_go
import uuid
from . import context

from enum import Enum
from dataclasses import dataclass, asdict


class TransactionMode(Enum):
    """
    Transaction mode enumeration.

    Args:
        Enum (_type_):
        NoCheck - No check on commit, most performant but does NOT guarantee ACIDity
        ForWriting - Mode for writing/updates/deletes.
        ForReading - Mode for reading only. Checks all read items' version # on commit to guarantee ACIDity.
    """

    NoCheck = 0
    ForWriting = 1
    ForReading = 2


# 250, should generate 1MB file segment. Formula: 250 X 4096 = 1MB
# Given a 50 slot size per node, should be able to manage 825,000 B-Tree items (key/value pairs).
#
# Formula: 250 * 66 * 50 = 825,000
# Or if you use 100 slot size per node, 'will give you 1,650,000 items, or assuming you have about 65%
# b-tree utilization, 1,072,500 usable space.
MIN_HASH_MOD_VALUE = 250
# 750k, should generate 3GB file segment.  Formula: 750k X 4096 = 3GB
MAX_HASH_MOD_VALUE = 750000


@dataclass
class ErasureCodingConfig:
    """
    Erasure Coding config is used to package the parameter configuration of Reed Solomon based EC replication.
    This is a special algorithm for replication allowing full operational capability even if you lose a half of your
    storage drives. :)

    For example, if you have 100% redundancy on four drives, losing two drives SOP will still be able to give you Read & Write.
    BUT of course, your IT needs to replace the drives and allow SOP to auto-reconstruct the redundant "shards" so your setup
    can offer tolerance once again.

    Returns:
        _type_: _description_
    """

    data_shards_count: int
    parity_shards_count: int
    base_folder_paths_across_drives: str
    repair_corrupted_shards: bool

    def __eq__(self, other):
        return (
            self.data_shards_count == other.data_shards_count
            and self.parity_shards_count == other.parity_shards_count
            and self.repair_corrupted_shards == other.repair_corrupted_shards
            and self.base_folder_paths_across_drives
            == other.base_folder_paths_across_drives
        )

    def __hash__(self):
        return hash(
            (
                self.data_shards_count,
                self.parity_shards_count,
                self.base_folder_paths_across_drives,
                self.repair_corrupted_shards,
            )
        )


@dataclass
class TransationOptions:
    mode: int
    # max_time in Python is in minutes, SOP in Golang will convert that to respective time.duration value.
    max_time: int
    # Registry hash mod, minimum value is 250, max is 750000. Hash mod is used on Registry map on disk.
    # At 250, 1MB segment file is generated. See comment about the equivalent in Golang side (for now).
    registry_hash_mod: int
    # Stores' base folder path (home folder).
    stores_folders: str
    # EC config.
    erasure_config: dict[str, ErasureCodingConfig]


class TransactionError(Exception):
    """Base exception for transaction-related errors."""

    pass


class InvalidTransactionStateError(TransactionError):
    """Raised when a transaction is attempted in an invalid state."""

    pass


class Transaction:
    """
    Transaction object is used to manage transaction (begin, commit, rollback).
    Delegates API calls to the SOP library that does Direct IO to disk drives w/ built-in L1/L2 caching.
    """

    def __init__(self, ctx: context.Context, options: TransationOptions):
        self.options = options
        self.transaction_id = uuid.UUID(int=0)

        res = call_go.manage_transaction(ctx.id, 1, json.dumps(asdict(options)))

        if res == None:
            raise TransactionError("unable to create a Tranasaction object in SOP")
        try:
            self.transaction_id = uuid.UUID(res)
        except:
            # if res can't be converted to UUID, it is expected to be an error msg from SOP.
            raise TransactionError(res)

    def begin(self):
        if self.transaction_id == uuid.UUID(int=0):
            raise InvalidTransactionStateError("transaction_id is missing")
        res = call_go.manage_transaction(0, 2, str(self.transaction_id))
        if res != None:
            raise TransactionError(f"Transaction begin failed, details {res}")

    def commit(
        self,
        ctx: context.Context,
    ):
        """Commit will finalize the transaction, all b-tree management operations to the backend storage. The committed changes
        will start to reflect on succeeding transactions b-tree store fetches/operations.

        Args:
            ctx (context.Context): _description_

        Raises:
            InvalidTransactionStateError: _description_
            TransactionError: _description_
        """
        if self.transaction_id == uuid.UUID(int=0):
            raise InvalidTransactionStateError("transaction_id is missing")
        res = call_go.manage_transaction(ctx.id, 3, str(self.transaction_id))
        if res != None:
            raise TransactionError(f"Transaction commit failed, details {res}")

    def rollback(
        self,
        ctx: context.Context,
    ):
        """Undo or rollback the changes done within the transaction.

        Args:
            ctx (context.Context): _description_

        Raises:
            InvalidTransactionStateError: _description_
            TransactionError: _description_
        """
        if self.transaction_id == uuid.UUID(int=0):
            raise InvalidTransactionStateError("transaction_id is missing")
        res = call_go.manage_transaction(ctx.id, 4, str(self.transaction_id))
        if res != None:
            raise TransactionError(f"Transaction rollback failed, details {res}")
