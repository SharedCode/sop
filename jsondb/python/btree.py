import json
import uuid
import call_go

from typing import TypeVar, Generic, Type
from dataclasses import dataclass, asdict

from transaction import Transaction, TransactionError

# Define TypeVars 'TK' & 'TV' to represent Key & Value generic types.
TK = TypeVar("TK")
TV = TypeVar("TV")

from datetime import timedelta

from enum import Enum, auto


class ValueDataSize(Enum):
    Small = 0
    Medium = 1
    Big = 2


class PagingDirection(Enum):
    Forward = 0
    Backward = 1


@dataclass
class PagingInfo:
    """
    Paging Info is used to package paging details to SOP.
    """

    # Offset defaults to 0, meaning, fetch from current cursor position in SOP backend.
    page_offset: int = 0
    # Fetch size of 20 is default.
    page_size: int = 20
    # Fetch direction of forward(0) is default.
    direction: int = 0


@dataclass
class CacheConfig:
    """
    Cache config specify the options available for caching in B-tree.
    """

    # Registry cache duration in minutes.
    registry_cache_duration: int = 10
    is_registry_cache_ttl: bool = False
    node_cache_duration: int = 5
    is_node_cache_ttl: bool = False
    store_info_cache_duration: int = 5
    is_store_info_cache_ttl: bool = False
    value_data_cache_duration: int = 0
    is_value_data_cache_ttl: bool = False


@dataclass
class BtreeOptions:
    """
    Btree options specify the options available for making a B-tree.
    """

    name: str
    is_unique: bool = False
    slot_length: int = 500
    description: str = ""
    is_value_data_in_node_segment: bool = True
    is_value_data_actively_persisted: bool = False
    is_value_data_globally_cached: bool = False
    cel_expressions: str = ""
    transaction_id: str = str(uuid.UUID(int=0))
    cache_config: CacheConfig = None
    is_primitive_key: bool = True

    def set_value_data_size(self, s: ValueDataSize):
        if s == ValueDataSize.Medium:
            self.is_value_data_actively_persisted = False
            self.is_value_data_globally_cached = True
            self.is_value_data_in_node_segment = False
        if s == ValueDataSize.Big:
            self.is_value_data_actively_persisted = True
            self.is_value_data_globally_cached = False
            self.is_value_data_in_node_segment = False


@dataclass
class Item(Generic[TK, TV]):
    key: TK
    value: TV
    id: str = str(uuid.UUID(int=0))


class BtreeError(TransactionError):
    """Exception for Btree-related errors, 'is derived from TransactionError."""

    pass


@dataclass
class ManageBtreeMetaData(Generic[TK, TV]):
    is_primitive_key: bool
    transaction_id: str
    btree_id: str


@dataclass
class ManageBtreePayload(Generic[TK, TV]):
    items: Item[TK, TV]


class BtreeAction(Enum):
    NewBtree = auto()
    OpenBtree = auto()
    Add = auto()
    AddIfNotExist = auto()
    Update = auto()
    Upsert = auto()
    Remove = auto()
    Find = auto()
    FindWithID = auto()
    GetItems = auto()
    GetValues = auto()
    GetKeys = auto()
    First = auto()
    Last = auto()
    IsUnique = auto()
    Count = auto()
    GetStoreInfo = auto()


class Btree(Generic[TK, TV]):
    transaction_id: uuid.uuid4
    id: uuid.uuid4

    def __init__(self, id: uuid.uuid4, is_primitive_key: bool, tid: uuid.uuid4):
        self.id = id
        self.is_primitive_key = is_primitive_key
        self.transaction_id = tid

    @classmethod
    def new(
        cls: Type["Btree[TK,TV]"],
        options: BtreeOptions,
        is_primitive_key: bool,
        trans: Transaction,
    ) -> "Btree[TK,TV]":
        """
        Create a new B-tree in the backend storage with the options specified then return an instance
        of Python Btree (facade) that can let caller code to manage the items.
        """

        options.transaction_id = str(trans.transaction_id)

        res = call_go.manage_btree(1, json.dumps(asdict(options)), "")

        if res == None:
            raise TransactionError("unable to create a Btree object in SOP")
        try:
            b3id = uuid.UUID(res)
        except:
            # if res can't be converted to UUID, it is expected to be an error msg from SOP.
            raise TransactionError(res)

        options.transaction_id = str(trans.transaction_id)
        options.is_primitive_key = is_primitive_key

        res = call_go.manage_btree(
            BtreeAction.NewBtree.value, json.dumps(asdict(options)), ""
        )

        if res == None:
            raise BtreeError("unable to create a Btree in SOP")
        try:
            b3id = uuid.UUID(res)
        except:
            # if res can't be converted to UUID, it is expected to be an error msg from SOP.
            raise BtreeError(res)

        return cls(b3id, is_primitive_key, trans.transaction_id)

    @classmethod
    def open(
        cls: Type["Btree[TK,TV]"], name: str, is_primitive_key: bool, trans: Transaction
    ) -> "Btree[TK,TV]":
        options: BtreeOptions = BtreeOptions(name=name)
        options.transaction_id = str(trans.transaction_id)
        options.is_primitive_key = is_primitive_key
        res = call_go.manage_btree(
            BtreeAction.OpenBtree.value, json.dumps(asdict(options)), ""
        )

        if res == None:
            raise BtreeError("unable to open a Btree in SOP")
        try:
            b3id = uuid.UUID(res)
        except:
            # if res can't be converted to UUID, it is expected to be an error msg from SOP.
            raise BtreeError(res)

        return cls(b3id, is_primitive_key, trans.transaction_id)

    def add(self, items: Item[TK, TV]) -> bool:
        return self._manage(BtreeAction.Add.value, items)

    def add_if_not_exists(self, items: Item[TK, TV]) -> bool:
        return self._manage(BtreeAction.AddIfNotExist.value, items)

    def update(self, items: Item[TK, TV]) -> bool:
        return self._manage(BtreeAction.Update.value, items)

    def upsert(self, items: Item[TK, TV]) -> bool:
        return self._manage(BtreeAction.Upsert.value, items)

    def remove(self, keys: TK) -> bool:
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        res = call_go.manage_btree(
            BtreeAction.Remove.value,
            json.dumps(asdict(metadata)),
            json.dumps(asdict(keys)),
        )
        if res == None:
            raise BtreeError("unable to remove item from a Btree in SOP")
        return self._return_result(res)

    def get_items(
        self, page_offset: int, page_size: int, direction: PagingDirection
    ) -> Item[TK, TV]:
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        pagingInfo = PagingInfo(
            page_offset=page_offset, page_size=page_size, direction=direction.value
        )
        result, error = call_go.get_from_btree(
            BtreeAction.GetItems.value,
            json.dumps(asdict(metadata)),
            json.dumps(asdict(pagingInfo)),
        )
        if error is not None:
            raise BtreeError(f"{error}")

        print(f"getItems result: {result}")

        return None
        # data_dict = json.loads(result)

        # print(f"getItems result: {data_dict}")

        # return Item[TK, TV](**data_dict)

    def get_values(self, keys: TK) -> TV:
        return None

    def get_keys(
        self, page_offset: int, page_size: int, direction: PagingDirection
    ) -> TK:
        return None

    def find(self, key: TK, first_item_with_key: bool) -> bool:
        return False

    def find_with_id(self, key: TK, id: uuid.uuid4) -> bool:
        return False

    def first(self) -> bool:
        return False

    def last(self) -> bool:
        return False

    def is_unique(self) -> bool:
        return False

    def count(self) -> int:
        return 0

    def get_store_info(self) -> BtreeOptions:
        return BtreeOptions()

    def _manage(self, action: int, items: Item[TK, TV]) -> bool:
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        payload: ManageBtreePayload = ManageBtreePayload(items=items)
        res = call_go.manage_btree(
            action,
            json.dumps(asdict(metadata)),
            json.dumps(asdict(payload)),
        )
        if res == None:
            raise BtreeError("unable to manage item to a Btree in SOP")

        return self._return_result(res)

    def _return_result(self, res: str) -> bool:
        if res.lower() == "true":
            return True
        if res.lower() == "false":
            return False

        raise BtreeError(res)
