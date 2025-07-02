import json
import uuid
import logging
import call_go

logger = logging.getLogger(__name__)

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
    # Count of elements to fetch starting with the page offset. If left 0, 'will fetch PageSize number of elements
    # after traversing the B-tree, bringing the cursor to the requested page offset.
    # Otherwise, will fetch FetchCount number of data elements starting from the page offset.
    fetch_count: int = 0
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
    cel_expression: str = ""
    transaction_id: str = str(uuid.UUID(int=0))
    cache_config: CacheConfig = None
    is_primitive_key: bool = True
    leaf_load_balancing: bool = False

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
    value: TV = None
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
        cls: Type["Btree[TK,TV]"], name: str, trans: Transaction
    ) -> "Btree[TK,TV]":
        options: BtreeOptions = BtreeOptions(name=name)
        options.transaction_id = str(trans.transaction_id)
        res = call_go.manage_btree(
            BtreeAction.OpenBtree.value, json.dumps(asdict(options)), ""
        )

        if res == None:
            raise BtreeError(f"unable to open a Btree:{name} in SOP")
        try:
            b3id = uuid.UUID(res)
        except:
            # if res can't be converted to UUID, it is expected to be an error msg from SOP.
            raise BtreeError(res)

        return cls(b3id, False, trans.transaction_id)

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
            raise BtreeError(
                f"unable to remove item w/ key:{keys} from a Btree:{self.id} in SOP"
            )
        return self._to_bool(res)

    def get_items(
        self,
        pagingInfo: PagingInfo,
    ) -> Item[TK, TV]:
        return self._get(BtreeAction.GetItems.value, pagingInfo)

    # Keys array contains the keys & their IDs to fetch value data of. Both input & output
    # are Item type though input only has Key & ID field populated to find the data & output has Value of found data.
    def get_values(self, keys: Item[TK, TV]) -> Item[TK, TV]:
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        payload: ManageBtreePayload = ManageBtreePayload(items=keys)
        result, error = call_go.get_from_btree(
            BtreeAction.GetValues.value,
            json.dumps(asdict(metadata)),
            json.dumps(asdict(payload)),
        )
        if error is not None:
            if result is None:
                raise BtreeError(error)
            else:
                # just log the error since there are partial results we can return.
                logger.error(error)

        if result is None:
            return None

        data_dicts = json.loads(result)
        return [Item[TK, TV](**data_dict) for data_dict in data_dicts]

    def get_keys(
        self,
        pagingInfo: PagingInfo,
    ) -> Item[TK, TV]:
        return self._get(BtreeAction.GetKeys.value, pagingInfo)

    def find(self, key: TK) -> bool:
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        payload: ManageBtreePayload = ManageBtreePayload(items=(Item(key=key),))
        res = call_go.navigate_btree(
            BtreeAction.Find.value,
            json.dumps(asdict(metadata)),
            json.dumps(asdict(payload)),
        )
        if res == None:
            raise BtreeError(
                f"unable to Find using key:{key} the item of a Btree:{self.id} in SOP"
            )

        return self._to_bool(res)

    def find_with_id(self, key: TK, id: uuid.uuid4) -> bool:
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        payload: ManageBtreePayload = ManageBtreePayload(items=(Item(key=key, id=id),))
        res = call_go.navigate_btree(
            BtreeAction.FindWithID.value,
            json.dumps(asdict(metadata)),
            json.dumps(asdict(payload)),
        )
        if res == None:
            raise BtreeError(
                f"unable to Find using key:{key} & ID:{id} the item of a Btree:{self.id} in SOP"
            )

        return self._to_bool(res)

    def first(self) -> bool:
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        res = call_go.navigate_btree(
            BtreeAction.First.value, json.dumps(asdict(metadata)), None
        )
        if res == None:
            raise BtreeError(f"First call failed for Btree:{self.id} in SOP")

        return self._to_bool(res)

    def last(self) -> bool:
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        res = call_go.navigate_btree(
            BtreeAction.Last.value, json.dumps(asdict(metadata)), None
        )
        if res == None:
            raise BtreeError(f"Last call failed for Btree:{self.id} in SOP")

        return self._to_bool(res)

    def is_unique(self) -> bool:
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        res = call_go.is_unique_btree(
            json.dumps(asdict(metadata)),
        )
        if res == None:
            raise BtreeError(f"IsUnique call failed for Btree:{self.id} in SOP")

        return self._to_bool(res)

    def count(self) -> int:
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        count, error = call_go.get_btree_item_count(
            json.dumps(asdict(metadata)),
        )
        if error is not None:
            raise BtreeError(error)
        return count

    def get_store_info(self) -> BtreeOptions:
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        payload, error = call_go.get_from_btree(
            BtreeAction.GetStoreInfo.value, json.dumps(asdict(metadata)), None
        )
        if error is not None:
            raise BtreeError(error)

        data_dict = json.loads(payload)
        return BtreeOptions(**data_dict)

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
            raise BtreeError(f"unable to manage item to a Btree:{self.id} in SOP")

        return self._to_bool(res)

    def _get(
        self,
        getAction: int,
        pagingInfo: PagingInfo,
    ) -> Item[TK, TV]:
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        result, error = call_go.get_from_btree(
            getAction,
            json.dumps(asdict(metadata)),
            json.dumps(asdict(pagingInfo)),
        )
        if error is not None:
            if result is None:
                raise BtreeError(error)
            else:
                # just log the error since there are partial results we can return.
                logger.error(error)

        if result is None:
            return None

        data_dicts = json.loads(result)
        return [Item[TK, TV](**data_dict) for data_dict in data_dicts]

    def _to_bool(self, res: str) -> bool:
        """
        Converts result string "true" or "false" to bool or an errmsg to raise an error.
        """
        if res.lower() == "true":
            return True
        if res.lower() == "false":
            return False

        raise BtreeError(res)
