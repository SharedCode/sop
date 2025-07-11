import json
import uuid
import logging
from . import call_go
from . import context

logger = logging.getLogger(__name__)

from typing import TypeVar, Generic, Type
from dataclasses import dataclass, asdict

from . import transaction

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
class IndexFieldSpecification:
    """
    Make sure to specify correct field name so Key data comparer will be able to make a good comparison
    between two Items when organizing Key/Value item pairs in the b-tree.

    Otherwise it will treat all of item's to have the same key field value and thus, will affect both
    performance and give incorrect ordering of the items.
    """

    field_name: str
    ascending_sort_order: bool = True


@dataclass
class IndexSpecification:
    """
    Index Specification lists the fields comprising the index on the Key class & their sort order.
    """

    index_fields: IndexFieldSpecification = None


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
    index_specification: str = ""
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


class BtreeError(transaction.TransactionError):
    """Exception for Btree-related errors, 'is derived from transaction.TransactionError."""

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
    """
    B-tree manager. See "new" & "open" class methods below for details how to use.
    Delegates API calls to the SOP library that does Direct IO to disk drives w/ built-in L1/L2 caching.

    Args:
        Generic (TK, TV): TK - type of the Key part. TV - type of the Value part.
    """

    transaction_id: uuid.uuid4
    id: uuid.uuid4

    def __init__(self, id: uuid.uuid4, is_primitive_key: bool, tid: uuid.uuid4):
        self.id = id
        self.is_primitive_key = is_primitive_key
        self.transaction_id = tid

    @classmethod
    def new(
        cls: Type["Btree[TK,TV]"],
        ctx: context.Context,
        options: BtreeOptions,
        trans: transaction.Transaction,
        index_spec: IndexSpecification = None,
    ) -> "Btree[TK,TV]":
        """Create a new B-tree store in the backend storage with the options specified then returns an instance
        of Python Btree (facade) that can let caller code to manage or search/fetch  the items of the store.

        Args:
            cls (Type[&quot;Btree[TK,TV]&quot;]): Supports generics for Key (TK) & Value (TV) pair.
            ctx (context.Context): context.Context object, useful for telling SOP in the backend the ID of the context for use in calls.
            options (BtreeOptions): _description_
            trans (transaction.Transaction): instance of a transaction.Transaction that the B-tree store to be opened belongs.
            index_spec (IndexSpecification, optional): Defaults to None.

        Raises:
            BtreeError: error message pertaining to creation of a b-tree store related in the backend.

        Returns:
            Btree[TK,TV]: B-tree instance
        """

        options.transaction_id = str(trans.transaction_id)
        if index_spec is not None:
            options.index_specification = json.dumps(asdict(index_spec))

        res = call_go.manage_btree(
            ctx.id, BtreeAction.NewBtree.value, json.dumps(asdict(options)), None
        )

        if res == None:
            raise BtreeError("unable to create a Btree in SOP")
        try:
            b3id = uuid.UUID(res)
        except:
            # if res can't be converted to UUID, it is expected to be an error msg from SOP.
            raise BtreeError(res)

        return cls(b3id, options.is_primitive_key, trans.transaction_id)

    @classmethod
    def open(
        cls: Type["Btree[TK,TV]"], ctx: context.Context, name: str, trans: transaction.Transaction
    ) -> "Btree[TK,TV]":
        """Open an existing B-tree store on the backend and returns a B-tree manager that can allow code to do operations on it.
        Args:
            cls (Type[&quot;Btree[TK,TV]&quot;]): Supports generics for Key (TK) & Value (TV) pair.
            ctx (context.Context): context.Context object, useful for telling SOP in the backend the ID of the context for use in calls.
            name (str): Name of the B-tree store to open.
            trans (transaction.Transaction): instance of a transaction.Transaction that the B-tree store to be opened belongs.

        Raises:
            BtreeError: error message generated when calling different methods of the B-tree.

        Returns:
            Btree[TK,TV]: Btree instance
        """
        options: BtreeOptions = BtreeOptions(name=name)
        options.transaction_id = str(trans.transaction_id)
        res = call_go.manage_btree(
            ctx.id, BtreeAction.OpenBtree.value, json.dumps(asdict(options)), ""
        )

        if res == None:
            raise BtreeError(f"unable to open a Btree:{name} in SOP")
        try:
            b3id = uuid.UUID(res)
        except:
            # if res can't be converted to UUID, it is expected to be an error msg from SOP.
            raise BtreeError(res)

        return cls(b3id, False, trans.transaction_id)

    def add(self, ctx: context.Context, items: Item[TK, TV]) -> bool:
        return self._manage(ctx, BtreeAction.Add.value, items)

    def add_if_not_exists(self, ctx: context.Context, items: Item[TK, TV]) -> bool:
        return self._manage(ctx, BtreeAction.AddIfNotExist.value, items)

    def update(self, ctx: context.Context, items: Item[TK, TV]) -> bool:
        return self._manage(ctx, BtreeAction.Update.value, items)

    def upsert(self, ctx: context.Context, items: Item[TK, TV]) -> bool:
        return self._manage(ctx, BtreeAction.Upsert.value, items)

    def remove(self, ctx: context.Context, keys: TK) -> bool:
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        res = call_go.manage_btree(
            ctx.id,
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
        ctx: context.Context,
        pagingInfo: PagingInfo,
    ) -> Item[TK, TV]:
        """Fetch items from the B-tree store.

        Args:
            ctx (context.Context): context.Context object
            pagingInfo (PagingInfo): Paging details that describe how to navigate(walk the b-tree) & fetch a batch of items.

        Returns: Item[TK, TV]: the fetched batch of items.
        """
        return self._get(ctx, BtreeAction.GetItems.value, pagingInfo)

    # Keys array contains the keys & their IDs to fetch value data of. Both input & output
    # are Item type though input only has Key & ID field populated to find the data & output has Value of found data.
    def get_values(self, ctx: context.Context, keys: Item[TK, TV]) -> Item[TK, TV]:
        """Fetch items' Value parts from the B-tree store.

        Args:
            ctx (context.Context): context.Context object
            keys (Item[TK, TV]): Keys that which, their Value parts will be fetched from the store.

        Raises:
            BtreeError: error message containing details what failed during (Values) fetch.

        Returns: Item[TK, TV]: items containing the fetched (Values) data.
        """
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        payload: ManageBtreePayload = ManageBtreePayload(items=keys)
        result, error = call_go.get_from_btree(
            ctx.id,
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
        ctx: context.Context,
        pagingInfo: PagingInfo,
    ) -> Item[TK, TV]:
        """Fetch a set of Keys from the store.

        Args:
            ctx (context.Context): context.Context object used when calling the SOP b-tree methods.
            pagingInfo (PagingInfo): Paging details specifying how to navigate/walk the b-tree and fetch keys.

        Returns: Item[TK, TV]: items with the Keys populated with the fetched data.
        """
        return self._get(ctx, BtreeAction.GetKeys.value, pagingInfo)

    def find(self, ctx: context.Context, key: TK) -> bool:
        """Find will navigate the b-tree and stop when the matching item, its Key matches with the key param, is found.
        Or a nearby item if there is no match found.
        This positions the cursor so on succeeding call to one or the fetch methods, get_keys, get_values, get_items, will
        fetch the data (items) starting from this cursor (item) position.

        Args:
            ctx (context.Context): _description_
            key (TK): key of the item to search for.

        Raises:
            BtreeError: error message pertaining to the error encountered while searching for the item.

        Returns: bool: true means the item with such key is found, false otherwise. If true is returned, the cursor is positioned
        to the item with matching key, otherwise to an item with key similar to the requested key.
        """
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        payload: ManageBtreePayload = ManageBtreePayload(items=(Item(key=key),))
        res = call_go.navigate_btree(
            ctx.id,
            BtreeAction.Find.value,
            json.dumps(asdict(metadata)),
            json.dumps(asdict(payload)),
        )
        if res == None:
            raise BtreeError(
                f"unable to Find using key:{key} the item of a Btree:{self.id} in SOP"
            )

        return self._to_bool(res)

    def find_with_id(self, ctx: context.Context, key: TK, id: uuid.uuid4) -> bool:
        """Similar to Find but which, includes the ID of the item to search for. This is useful when there are duplicated (on key) items.
        Code can then search for the right one despite having many items of same key, based on the item ID.

        Args:
            ctx (context.Context): _description_
            key (TK): _description_
            id (uuid.uuid4): _description_

        Raises:
            BtreeError: _description_

        Returns:
            bool: _description_
        """
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        payload: ManageBtreePayload = ManageBtreePayload(items=(Item(key=key, id=id),))
        res = call_go.navigate_btree(
            ctx.id,
            BtreeAction.FindWithID.value,
            json.dumps(asdict(metadata)),
            json.dumps(asdict(payload)),
        )
        if res == None:
            raise BtreeError(
                f"unable to Find using key:{key} & ID:{id} the item of a Btree:{self.id} in SOP"
            )

        return self._to_bool(res)

    def first(
        self,
        ctx: context.Context,
    ) -> bool:
        """Navigate or positions the cursor to the first or top of the b-tree store.

        Args:
            ctx (context.Context): _description_

        Raises:
            BtreeError: _description_

        Returns:
            bool: _description_
        """
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        res = call_go.navigate_btree(
            ctx.id, BtreeAction.First.value, json.dumps(asdict(metadata)), None
        )
        if res == None:
            raise BtreeError(f"First call failed for Btree:{self.id} in SOP")

        return self._to_bool(res)

    def last(
        self,
        ctx: context.Context,
    ) -> bool:
        """Navigate or positions the cursor to the last or bottom of the b-tree store.

        Args:
            ctx (context.Context): _description_

        Raises:
            BtreeError: _description_

        Returns:
            bool: _description_
        """
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        res = call_go.navigate_btree(
            ctx.id, BtreeAction.Last.value, json.dumps(asdict(metadata)), None
        )
        if res == None:
            raise BtreeError(f"Last call failed for Btree:{self.id} in SOP")

        return self._to_bool(res)

    def is_unique(self) -> bool:
        """true specifies that the b-tree store has no duplicated keyed items. false otherwise.

        Raises:
            BtreeError: _description_

        Returns:
            bool: _description_
        """
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
        """Returns the count of items in the b-tree store.

        Raises:
            BtreeError: _description_

        Returns:
            int: _description_
        """
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
        """Returns the b-tree information as specified during creation (Btree.new method call).

        Raises:
            BtreeError: _description_

        Returns:
            BtreeOptions: _description_
        """
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        payload, error = call_go.get_from_btree(
            0, BtreeAction.GetStoreInfo.value, json.dumps(asdict(metadata)), None
        )
        if error is not None:
            raise BtreeError(error)

        data_dict = json.loads(payload)
        return BtreeOptions(**data_dict)

    def _manage(self, ctx: context.Context, action: int, items: Item[TK, TV]) -> bool:
        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        payload: ManageBtreePayload = ManageBtreePayload(items=items)
        res = call_go.manage_btree(
            ctx.id,
            action,
            json.dumps(asdict(metadata)),
            json.dumps(asdict(payload)),
        )
        if res == None:
            raise BtreeError(f"unable to manage item to a Btree:{self.id} in SOP")

        return self._to_bool(res)

    def _get(
        self,
        ctx: context.Context,
        getAction: int,
        pagingInfo: PagingInfo,
    ) -> Item[TK, TV]:
        if pagingInfo.direction > PagingDirection.Backward.value:
            pagingInfo.direction = PagingDirection.Backward.value
        if pagingInfo.direction < PagingDirection.Forward.value:
            pagingInfo.direction = PagingDirection.Forward.value

        metadata: ManageBtreeMetaData = ManageBtreeMetaData(
            is_primitive_key=self.is_primitive_key,
            btree_id=str(self.id),
            transaction_id=str(self.transaction_id),
        )
        result, error = call_go.get_from_btree(
            ctx.id,
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
