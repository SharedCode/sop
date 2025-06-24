import uuid

from typing import TypeVar, Generic

from jsondb.python.transaction import Transaction

# Define TypeVars 'TK' & 'TV' to represent Key & Value generic types.
TK = TypeVar("TK")
TV = TypeVar("TV")

from datetime import timedelta

from enum import Enum


class ValueDataSize(Enum):
    Small = 0
    Medium = 1
    Big = 2


class PagingDirection(Enum):
    Forward = 0
    Backward = 1


MIN_CACHE_DURATION = 5 * timedelta.minute


class CacheConfig:
    """
    Cache config specify the options available for caching in B-tree.
    """

    def __init__(self, cache_duration: timedelta, is_ttl: bool):
        if cache_duration > 0 and cache_duration < MIN_CACHE_DURATION:
            cache_duration = MIN_CACHE_DURATION
        if cache_duration == 0 and is_ttl:
            is_ttl = False
        self.registry_cache_duration = cache_duration
        self.is_registry_cache_ttl = is_ttl
        self.node_cache_duration = cache_duration
        self.is_node_cache_ttl = is_ttl
        self.store_info_cache_duration = cache_duration
        self.is_store_info_cache_ttl = is_ttl
        self.value_data_cache_duration = cache_duration
        self.is_value_data_cache_ttl = is_ttl


class BtreeOptions:
    """
    Btree options specify the options available for making a B-tree.
    """

    def __init__(
        self,
        name: str,
        is_unique: bool,
        slot_length: int,
        desc: str,
        value_size: ValueDataSize,
        cache_config: CacheConfig,
    ):
        self.name = name
        self.is_unique = is_unique
        self.slot_length = slot_length
        self.desc = desc
        # Defaults to Small data size.
        self.is_value_data_in_node_segment = True
        self.is_value_data_globally_cached = False
        self.is_value_data_actively_persisted = False
        if value_size == ValueDataSize.Medium:
            self.is_value_data_in_node_segment = False
            self.is_value_data_globally_cached = True
        if value_size == ValueDataSize.Big:
            self.is_value_data_in_node_segment = False
            self.is_value_data_globally_cached = False
            self.is_value_data_actively_persisted = True
        self.cache_config = cache_config


class Item(Generic[TK, TV]):
    def __init__(
        self, key: TK = None, value: TV = None, id: uuid.uuid4 = uuid.UUID(int=0)
    ):
        self.key = key
        self.value = value
        self.id = id


class Btree(Generic[TK, TV]):

    # @staticmethod
    # def comparerBuilder()

    @staticmethod
    def new(options: BtreeOptions, trans: Transaction):
        Btree()

    @staticmethod
    def open(name: str, trans: Transaction):
        return self

    @classmethod
    def add(items: list[Item[TK, TV]]) -> bool:
        return false

    @classmethod
    def add_if_not_exists(items: list[Item[TK, TV]]) -> bool:
        return false

    @classmethod
    def update(items: list[Item[TK, TV]]) -> bool:
        return false

    @classmethod
    def upsert(items: list[Item[TK, TV]]) -> bool:
        return false

    @classmethod
    def remove(keys: list[TK]) -> bool:
        return false

    @classmethod
    def get_items(
        page_number: int, page_size: int, direction: PagingDirection
    ) -> list[Item[TK, TV]]:
        return false

    @classmethod
    def find(key: TK, firstItemWithKey: bool) -> bool:
        return false

    @classmethod
    def find_with_id(key: TK, id: uuid.uuid4) -> bool:
        return false

    @classmethod
    def first() -> bool:
        return false

    @classmethod
    def last() -> bool:
        return false

    @classmethod
    def is_unique() -> bool:
        return false

    @classmethod
    def count() -> numpy.int64:
        return 0

    @classmethod
    def get_store_info() -> BtreeOptions:
        return BtreeOptions()
