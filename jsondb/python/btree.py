import uuid

from typing import TypeVar, Generic
from dataclasses import dataclass, asdict

from transaction import Transaction

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


MIN_CACHE_DURATION = timedelta(minutes=5)


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
    is_unique: bool
    slot_length: int
    description: str
    value_size: ValueDataSize
    cache_config: CacheConfig


@dataclass
class Item(Generic[TK, TV]):
    key: TK
    value: TV
    id: uuid.uuid4 = uuid.UUID(int=0)


class Btree(Generic[TK, TV]):

    # @staticmethod
    # def comparerBuilder()

    @staticmethod
    def new(options: BtreeOptions, trans: Transaction):
        """
        Create a new B-tree in the backend storage with the options specified then return an instance
        of Btree that can let caller code to manage the items.
        """

        b3 = Btree()
        return b3

    @classmethod
    def open(name: str, trans: Transaction):
        b3 = Btree()
        return b3

    def add(self, items: list[Item[TK, TV]]) -> bool:
        return False

    def add_if_not_exists(self, items: list[Item[TK, TV]]) -> bool:
        return False

    def update(self, items: list[Item[TK, TV]]) -> bool:
        return False

    def upsert(self, items: list[Item[TK, TV]]) -> bool:
        return False

    def remove(self, keys: list[TK]) -> bool:
        return False

    def get_items(
        self, page_number: int, page_size: int, direction: PagingDirection
    ) -> list[Item[TK, TV]]:
        return None

    def get_values(
        self, page_number: int, page_size: int, direction: PagingDirection
    ) -> list[TV]:
        return None

    def get_keys(
        self, page_number: int, page_size: int, direction: PagingDirection
    ) -> list[TK]:
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
