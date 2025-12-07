import unittest
from . import btree
from . import context
from .ai import Database, DatabaseType
from .database import DatabaseOptions

from .redis import *
from .test_btree import to, stores_folders, ec

from dataclasses import dataclass


@dataclass
class Key:
    address1: str
    address2: str


@dataclass
class Person:
    first_name: str
    last_name: str


# create a context for use in Transaction & B-tree API calls.
ctx = context.Context()


class TestBtreeIndexSpecs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # ro = RedisOptions()
        # Redis.open_connection("redis://localhost:6379")

        # Initialize DB
        cls.db = Database(DatabaseOptions(type=DatabaseType.Standalone, stores_folders=list(stores_folders), erasure_config=ec))

        t = cls.db.begin_transaction(ctx, mode=to.mode, max_time=to.max_time)

        cache = btree.CacheConfig()
        bo = btree.BtreeOptions("personidx", True, cache_config=cache)
        bo.set_value_data_size(btree.ValueDataSize.Small)
        bo.is_primitive_key = False
        cls.db.new_btree(
            ctx,
            "personidx",
            t,
            options=bo,
            # specify the Index fields of the Key class. You control how many fields get included
            # and each field's sort order (asc or desc).
            index_spec=btree.IndexSpecification(
                index_fields=(
                    btree.IndexFieldSpecification(
                        "address1", ascending_sort_order=False
                    ),
                    btree.IndexFieldSpecification("address2"),
                )
            ),
        )

        t.commit(ctx)

    def test_add(self):
        t = self.db.begin_transaction(ctx, mode=to.mode, max_time=to.max_time)

        b3 = self.db.open_btree(ctx, "personidx", t)

        pk = Key(address1="123 main st", address2="Fremont, CA")
        l = [btree.Item(pk, Person(first_name="joe", last_name="petit"))]

        for i in range(20):
            pk = Key(address1=f"{i}123 main st", address2="Fremont, CA")
            l.append(btree.Item(pk, Person(first_name=f"joe{i}", last_name="petit")))

        b3.add_if_not_exists(ctx, l)

        t.commit(ctx)

    def test_get_items_batch(self):
        t = self.db.begin_transaction(ctx, mode=to.mode, max_time=to.max_time)

        b3 = self.db.open_btree(ctx, "personidx", t)
        items = b3.get_items(
            ctx,
            btree.PagingInfo(0, 0, 10, direction=btree.PagingDirection.Forward.value),
        )
        print(f"read items from indexed keyed b-tree {items}")

        t.commit(ctx)
