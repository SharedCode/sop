import unittest
import transaction
import btree
import context

from redis import *
from test_btree import to

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
    def setUpClass():
        ro = RedisOptions()
        Redis.open_connection(ro)

        t = transaction.Transaction(ctx, to)
        t.begin()

        cache = btree.CacheConfig()
        bo = btree.BtreeOptions("personidx", True, cache_config=cache)
        bo.set_value_data_size(btree.ValueDataSize.Small)
        bo.is_primitive_key = False
        btree.Btree.new(
            ctx,
            bo,
            t,
            # specify the Index fields of the Key class. You control how many fields get included
            # and each field's sort order (asc or desc).
            btree.IndexSpecification(
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
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "personidx", t)

        pk = Key(address1="123 main st", address2="Fremont, CA")
        l = [btree.Item(pk, Person(first_name="joe", last_name="petit"))]

        for i in range(20):
            pk = Key(address1=f"{i}123 main st", address2="Fremont, CA")
            l.append(btree.Item(pk, Person(first_name=f"joe{i}", last_name="petit")))

        b3.add_if_not_exists(ctx, l)

        t.commit(ctx)

    def test_get_items_batch(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "personidx", t)
        items = b3.get_items(
            ctx,
            btree.PagingInfo(0, 0, 10, direction=btree.PagingDirection.Forward.value),
        )
        print(f"read items from indexed keyed b-tree {items}")

        t.commit(ctx)
