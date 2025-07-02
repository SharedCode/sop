import pytest
import unittest
import transaction
import btree

from redis import *
from test_btree import to

from dataclasses import dataclass, asdict


@dataclass
class Key:
    address1: str
    address2: str


@dataclass
class Person:
    first_name: str
    last_name: str


class TestBtreeIndexSpecs(unittest.TestCase):
    def setUpClass():
        ro = RedisOptions()
        Redis.open_connection(ro)

        t = transaction.Transaction(to)
        t.begin()

        cache = btree.CacheConfig()
        bo = btree.BtreeOptions("personidx", True, cache_config=cache)
        bo.set_value_data_size(btree.ValueDataSize.Small)
        bo.is_primitive_key = False
        btree.Btree.new(
            bo,
            t,
            btree.IndexSpecification(
                index_fields=(
                    btree.IndexFieldSpecification(
                        "address1", ascending_sort_order=False
                    ),
                    btree.IndexFieldSpecification("address2"),
                )
            ),
        )

        t.commit()

    def test_add(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("personidx", t)

        pk = Key(address1="123 main st", address2="Fremont, CA")
        l = [btree.Item(pk, Person(first_name="joe", last_name="petit"))]

        for i in range(20):
            pk = Key(address1=f"{i}123 main st", address2="Fremont, CA")
            l.append(btree.Item(pk, Person(first_name=f"joe{i}", last_name="petit")))

        b3.add_if_not_exists(l)

        t.commit()

    def test_get_items_batch(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("personidx", t)
        items = b3.get_items(
            btree.PagingInfo(0, 0, 10, direction=btree.PagingDirection.Forward.value)
        )
        print(f"read items from indexed keyed b-tree {items}")

        t.commit()
