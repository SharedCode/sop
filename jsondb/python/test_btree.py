import json
import pytest
import unittest
import transaction
import btree

from datetime import timedelta
from redis import *

from dataclasses import dataclass, asdict

stores_folders = ("/Users/grecinto/sop_data/disk1", "/Users/grecinto/sop_data/disk2")
ec = {
    # Erasure Config default entry(key="") will allow different B-tree(tables) to share same EC structure.
    "": transaction.ErasureCodingConfig(
        2,
        1,
        (
            "/Users/grecinto/sop_data/disk1",
            "/Users/grecinto/sop_data/disk2",
            "/Users/grecinto/sop_data/disk3",
        ),
        True,
    )
}

# Run unit tests in cmdline:
# python3 -m unittest -v


@dataclass
class pKey:
    key: str


to = transaction.TransationOptions(
    transaction.TransactionMode.ForWriting.value,
    5,
    transaction.MIN_HASH_MOD_VALUE,
    stores_folders,
    ec,
)


class TestBtree(unittest.TestCase):
    def setUpClass():
        ro = RedisOptions()
        Redis.open_connection(ro)

        t = transaction.Transaction(to)
        t.begin()

        cache = btree.CacheConfig()
        bo = btree.BtreeOptions("barstoreec", True, cache_config=cache)
        bo.set_value_data_size(btree.ValueDataSize.Small)

        b3 = btree.Btree.new(bo, True, t)
        l = [
            btree.Item(1, "foo"),
        ]
        b3.add(l)

        t.commit()
        print("new B3 succeeded")

    def test_add_if_not_exists(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("barstoreec", True, t)
        l = [
            btree.Item(1, "foo"),
        ]
        if b3.add_if_not_exists(l):
            print("addIfNotExists should have failed.")

        t.commit()
        print("test add_if_not_exists")

    def test_add_if_not_exists_mapkey(self):
        t = transaction.Transaction(to)
        t.begin()

        cache = btree.CacheConfig()
        bo = btree.BtreeOptions("barstoreec_mk", True, cache_config=cache)
        bo.set_value_data_size(btree.ValueDataSize.Small)

        b3 = btree.Btree.new(bo, False, t)

        pk = pKey("foo")
        l = [
            btree.Item(pk, "foo"),
        ]
        if b3.add_if_not_exists(l) == False:
            print("addIfNotExistsMapkey should fail.")

        t.commit()
        print("test add_if_not_exists")

    def test_add_if_not_exists_mapkey_fail(self):
        t = transaction.Transaction(to)
        t.begin()

        cache = btree.CacheConfig()
        bo = btree.BtreeOptions("barstoreec_mk2", True, cache_config=cache)
        bo.set_value_data_size(btree.ValueDataSize.Small)

        b3 = btree.Btree.new(bo, False, t)

        l = [
            btree.Item(1, "foo"),
        ]
        try:
            if b3.add_if_not_exists(l) == False:
                print("addIfNotExistsMapkey should fail.")
            pytest.fail("SHOULD NOT REACH THIS.")
        except:
            pass

        t.commit()
        print("test add_if_not_exists mapkey fail case")

    def test_get_items(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("barstoreec", True, t)
        res = b3.get_items(0, 5, btree.PagingDirection.Forward)
        print(f"get_items succeeded {res}.")

        t.commit()

    def test_get_keys(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("barstoreec", True, t)
        res = b3.get_keys(0, 5, btree.PagingDirection.Forward)
        print(f"get_keys succeeded {res}.")

        t.commit()

    def test_get_values(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("barstoreec", True, t)
        keys = b3.get_keys(0, 5, btree.PagingDirection.Forward)
        res = b3.get_values(keys)

        print(f"get_values succeeded {res}.")

        t.commit()

    def test_find(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("barstoreec", True, t)
        res = b3.find(1)

        print(f"find succeeded {res}.")

        t.commit()

    def test_find_with_id(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("barstoreec", True, t)
        keys = b3.get_keys(0, 5, btree.PagingDirection.Forward)
        res = b3.find_with_id(keys[0].key, keys[0].id)

        print(f"find with id succeeded {res}.")

        t.commit()

    def test_goto_first(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("barstoreec", True, t)
        res = b3.first()

        print(f"goto first succeeded {res}.")
        t.commit()

    def test_goto_last(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("barstoreec", True, t)
        res = b3.last()

        print(f"goto last succeeded {res}.")
        t.commit()

    def test_is_unique(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("barstoreec", True, t)
        res = b3.is_unique()

        print(f"is_unique succeeded {res}.")
        t.commit()

    def test_count(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("barstoreec", True, t)
        res = b3.count()

        print(f"count succeeded {res}.")
        t.commit()

    def test_get_store_info(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("barstoreec", True, t)
        res = b3.get_store_info()

        print(f"storeInfo: {res}")
        t.commit()


class TestBtreeMapKey(unittest.TestCase):
    def setUpClass():
        ro = RedisOptions()
        Redis.open_connection(ro)

        t = transaction.Transaction(to)
        t.begin()

        cache = btree.CacheConfig()
        bo = btree.BtreeOptions("foobar", True, cache_config=cache)
        bo.set_value_data_size(btree.ValueDataSize.Small)

        b3 = btree.Btree.new(bo, False, t)
        l = [
            btree.Item(pKey(key="123"), "foo"),
        ]
        b3.add(l)

        t.commit()
        print("new B3 succeeded")

    def test_add_if_not_exists(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("foobar", False, t)
        l = [
            btree.Item(pKey(key="123"), "foo"),
        ]
        if b3.add_if_not_exists(l):
            print("addIfNotExists should have failed.")

        t.commit()
        print("test add_if_not_exists")

    def test_get_items(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("foobar", False, t)
        res = b3.get_items(0, 5, btree.PagingDirection.Forward)
        print(f"get_items succeeded {res}.")

        t.commit()

    def test_get_keys(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("foobar", False, t)
        res = b3.get_keys(0, 5, btree.PagingDirection.Forward)
        print(f"get_keys succeeded {res}.")

        t.commit()

    def test_get_values(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("foobar", False, t)
        keys = b3.get_keys(0, 5, btree.PagingDirection.Forward)
        res = b3.get_values(keys)

        print(f"get_values succeeded {res}.")

        t.commit()

    def test_find(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("foobar", False, t)
        res = b3.find(pKey(key="123"))

        print(f"find succeeded {res}.")

        t.commit()

    def test_find_with_id(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("foobar", False, t)
        keys = b3.get_keys(0, 5, btree.PagingDirection.Forward)
        res = b3.find_with_id(keys[0].key, keys[0].id)

        print(f"find with id succeeded {res}.")

        t.commit()

    def test_goto_first(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("foobar", False, t)
        res = b3.first()

        print(f"goto first succeeded {res}.")
        t.commit()

    def test_goto_last(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("foobar", False, t)
        res = b3.last()

        print(f"goto last succeeded {res}.")
        t.commit()

    def test_is_unique(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("foobar", False, t)
        res = b3.is_unique()

        print(f"is_unique succeeded {res}.")
        t.commit()

    def test_count(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("foobar", False, t)
        res = b3.count()

        print(f"count succeeded {res}.")
        t.commit()

    def test_get_store_info(self):
        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("foobar", False, t)
        res = b3.get_store_info()

        print(f"storeInfo: {res}")
        t.commit()
