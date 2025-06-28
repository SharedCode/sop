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


class TestBtree(unittest.TestCase):
    def setUpClass():
        ro = RedisOptions()
        Redis.open_connection(ro)

    def tearDownClass():
        Redis.close_connection()

    def test_new_btree(self):
        to = transaction.TransationOptions(
            transaction.TransactionMode.ForWriting.value,
            5,
            transaction.MIN_HASH_MOD_VALUE,
            stores_folders,
            ec,
        )

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
        print("test new")

    def test_open_btree(self):
        to = transaction.TransationOptions(
            transaction.TransactionMode.ForWriting.value,
            5,
            transaction.MIN_HASH_MOD_VALUE,
            stores_folders,
            ec,
        )

        t = transaction.Transaction(to)
        t.begin()

        b3 = btree.Btree.open("barstoreec", True, t)
        l = [
            btree.Item(1, "foo"),
        ]
        if b3.add(l):
            print("add should have failed.")

        t.commit()
        print("test open")

    def test_add_if_not_exists(self):
        to = transaction.TransationOptions(
            transaction.TransactionMode.ForWriting.value,
            5,
            transaction.MIN_HASH_MOD_VALUE,
            stores_folders,
            ec,
        )

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
        to = transaction.TransationOptions(
            transaction.TransactionMode.ForWriting.value,
            5,
            transaction.MIN_HASH_MOD_VALUE,
            stores_folders,
            ec,
        )

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
        to = transaction.TransationOptions(
            transaction.TransactionMode.ForWriting.value,
            5,
            transaction.MIN_HASH_MOD_VALUE,
            stores_folders,
            ec,
        )

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
