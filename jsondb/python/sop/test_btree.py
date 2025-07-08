import pytest
import unittest
import transaction
import btree
import context

from datetime import timedelta
from redis import *

from dataclasses import dataclass

# Stores home folder(s). Replication requires two paths, one for active and the 2nd, the passive.
stores_folders = ("/Users/grecinto/sop_data/disk1", "/Users/grecinto/sop_data/disk2")
# EC configuration specifies the Erasure Coding parameters like:
# "data shards count" (2), "parity shards count" (1), folder paths (disk1, disk2, disk3) where the shards &
# parities data file will be stored. And a flag (True) whether to auto-repair any shard that failed to read.
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


@dataclass
class Person:
    first_name: str
    last_name: str


# Transaction Options (to).
to = transaction.TransationOptions(
    transaction.TransactionMode.ForWriting.value,
    5,
    transaction.MIN_HASH_MOD_VALUE,
    stores_folders,
    ec,
)

# Context object.
ctx = context.Context()


class TestBtree(unittest.TestCase):
    def setUpClass():

        # initialize SOP global Redis connection
        ro = RedisOptions()
        Redis.open_connection(ro)

        # create the "barstoreec" b-tree store.
        t = transaction.Transaction(ctx, to)
        t.begin()

        cache = btree.CacheConfig()
        bo = btree.BtreeOptions("barstoreec", True, cache_config=cache)
        bo.set_value_data_size(btree.ValueDataSize.Small)

        b3 = btree.Btree.new(ctx, bo, t)
        l = [
            btree.Item(1, "foo"),
        ]
        b3.add(ctx, l)

        # commit the transaction to finalize the new store changes.
        t.commit(ctx)
        print("new B3 succeeded")

    def test_add_if_not_exists(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "barstoreec", t)
        l = [
            btree.Item(1, "foo"),
        ]
        if b3.add_if_not_exists(ctx, l):
            print("addIfNotExists should have failed.")

        t.commit(ctx)
        print("test add_if_not_exists")

    def test_add_if_not_exists_mapkey(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        cache = btree.CacheConfig()
        bo = btree.BtreeOptions("barstoreec_mk", True, cache_config=cache)
        bo.set_value_data_size(btree.ValueDataSize.Small)
        bo.is_primitive_key = False

        b3 = btree.Btree.new(ctx, bo, t)

        pk = pKey("foo")
        l = [
            btree.Item(pk, "foo"),
        ]
        if b3.add_if_not_exists(ctx, l) == False:
            print("addIfNotExistsMapkey should fail.")

        t.commit(ctx)
        print("test add_if_not_exists")

    def test_add_if_not_exists_mapkey_fail(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        cache = btree.CacheConfig()
        bo = btree.BtreeOptions("barstoreec_mk2", True, cache_config=cache)
        bo.set_value_data_size(btree.ValueDataSize.Small)
        bo.is_primitive_key = False

        b3 = btree.Btree.new(ctx, bo, t)

        l = [
            btree.Item(1, "foo"),
        ]
        try:
            if b3.add_if_not_exists(ctx, l) == False:
                print("addIfNotExistsMapkey should fail.")
            pytest.fail("SHOULD NOT REACH THIS.")
        except:
            pass

        t.commit(ctx)
        print("test add_if_not_exists mapkey fail case")

    def test_get_items(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "barstoreec", t)
        res = b3.get_items(
            ctx, btree.PagingInfo(0, 5, direction=btree.PagingDirection.Forward.value)
        )
        print(f"get_items succeeded {res}.")

        t.commit(ctx)

    def test_get_keys(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "barstoreec", t)
        res = b3.get_keys(
            ctx, btree.PagingInfo(0, 5, direction=btree.PagingDirection.Forward.value)
        )
        print(f"get_keys succeeded {res}.")

        t.commit(ctx)

    def test_get_values(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "barstoreec", t)
        keys = b3.get_keys(
            ctx, btree.PagingInfo(0, 5, direction=btree.PagingDirection.Forward.value)
        )
        res = b3.get_values(ctx, keys)

        print(f"get_values succeeded {res}.")

        t.commit(ctx)

    def test_find(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "barstoreec", t)
        res = b3.find(ctx, 1)

        print(f"find succeeded {res}.")

        t.commit(ctx)

    def test_find_with_id(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "barstoreec", t)
        keys = b3.get_keys(
            ctx, btree.PagingInfo(0, 5, direction=btree.PagingDirection.Forward.value)
        )
        res = b3.find_with_id(ctx, keys[0].key, keys[0].id)

        print(f"find with id succeeded {res}.")

        t.commit(ctx)

    def test_goto_first(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "barstoreec", t)
        res = b3.first(ctx)

        print(f"goto first succeeded {res}.")
        t.commit(ctx)

    def test_goto_last(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "barstoreec", t)
        res = b3.last(ctx)

        print(f"goto last succeeded {res}.")
        t.commit(ctx)

    def test_is_unique(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "barstoreec", t)
        res = b3.is_unique()

        print(f"is_unique succeeded {res}.")
        t.commit(ctx)

    def test_count(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "barstoreec", t)
        res = b3.count()

        print(f"count succeeded {res}.")
        t.commit(ctx)

    def test_get_store_info(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "barstoreec", t)
        res = b3.get_store_info()

        print(f"storeInfo: {res}")
        t.commit(ctx)


class TestBtreeMapKey(unittest.TestCase):
    def setUpClass():
        ro = RedisOptions()
        Redis.open_connection(ro)

        t = transaction.Transaction(ctx, to)
        t.begin()

        cache = btree.CacheConfig()
        bo = btree.BtreeOptions("foobar", True, cache_config=cache)
        bo.set_value_data_size(btree.ValueDataSize.Small)
        bo.is_primitive_key = False

        b3 = btree.Btree.new(ctx, bo, t)
        l = [
            btree.Item(pKey(key="123"), "foo"),
        ]
        print(f"foobar b3 add result: {b3.add(ctx, l)}")

        bo = btree.BtreeOptions("person", True, cache_config=cache)
        bo.set_value_data_size(btree.ValueDataSize.Small)
        bo.is_primitive_key = False

        btree.Btree.new(ctx, bo, t)

        t.commit(ctx)

        print("new B3 succeeded")

    def test_add_if_not_exists(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "foobar", t)
        l = [
            btree.Item(pKey(key="123"), "foo"),
        ]
        if b3.add_if_not_exists(ctx, l):
            print("addIfNotExists should have failed.")

        t.commit(ctx)
        print("test add_if_not_exists")

    def test_get_items(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "foobar", t)
        res = b3.get_items(
            ctx, btree.PagingInfo(0, 5, direction=btree.PagingDirection.Forward.value)
        )
        print(f"get_items succeeded {res}.")

        t.commit(ctx)

    def test_get_keys(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "foobar", t)
        res = b3.get_keys(
            ctx, btree.PagingInfo(0, 5, direction=btree.PagingDirection.Forward.value)
        )
        print(f"get_keys succeeded {res}.")

        t.commit(ctx)

    def test_get_values(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "foobar", t)
        keys = b3.get_keys(
            ctx, btree.PagingInfo(0, 5, direction=btree.PagingDirection.Forward.value)
        )
        res = b3.get_values(ctx, keys)

        print(f"get_values succeeded {res}.")

        t.commit(ctx)

    def test_find(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "foobar", t)
        res = b3.find(ctx, pKey(key="123"))

        print(f"find succeeded {res}.")

        t.commit(ctx)

    def test_find_with_id(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "foobar", t)
        keys = b3.get_keys(
            ctx, btree.PagingInfo(0, 5, direction=btree.PagingDirection.Forward.value)
        )
        res = b3.find_with_id(ctx, keys[0].key, keys[0].id)

        print(f"find with id succeeded {res}.")

        t.commit(ctx)

    def test_goto_first(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "foobar", t)
        res = b3.first(ctx)

        print(f"goto first succeeded {res}.")
        t.commit(ctx)

    def test_goto_last(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "foobar", t)
        res = b3.last(ctx)

        print(f"goto last succeeded {res}.")
        t.commit(ctx)

    def test_is_unique(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "foobar", t)
        res = b3.is_unique()

        print(f"is_unique succeeded {res}.")
        t.commit(ctx)

    def test_count(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "foobar", t)
        res = b3.count()

        print(f"count succeeded {res}.")
        t.commit(ctx)

    def test_get_store_info(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "foobar", t)
        res = b3.get_store_info()

        print(f"storeInfo: {res}")
        t.commit(ctx)

    def test_add_people(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "person", t)

        # Prepare a batch of 500 Person records.
        l = []
        for i in range(500):
            l.append(btree.Item(pKey(key=f"{i}"), Person(f"joe{i}", "petit")))

        # Add the batch to the B-tree.
        if not b3.add_if_not_exists(ctx, l):
            print("failed to add list of persons to backend db")

        t.commit(ctx)

    def test_get_keys_get_values(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "person", t)
        b3.first(ctx)
        keys = b3.get_keys(
            ctx,
            btree.PagingInfo(10, 20, 2, direction=btree.PagingDirection.Forward.value),
        )
        values = b3.get_values(ctx, keys)
        print(f"values: {values}")

        t.commit(ctx)

    def test_get_keys_backwards_get_values(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "person", t)
        # Position cursor to the last item.
        b3.last(ctx)
        # Navigate to the 200th item backwards then fetch that item & the item previous to it.
        keys = b3.get_keys(
            ctx,
            btree.PagingInfo(10, 20, 2, direction=btree.PagingDirection.Backward.value),
        )
        # Use the returned keys to ask B-tree to fetch the values of these keys.
        values = b3.get_values(ctx, keys)
        print(f"values: {values}")

        t.commit(ctx)

    def test_get_keys_over_the_edge_get_values(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "person", t)
        b3.first(ctx)
        keys = b3.get_keys(
            ctx,
            # There are 500 records in the DB, navigate to the 490th then fetch last 10 records.
            # Since there are only 10 records after reaching 490 item location, fetching 20 will just return remaining 10 records.
            btree.PagingInfo(49, 10, 20, direction=btree.PagingDirection.Forward.value),
        )
        # Use the returned keys to ask B-tree to fetch the value parts of these keys.
        values = b3.get_values(ctx, keys)
        print(f"values: {values}")

        t.commit(ctx)
