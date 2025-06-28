import json
import unittest
import transaction
import btree

from datetime import timedelta
from redis import *

from dataclasses import dataclass, asdict

stores_folders = ("/Users/grecinto/sop_data/disk1", "/Users/grecinto/sop_data/disk2")
ec = {
    "barstoreec": transaction.ErasureCodingConfig(
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

        b3 = btree.Btree.new(True, bo, t)
        # l = (btree.Item({"k":1,"k2":"l"}, {"v":1,"v2":"l"}))
        l = [btree.Item(1, "foo")]
        b3.add(l)

        t.commit()
        print("test new")

    # def test_open_btree(self):
    #     to = transaction.TransationOptions(
    #         transaction.TransactionMode.ForWriting.value,
    #         5,
    #         transaction.MIN_HASH_MOD_VALUE,
    #         stores_folders,
    #         ec,
    #     )

    #     t = transaction.Transaction(to)
    #     t.begin()

    #     b3 = btree.Btree.open("barstoreec", t)
    #     l = [btree.Item(1, "foo")]
    #     b3.add(l)

    #     t.commit()
    #     print("test open")
