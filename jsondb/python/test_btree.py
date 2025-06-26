import unittest

from btree import *
from transaction import *

stores_folders = ["/Users/grecinto/sop_data/disk1", "/Users/grecinto/sop_data/disk2"]
ec = {
    "barstoreec",
    ErasureCodingConfig(
        2,
        1,
        [
            "/Users/grecinto/sop_data/disk1",
            "/Users/grecinto/sop_data/disk2",
            "/Users/grecinto/sop_data/disk3",
        ],
        True,
    ),
}


class TestBtree(unittest.TestCase):
    def test_new_btree(self):
        # ro = RedisOptions()
        # open_redis_connection()
        to = TransationOptions(
            TransactionMode.ForWriting,
            timedelta(minutes=5),
            MIN_HASH_MOD_VALUE,
            stores_folders,
            ec,
        )
        t = Transaction(to)
        t.begin()

        cache = CacheConfig(timedelta(minutes=5), False)
        bo = BtreeOptions("barstoreec", True, 8, "", ValueDataSize.Small, cache)
        b3 = Btree.new(bo, t)
        l = [Item(1, "foo")]
        b3.add(l)

        t.commit()
        print("test")
