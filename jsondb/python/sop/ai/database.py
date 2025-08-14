from sop import transaction
from sop import btree
from sop import context
from sop import redis

from dataclasses import dataclass


class Database:
    
    @classmethod
    def set_environments(environments dict[str,transaction.TransactionOptions]):
  
        

# # Stores home folder(s). Replication requires two paths, one for active and the 2nd, the passive.
# stores_folders = ("/Users/grecinto/sop_data/disk1", "/Users/grecinto/sop_data/disk2")
# # EC configuration specifies the Erasure Coding parameters like:
# # "data shards count" (2), "parity shards count" (1), folder paths (disk1, disk2, disk3) where the shards &
# # parities data file will be stored. And a flag (True) whether to auto-repair any shard that failed to read.
# ec = {
#     # Erasure Config default entry(key="") will allow different B-tree(tables) to share same EC structure.
#     "": transaction.ErasureCodingConfig(
#         2,
#         1,
#         (
#             "/Users/grecinto/sop_data/disk1",
#             "/Users/grecinto/sop_data/disk2",
#             "/Users/grecinto/sop_data/disk3",
#         ),
#         True,
#     )
# }

# # Transaction Options (to).
# to = transaction.TransationOptions(
#     transaction.TransactionMode.ForWriting.value,
#     5,
#     transaction.MIN_HASH_MOD_VALUE,
#     stores_folders,
#     ec,
# )

# # Context object.
# ctx = context.Context()


#         # initialize SOP global Redis connection
#         ro = RedisOptions()
#         Redis.open_connection(ro)

#         # create the "barstoreec" b-tree store.
#         t = transaction.Transaction(ctx, to)
#         t.begin()

#         cache = btree.CacheConfig()
#         bo = btree.BtreeOptions("barstoreec", True, cache_config=cache)
#         bo.set_value_data_size(btree.ValueDataSize.Small)

#         b3 = btree.Btree.new(ctx, bo, t)
#         l = [
#             btree.Item(1, "foo"),
#         ]
#         b3.add(ctx, l)

#         # commit the transaction to finalize the new store changes.
#         t.commit(ctx)
#         print("new B3 succeeded")
