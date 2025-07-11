# What is SOP?

Scalable Objects Persistence (SOP) is a raw storage engine that bakes together a set of storage related features & algorithms in order to provide the most efficient & reliable (ACID attributes of transactions) technique (known) of storage management and rich search, as it brings to the application, the raw muscle of "raw storage", direct IO communications w/ disk drives. In a code library form factor.

# SOP supported Hardware/OS
SOP supports popular architectures & Operating Systems such as Linux, Darwin & Microsoft Windows, in both ARM64 & AMD64 architectures. For Windows, only AMD64 is supported since it is the only architecture Windows is available in.

# SOP Dependencies
* Redis, you will need to have one of the recent or latest version of Redis for use in SOP caching.
* More than one Disk Drives(recommended is around four or more, for replication) with plenty of drive space available, for storage management. Example:
```
    /disk1
    /disk2
    /disk3
    /disk4
```

# SOP for Python package
Following steps outlines how to use the Scalable Objects Persistence code library for Python:
* Install the package using: pip install sop-python-beta-3
* Follow standard Python package import and start coding to use the SOP for Python code library for data management. Import the sop package in your python code file.
* Specify Home base folders where Store info & Registry data files will be stored.
* Specify Erasure Coding (EC) configuration details which will be used by SOP's EC based replication.
* Create a transaction
* Begin a transaction
* Create a new B-tree, or Open an existing B-tree
* Manage data, do some CRUD operations
* Commit the transaction

Below is an example code black for illustrating the above steps. For other SOP B-tree examples, you can checkout the code in the unit tests test_btree.py & test_btree_idx.py files that comes w/ the SOP package you downloaded from pypi.

```
import sop.transaction
import sop.btree
import sop.context

stores_folders = ("/disk1", "/disk2")
ec = {
    # Erasure Config default entry(key="") will allow different B-tree(tables) to share same EC structure.
    "": transaction.ErasureCodingConfig(
        2,  # two data shards
        2,  # two parity shards
        (
            # 4 disk drives paths
            "/disk1",
            "/disk2",
            "/disk3",
            "/disk4",
        ),
        # False means Auto repair of failed reads from (shards') disk drive will not get repaired.
        False,
    )
}

# Transaction Options (to).
to = transaction.TransationOptions(
    transaction.TransactionMode.ForWriting.value,
    # commit timeout of 5mins
    5,
    # Min Registry hash mod value is 250, you can specify higher value like 1000. A 250 hashmod
    # will use 1MB sized file segments. Good for demo, but for Prod, perhaps a bigger value is better.
    transaction.MIN_HASH_MOD_VALUE,
    # Store info & Registry home base folders. Array of strings of two elements, one for Active & another, for passive folder.
    stores_folders,
    # Erasure Coding config as shown above.
    ec,
)

# Context object.
ctx = context.Context()

# initialize/open SOP global Redis connection
ro = RedisOptions()
Redis.open_connection(ro)

t = transaction.Transaction(ctx, to)
t.begin()

cache = btree.CacheConfig()

# "barstoreec" is new b-tree name, 2nd parameter set to True specifies B-tree Key field to be native data type
bo = btree.BtreeOptions("barstoreec", True, cache_config=cache)
bo.set_value_data_size(btree.ValueDataSize.Small)

# create the new "barstoreec" b-tree store.
b3 = btree.Btree.new(ctx, bo, t)

# Since we've specified Native data type = True in BtreeOptions, we can use "integer" values as Key.
l = [
    btree.Item(1, "foo"),
]

# Add Item to the B-tree,
b3.add(ctx, l)

# Commit the transaction to finalize the new B-tree (store) change.
t.commit(ctx)
```

# SOP in Github
SOP open source project (MIT license) is in github. You can checkout the "...sop/jsondb/" package which contains the Go code enabling general purpose JSON data management & the Python wrapper, coding guideline of which, was described above.

Please feel free to join the SOP project if you have the bandwidth and participate/co-own/lead! the project engineering.