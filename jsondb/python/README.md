# What is SOP?

Scalable Objects Persistence (SOP) is a raw storage engine that bakes together a set of storage related features & algorithms in order to provide the most efficient & reliable (ACID attributes of transactions) technique (known) of storage management and rich search, as it brings to the application, the raw muscle of "raw storage", direct IO operations w/ disk drives. In a code library form factor.

Other "key advances" in database technology available in this SOP release:
* Serverless (or all servers!) operations mode in the cluster, i.e. - your applications that use SOP library are server-less
* Uses a new storage & L1/L2 caching strategy that is ground breaking in performance & efficiency
* New superfast realtime Orchestration engine fit for database transactions
* Support for small, medium to large/very large (multi-GBs/TB) data management
* Sports advanced/efficient/high tolerance software based (via Erasure Coding) replication
* Horizontal & Vertical scaling in storage and cluster processes
* New database storage strategy that uses segment files which avoids having to manage a huge file, resulting in higher IO efficiency & file system/disk drive friendly (medium sized) data files
* Built-in data caching, your application data can be cached automatically via config setting. No need to write special code to cache certain data set in Redis, for example. SOP can provide that if configured
* Host your applications across platforms, e.g. - microservice cluster running in Linux, another cluster or instance running in Windows, and all SOPs inter-operating with one another seamlessly and data stored in same set of disk drives. Freedom to use popular hardware architecture & OS of your choice(s)!
* Unlimited B-trees, limited only by your hardware resources. Each B-tree store, as the name implies, is a B-tree serving super-fast, sorted by Key (Key & Value pair) data sets

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
* Install the package using: pip install sop4py
* Follow standard Python package import and start coding to use the SOP for Python code library for data management. Import the sop package in your python code file.
* Specify Home base folders where Store info & Registry data files will be stored.
* Specify Erasure Coding (EC) configuration details which will be used by SOP's EC based replication.
* Open the global Redis connection
* Create a transaction
* Begin a transaction
* Create a new B-tree, or Open an existing B-tree
* Manage data, do some CRUD operations
* Commit the transaction

Below is an example code block for illustrating the above steps. For other SOP B-tree examples, you can checkout the code in the unit tests test_btree.py & test_btree_idx.py files that comes w/ the SOP package you downloaded from pypi.

```
from sop import transaction
from sop import btree
from sop import context
from sop import redis

stores_folders = ("/disk1", "/disk2")
ec = {
    # Erasure Config default entry(key="") will allow different B-tree (data store) to share same EC structure.
    # You can also specify a different one exclusive to a B-tree with the given name.
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
    # commit timeout of 15mins
    15,
    # Min Registry hash mod value is 250, you can specify higher value like 1000. A 250 hashmod
    # will use 1MB sized file segments. Good for demo, but for Prod, perhaps a bigger value is better.
    transaction.MIN_HASH_MOD_VALUE,
    # Store info & Registry home base folders. Array of strings of two elements, one for Active & another, for passive folder.
    stores_folders,
    # Erasure Coding config as shown above.
    ec,
)

# Context object. Your code can call the "cancel" method of the context if you want to abort the operation & rollback the transaction.
# Useful for example, if you have a concurrently running python code and wanted to abort the running SOP transaction.
ctx = context.Context()

# initialize/open SOP global Redis connection. You can specify your Redis cluster host address(es) & port, etc
# but defaults to localhost and default Redis port # if none is specified.
ro = redis.RedisOptions()
redis.Redis.open_connection(ro)

t = transaction.Transaction(ctx, to)
t.begin()

cache = btree.CacheConfig()

# "barstoreec" is new b-tree name, 2nd parameter set to True specifies B-tree Key field to be native data type
bo = btree.BtreeOptions("barstoreec", True, cache_config=cache)
bo.set_value_data_size(btree.ValueDataSize.Small)

# create the new "barstoreec" b-tree store. Once created, you can just use the "open" method. Perhaps
# you can have a code set for creating B-trees like "admin only" script. Then all other app code uses
# the "open" method.
b3 = btree.Btree.new(ctx, bo, t)

# Since we've specified Native data type = True in BtreeOptions, we can use "integer" values as Key.
l = [
    btree.Item(1, "foo"),
]

# Add Item to the B-tree.
b3.add(ctx, l)

# Commit the transaction to finalize the new B-tree (store) change.
t.commit(ctx)
print("ended.")
```

# SOP in Github
SOP open source project (MIT license) is in github. You can checkout the "...sop/jsondb/" package which contains the Go code enabling general purpose JSON data management & the Python wrapper, coding guideline of which, was described above.

Please feel free to join the SOP project if you have the bandwidth and participate/co-own/lead! the project engineering.
SOP project link: https://github.com/sharedcode/sop