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
* Create new B-tree(s), or Open existing B-tree(s)
* Manage data, do some CRUD operations
* Commit the transaction

Below is an example code block for illustrating the above steps. For other SOP B-tree examples, you can checkout the code in the unit tests test_btree.py & test_btree_idx.py files that comes w/ the SOP package you downloaded from pypi.

```
from sop import transaction
from sop import btree
from sop import context
from sop import redis

# Store info & Registry home base folders. Array of strings of two elements, one for Active & another, for passive folder.
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

# Index Specification
You can specify a Key structure that can be complex like a class and cherry pick the fields you want to affect the indexing and data organization in the B-tree. Like pick two fields from the Key class as comprised your index. And optionally specify the sort order, like 1st field is descending order and the 2nd field ascending.

Here is a good example to illustrate this use-case.
```
import unittest
from . import transaction
from . import btree
from . import context

from .redis import *
from .test_btree import to

from dataclasses import dataclass

# Define your Key data class with two fields.
@dataclass
class Key:
    address1: str
    address2: str

# Define your Value data class.
@dataclass
class Person:
    first_name: str
    last_name: str

# Create a context for use in Transaction & B-tree API calls.
ctx = context.Context()

class TestBtreeIndexSpecs(unittest.TestCase):
    def setUpClass():
        ro = RedisOptions()
        Redis.open_connection(ro)

        t = transaction.Transaction(ctx, to)
        t.begin()

        cache = btree.CacheConfig()
        bo = btree.BtreeOptions("personidx", True, cache_config=cache)

        # Specify Small size, meaning, both Key & Value object values will be stored in the B-tree node segment.
        # This is very efficient/appropriate for small data structure sizes (of Key & Value pairs).
        bo.set_value_data_size(btree.ValueDataSize.Small)

        # Specify that the B-tree will host non-primitive Key data type, i.e. - it is a dataclass.
        bo.is_primitive_key = False

        btree.Btree.new(
            ctx,
            bo,
            t,
            # Specify the Index fields of the Key class. You control how many fields get included
            # and each field's sort order (asc or desc).
            btree.IndexSpecification(
                index_fields=(
                    # 1st field is "address1" in descending order.
                    btree.IndexFieldSpecification(
                        "address1", ascending_sort_order=False
                    ),
                    # 2nd field is "address2" in ascending order (default).
                    btree.IndexFieldSpecification("address2"),
                )
            ),
        )

        # Commit the transaction to finalize it.
        t.commit(ctx)

    def test_add(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "personidx", t)

        pk = Key(address1="123 main st", address2="Fremont, CA")
        l = [btree.Item(pk, Person(first_name="joe", last_name="petit"))]

        # Populate with some sample Key & Value pairs of data set.
        for i in range(20):
            pk = Key(address1=f"{i}123 main st", address2="Fremont, CA")
            l.append(btree.Item(pk, Person(first_name=f"joe{i}", last_name="petit")))

        # Submit the entire generated batch to be added to B-tree in one call.
        b3.add_if_not_exists(ctx, l)

        # Commit the changes.
        t.commit(ctx)

    def test_get_items_batch(self):
        t = transaction.Transaction(ctx, to)
        t.begin()

        b3 = btree.Btree.open(ctx, "personidx", t)

        # Fetch the data starting with 1st record up to 10th.
        # Paging info is:
        # 0 page offset means the current record where the cursor is located, 0 skipped items, 10 items to fetch.
        # In forward direction.
        items = b3.get_items(
            ctx,
            btree.PagingInfo(0, 0, 10, direction=btree.PagingDirection.Forward.value),
        )

        print(f"read items from indexed keyed b-tree {items}")

        # End the transaction by calling commit. Commit in this case will also double check that the fetched items
        # did not change while in the transaction. If there is then it will return an error to denote this and
        # thus, your code can treat it as failure if it needs to, like in a financial transaction.
        t.commit(ctx)

```

** Above is the same code of "sop/test_btree_idx.py" unit test file.

# Navigation methods such as Find, First, Last then Fetch
B-tree fetch operations are all cursor oriented. That is, you position the cursor to the item you want to fetch then fetch a batch. Each fetch (or getXx) call will move the cursor forward or backward to allow you to easily navigate and retrieve items (records) from a B-tree. There are the navigation set of methods to help in traversing the B-tree.

The navigate then fetch batch (pattern) using "PagingInfo" as shown in above section is quite handy, specially when you are working on a UI that allows enduser(s) to browse through a series of data pages and needs to work out the B-tree, slice and dice the items (across thru data pages) and allows enduser operations/data entry-management.

There are few navigation methods such as:
* First - this will position the cursor to the first item of the B-tree, as per the "key sort order".
* Last - this will position the cursor to the last item of the B-tree.
* Find/FindWithID - allows you to find an item with a given Key, or at least, an item nearby. When there is no exact match found for the Key, B-tree will position the cursor to the item with a similar Key (one compare unit greater than the Key sought for). This is very handy, for example, you can issue a Find, or FindWithID if there are duplicates by key and you have ID of the one you are interested in. Then use the fetch methods (see get_keys, get_items, get_values) with paging info describing relative offset (from current or few pages forward or backward, & how many) to fetch the batch of items.

# Other Management Methods
And also, there are other methods of the B-tree you can use to manage the items such as: Update, Remove, Upsert. All of these accepts an array or a batch of items (key &/or value pairs as appropriate). See btree.py of the sop4py's "sop" package for complete list.

# SOP in Github
SOP open source project (MIT license) is in github. You can checkout the "...sop/jsondb/" package which contains the Go code enabling general purpose JSON data management & the Python wrapper, coding guideline of which, was described above.

Please feel free to join the SOP project if you have the bandwidth and participate/co-own/lead! the project engineering.
SOP project links: https://github.com/sharedcode/sop & https://pypi.org/project/sop4py