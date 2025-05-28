Scaleable Objects Persistence (SOP) Framework - Golang V2

Code Coverage: https://app.codecov.io/github/SharedCode/sop

### High level features/usability articles about SOP:
SOP's Swarm Computing Proposition: https://www.linkedin.com/pulse/geminis-analysis-sops-swarm-computing-gerardo-recinto-cqzqc

Revolutionary Storage & Cache Strategy: https://www.linkedin.com/pulse/revolutionizing-b-tree-performance-universal-l1-cache-gerardo-recinto-87jjc

Google Slides Presentation: https://docs.google.com/presentation/d/17BWiLXcz1fPGVtCkAwvE9wR0cDq_dJPjxKgzMcWKkp4/edit#slide=id.p

SOP as AI database: https://www.linkedin.com/pulse/sop-ai-database-engine-gerardo-recinto-tzlbc/?trackingId=yRXnbOEGSvS2knwVOAyxCA%3D%3D

Anatomy of a Video Blob: https://www.linkedin.com/pulse/sop-anatomy-video-blob-gerardo-recinto-4170c/?trackingId=mXG7oM1IRVyP4yIZtWWlmg%3D%3D

B-Tree, a Native of the Cluster: https://www.linkedin.com/pulse/b-tree-native-cluster-gerardo-recinto-chmjc/?trackingId=oZmC6tUHSiCBcYXUqwfGUQ%3D%3D

SOP in File System: https://www.linkedin.com/pulse/scaleable-object-persistencesop-file-system-gerardo-recinto-zplbc/?trackingId=jPp8ccwvQEydxt3pppa8eg%3D%3D

Hash Map on Disk: https://www.linkedin.com/posts/coolguru_hash-map-on-a-file-can-offer-up-to-13-activity-7313645523024891905-8yem?utm_source=share&utm_medium=member_desktop&rcm=ACoAAABC-LQBTk6hP9wAIOqQDfLJ3w2_hZ-nyh0

Master less cluster wide distributed locking (RSRR algorithm) :https://www.linkedin.com/posts/coolguru_new-master-less-cluster-wide-resource-locking-activity-7322020975674302465-lUjl?utm_source=social_share_send&utm_medium=member_desktop_web&rcm=ACoAAABC-LQBTk6hP9wAIOqQDfLJ3w2_hZ-nyh0

RSRR as compared to DynamoDB's distributed locking: https://www.linkedin.com/posts/coolguru_i-just-found-out-thanks-to-my-eldest-that-activity-7325255314474250241-f07g?utm_source=social_share_send&utm_medium=member_desktop_web&rcm=ACoAAABC-LQBTk6hP9wAIOqQDfLJ3w2_hZ-nyh0

# Documentation
A lot has changed in SOP. "sop/in_red_fs" is shaping to be the primary and recommended package. It is a very lean package only needing Redis as dependency. There are still value in below discussions but be very careful, self discerning as they are getting obsolete by the day.

SOP in Redis & File System (in_red_fs) will need a new, simple documentation. Bear with below for now. Please feel free to use the "in_red_fs/integration_tests" automated test code as examples, guide in SOP API usage. Also, you can use below guide and if wanting to use "in_red_fs", replace the package name "in_red_cfs" with "in_red_fs" and most instructions or API call should still work the same way, in theory.

Once the remaining bits in "in_red_fs" like replication for the "registry" gets completed, this documentation will be updated to sync with the "in_red_fs" recommended package, together with additional contents like replication feature.

"in_red_cfs" is complete and (almost!) ready for production but it has Cassandra(C*) dependency. Replication is provided by C* and the ReedSolomon Erasure Coding (EC) based replication on the file based B-tree nodes & large data (if your setup has any) blobs. However, it is not as lean as the "in_red_fs" package because of Cassandra usage in the registry (i.e. - the virtual ID repository) which still has replication on registry being completed. Also, the new "in_red_fs" hash map on disk which is another breakthrough algorithm implementation for what it does, has turned out to outperform C* on managing the registry table. No comparison because of use-fit of hash map on disk w/ direct IO on this module usability, so why a big push on "in_red_fs" completion.

However, both "in_red_fs" & "in_red_cfs" turned out to be a breakthrough tech's first reference implementations. They are showing real cutting edge performance based on the SOP's new "swarm computing" and new "storage & caching strategy" that the design had been "validated" & labelled as "revolutionary and a blue print of high performance, scalable storage system in a complex distributed environment", by an AI engine (name withheld to protect AI's privacy).

# Software Based Efficient Replication
Quick update, SOP now sports very efficient software based replication via Reed Solomon algorithm erasure coding. SOP's Registry data already had replication via Cassandra & now, the data blobs stored in File System (see sop/in_red_cfs) are safe having very efficient software based modern replication.
The feature is complete, it has auto-repair of detected missing or bitrot shards, if the RepairCorruptedShards flag (passed in the sop/in_red_cfs/NewTransactionWithEC erasure config) is turned on.
Use-case, you can set this flag off then upon disk drive failure detection, you can replace the drive turn on this flag then restart the app. SOP will then automatically repair the missing shard files (on the newly reinstated drive) that can get reconstructed from the available shards.

If left untouched, SOP can operate(read-only) even with drive(s) failures so long as data can be reconstructed from the available shards. The sample I made(see in_red_cfs/integration_tests/basic_ec_test.go) uses 2 data shards and 1 parity shard. Yes, you can use minimal replication and it will work to your desire, if enough to support drive(s) failure.

Sample code to use this Erasure Coding based replication feature:
```
import (
	"github.com/SharedCode/sop/in_red_cfs"
	"github.com/SharedCode/sop/in_red_cfs/fs"
)

// ...

	// Make sure not to change the ErasureCodingConfig, once set this should be permanent.
	// Otherwise you have to rebuild entire data set to use new configuration. The EC config
	// is specific to a given transaction and any btree you create within this transaction(s)
	// will share the same disk drives and base folder paths specified for the transaction.
	//
	// And yes, you are free to use different EC config including different storage data paths
	// across different transactions. It is up to you/your code to maintain the separate data
	// boundaries.
	trans, _ := in_red_cfs.NewTransactionWithEC(sop.ForWriting, -1, true, &fs.ErasureCodingConfig{
		DataShardsCount:   2,
		ParityShardsCount: 1,
		BaseFolderPathsAcrossDrives: []string{
			"c://sop_data",
			"d://sop_data",
			"e://sop_data",
		},
		RepairCorruptedShards: false,
	})
	trans.Begin()
	b3, _ := in_red_cfs.NewBtreeWithEC[int, string](ctx, sop.StoreOptions{
		Name:                     "foobar",
		SlotLength:               200,
		// nothing special, use your judgement whether to save on Btree node or a separate segment.
		IsValueDataInNodeSegment: true,
	}, trans)

	// Nothing special, use B-Tree interface methods to manage your data then call
	// trans.Commit to finalize changes, like the usual SOP usage.
	b3.Add(ctx, 1, "hello world")

```

# Quick Store Caching Config Guide
At the latest release, V2 Beta 2.1.7+, SOP is able to achieve "persisted data & caching" virtualization. This is a very important milestone, because SOP further cemented its capabilities, estimate is, nothing in the market can compete with SOP for the things it provides & in the magnitude of its "horizontal & vertical scaling" & "data caching virtualization" capabilities. In fact, it is the only solution known that does this.

Sample code for customization of store level caching:
* Store data cache does not expire
  
  NOTE: Setting duration to 0 achieves caching with no expiration
  ```
  	b3, _ := in_red_cfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "storecaching",
		SlotLength:               200,
		IsValueDataInNodeSegment: true,
		BlobStoreBaseFolderPath:  dataPath,
		CacheConfig:              sop.NewStoreCacheConfig(time.Duration(0), false),
	}, trans)
  ```
* Store data cache is "sliding window"
  
  NOTE: Setting 2nd param(isCacheTTL) true of sop.NewStoreCacheConfig(..) sets the store so each operation including fetch(get) will instruct Redis to extend the caching for the target data, a.k.a. "sliding time" or TTL
  ```
  	b3, _ := in_red_cfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "storecachingTTL",
		SlotLength:               200,
		IsValueDataInNodeSegment: true,
		BlobStoreBaseFolderPath:  dataPath,
		CacheConfig:              sop.NewStoreCacheConfig(time.Duration(5*time.Hour), true),
	}, trans)
  ```
* Store data cache has absolute expiration(default)
  
  NOTE: This is the default mode and is also achieved in the sop.NewStoreCacheConfig(..) call by passing false to the 2nd param(isCacheTTL) & a > 0 duration.
  ```
  	b3, _ := in_red_cfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "storecaching",
		SlotLength:               200,
		IsValueDataInNodeSegment: true,
		BlobStoreBaseFolderPath:  dataPath,
		CacheConfig:              sop.NewStoreCacheConfig(time.Duration(5*time.Hour), false),
	}, trans)
  ```
* B-Tree Node & Application data is "sliding window"
  
  NOTE: You can set app data to get stored in B-Tree Node & make the Node caching as "sliding window", thus, your app data also gets such caching behavior. Here is how:
  ```
  b3, _ := in_red_cfs.NewBtree[int, string](ctx, sop.StoreOptions{
	Name:                     "storecaching",
	SlotLength:               200,
	IsValueDataInNodeSegment: true,
	BlobStoreBaseFolderPath:  dataPath,
	CacheConfig: &sop.StoreCacheConfig{
		RegistryCacheDuration:  time.Duration(5 * time.Hour),
		StoreInfoCacheDuration: time.Duration(5 * time.Hour),
		NodeCacheDuration:      time.Duration(5 * time.Hour),
		IsNodeCacheTTL   :      true,
	},
  }, trans)
  ```
* Application data cache is "sliding window"
  
  NOTE: When you would like to conserve Redis cache but still provide great level of caching of your application data, you can set the application data to do "sliding window"(TTL) and set store meta data to absolute expiration. Here is how to do it:
  ```
  b3, _ := in_red_cfs.NewBtree[int, string](ctx, sop.StoreOptions{
	Name:                     "storecaching",
	SlotLength:               200,
	IsValueDataInNodeSegment: false,
	BlobStoreBaseFolderPath:  dataPath,
	CacheConfig: &sop.StoreCacheConfig{
		RegistryCacheDuration:  time.Duration(2 * time.Hour),
		NodeCacheDuration:      time.Duration(2 * time.Hour),
		StoreInfoCacheDuration: time.Duration(2 * time.Hour),
		ValueDataCacheDuration: time.Duration(7 * time.Hour),
		IsValueDataCacheTTL   : true,
	},
  }, trans)
  ```
After 2 hours, Registry, Node & StoreInfo meta data for this "storecaching" SOP store will expire and thus, reduce the data cached in Redis. BUT since the application data(ValueData) is set to do "sliding window" or TTL, then those that got accessed(including fetch!) within the set duration will be retained, so future access to them will take it from Redis cache instead of reading from backend storage, and each access extending cache retention, a.k.a. - "sliding window". Keeping the frequently accessed data in the cache.
You do need to set the StoreOption field **IsValueDataInNodeSegment** = false in order to make this to work. So the application data is stored in their data segments and not in the B-Tree Nodes themselves.

Of course, you have to do fine tuning as there are tradeoffs :), determine what works best in your particular situation. As there are quite a few "knobs" you can tweak in SOP to achieve what you want. See below discussions for more details in this area.

# In File System(FS) or in Cassandra Blobs
SOP supports storing the data blobs(containing both SOP metadata, specifically the B-Tree Nodes & application data) either in FS or in Cassandra (via Cassandra's support for blobs storage). Choose whichever you want. However, my favorite is the FS, as it is very lean.

Following are the package locations for the different flavors:
* in File System w/ registry in Cassandra: 	sop/in_red_cfs
* in File System (early preview!): 		sop/in_red_fs
NOTE: **in_red_fs** (under construction) will not require any external system as SOP replaces Cassandra based registry with a more optimal hash map on disk implementation. Redis for caching is still available.

The API for each package to construct a new BTree, Open an existing one or Remove one are pretty much consistent across the three. Streaming Data Store API are also available across the three and consistent in shape too.

# Data Partitioning
* SOP in Cassandra stores a blob in its own dedicated partition, which is the optimal form.
* SOP in File System has the following to address data partitioning:
  - Vertical partitioning is built-in, you can take advantage of this by specifying different drive in the directory path field of StoreOptions argument of the BTree constructor function. E.g. - for store1, specify "c://sop_data" and on store2, specify "d://sop_data". This will allow SOP to store the blob files for each store on its own drive. Thus, combined with SOP's efficient I/O(i.e. - 4 level "tree like" directory structuring) achieves super efficient parallel I/O, per store.
  - Horizontal partitioning however, you need to do a little bit of customization, i.e. - specify a lambda expression that has the logic to carve into different drive(s) or storage path(s) per given blob file ID. You can pass in this function in the respective "in_red_xxx.NewTransactionExt(..)" function call and it will be utilized to drive where the file blob(s) will be stored. Sample functions that can allocate(or partition!) across different drives the file blobs are below.
    
Here is the SOP's **DefaultToFilePath** function(in package sop/in_red_fs) that drives storage of file blobs across passed in **base directory path** argument of StoreOptions in NewBtree call:
```
package fs

import (
	"fmt"
	"os"

	"github.com/SharedCode/sop"
)

type ToFilePathFunc func(basePath string, id sop.UUID) string

// ToFilePath is used for formatting a base folder path & a GUID into a file path & file name
// optimized for efficient I/O. It can be a simple logic as the default function shown below
// or you can implement as fancy as partitioning across many storage devices, e.g. - using
// the 1st hex digit, apply modulo to distribute to your different storage devices.
//
// Or using the basePath to specify different storage path, this perhaps is the typical case(default).
var ToFilePath ToFilePathFunc = DefaultToFilePath

// Default file path formatter, given a base path & a GUID.
func DefaultToFilePath(basePath string, id sop.UUID) string {
	if len(basePath) > 0 && basePath[len(basePath)-1] == os.PathSeparator {
		return fmt.Sprintf("%s%s", basePath, Apply4LevelHierarchy(id))
	}
	return fmt.Sprintf("%s%c%s", basePath, os.PathSeparator, Apply4LevelHierarchy(id))
}

// Support 4 level folders file distribution algorithm, a.k.a. tree like folder hierarchy.
func Apply4LevelHierarchy(id sop.UUID) string {
	s := id.String()
	ps := os.PathSeparator
	return fmt.Sprintf("%x%c%x%c%x%c%x", s[0], ps, s[1], ps, s[2], ps, s[3])
}
```

And here is a sample to show how you can specify horizontally partitioned blob files:
```
type struct fileFormatter {
	driveLookup map[int]string
}
// My File path formatter, given a base path & a GUID, use the GUID to specify 
func (ff *fileFormatter)MyToFilePath(basePath string, id sop.UUID) string {
	// assuming basePath contains a %s format char for the drive, e.g.: "%s://sop_data"
	// then you can assemble a new drive/dir path that does horizontal partitioning based off of the 1st component of UUID.

	drvIndex := id[0] % 4
	return fmt.Sprintf(basePath, ff.driveLookup[drvIndex])
}
```

And in your code where you construct the Transaction and Btree objects, here is how it looks like plugging in the new custom file formatter:
```
ff := fileFormatter{
	driveLookup: map[int]string{
		{0, "c"},
		{1, "d"},
		{2, "e"},
		{3, "f"},
	}
}
trans, _ := in_red_cfs.NewTransactionExt(ff.MyToFilePath, sop.ForWriting, -1, false)

```

# Usability
SOP can be used in a wide, diverse storage usability scenarios. Ranging from general purpose data storage - search & management, to highly scaleable and performant version of the same, to domain specific use-cases. As SOP has many feature knobs you can turn on or off, it can be used and get customized with very little to no coding required. Some examples bundled out of the box are:
  * A. General purpose data/object storage management system
  * B. Large data storage and management, where your data is stored in its own data segment. See StoreInfo.IsValueDataInNodeSegment = false flag
  * C. Streaming Data application domain enabling very large data storage - search and management, supporting multi-GBs record or item, limited only by your storage drive/sub-system. See sop/streaming_data package(in_red_cfs) for code & sample usage in test
  * D. High Performance Search Engine, alternative to ElasticSearch/SOLR but also has attributes of a real database engine, with ACID/two phase commit transactions

Above list already covers most data storage scenarios one can think of. Traditionally, (R)DBMS systems including NoSqls can't support storage - search & management of these three different data size use-cases. It is typically one of them and up to two, e.g. - A and/or B(SQL server) or just C(AWS S3 & a DBMS like Postgres for indexing). But SOP supports all four of them out of the box.

In all of these, ACID transactions, high speed, scaleable searches and management comes built-in. As SOP turned M-Way Trie data structures & algorithms a commodity available in all of its usage scenarios. Horizontally scaleable in the cluster, meaning, there is no single point of failure. SOP offers a decentralized approach in searching & management of your data. It works with optimal efficiency in the cluster. It fully parallelize I/O in the cluster, not needing any communication for "orchestration"(see new "communication free" OOA algorithm section below) to detect conflict and auto-merging of changes across transactions occuring simultaneously or in time.

# Best Practices
Following are the best practices using SOP outlined so you can get a good understanding of best outcome from SOP for your implementation use-cases:

## As a general purpose DB engine
  * Single Writer, many Readers - a dedicated background worker populating your SOP/Cassandra DB doing management operations such as: adds, updates and/or deletes. And having many readers across the cluster.
  * Many Writers, many Readers - this setup "can be" slow as you are exposed to having conflicting transactions and data merges. BUT if your use-case or organized the transactions in a way that there is minimal or not excessive conflicts per data submitted, then you can achieve a very decent/great performance considering you are benefiting from ACID transactions, thus achieving higher data quality in high volume mgmt, sorting & searching.
  * No check mode - Transaction mode ```NoCheck``` allows your code to do reader transaction that will not do any check on commit. Useful for cases you are sure there will be no changes to the items & their Nodes(by other transactions in the cluster) you will be reading(or you don't care) in this transaction and thus, can tell SOP not to do any item version check during commit. This is the leanest & most performant mode if such guarantee is met.

Still, you have to bear in mind that these use-cases are geared for achieving higher data quality. Comparing the solution with other ACID transactions data providers, you will find that what SOP provides will match or, most likely, surpass whatever is available in the market. Because the solution provides a sustained throughput as there is no bottleneck and the entire data processing/mgmt solution is as parallelized as possible. The OOA algorithm for orchestration for example, provides decentralized & sustained throughput performance.
But of course, even SOP can't be compared if you will use or compare it to an ```eventual consistency```(no ACID transaction) with comparable paired caching(e.g. - Redis) DB storage solution. At the cost or in exchange of not having ACID, so, it depends on your needs.

## As a large or very large Search &/or DB engine - 2nd, 3rd & 4th SOP use-case
For these three use-cases, there is not much competition for what SOP has to offer here, considering SOP is addressing being able to provide better data quality on top of supporting these use-cases.

Please feel free to file a request/discussion entry if you have a special domain-use in mind, as perhaps we can further optimize. Today, SOP piggy backs on the global cache(Redis) re-seeding the local cache of each transaction. It has a lot of advantages including solving data synchronization requirements among different instances running in the cluster without requiring to communicate & "orchestrate" with one another thus, maintaining a fully parallelized execution model with sustained throughput for each instance.

# SOP in Redis, Cassandra & File System
M-Way Trie data structures & algorithms based Objects persistence, using Cassandra for Registry, File System as backend storage of Blobs(see in_red_cfs package) & Redis for caching, orchestration & node/data merging. Sporting ACID transactions and two phase commit for seamless 3rd party database integration. SOP uses a new, unique algorithm(see OOA) for orchestration where it uses Redis I/O for attaining locks. NOT the ```Redis Lock API```, but just simple Redis "fetch and set" operations. That is it. Ultra high speed algorithm brought by in-memory database for locking, and thus, not constrained by any client/server communication limits.

SOP has all the bits required to be used like a golang map but which, has the features of a b-tree, which is, manage & fetch data in your desired sort order (as driven by your item key type & its Comparer implementation), and do other nifty features such as "range query" & "range updates", turning "go" into a very powerful data management language, imagine the power of "go channels" & "go routines" mixed in to your (otherwise) DML scripts, but instead, write it in "go", the same language you write your app. No need to have impedance mismatch.

Requirements:
  * Cassandra
  * Redis
  * Storage Drive(s) or sub-system (paths) for Blobs
  * Read/Write permissions(e.g. - 0777) to the drive(s)/storage path(s)
  * Golang that supports generics, currently set to 1.21.5 and higher

## Sample Code
Below is a sample code, edited for brevity and to show the important parts.

```
import (
	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_red_cfs"
	"github.com/SharedCode/sop/cassandra"
	"github.com/SharedCode/sop/redis"
)

var cassConfig = cassandra.Config{
	ClusterHosts: []string{"localhost:9042"},
	Keyspace:     "btree",
}
var redisConfig = redis.Options{
	Address:                  "localhost:6379",
	Password:                 "", // no password set
	DB:                       0,  // use default DB
	DefaultDurationInSeconds: 24 * 60 * 60,
}

// Initialize Cassandra & Redis.
func init() {
	in_red_cfs.Initialize(cassConfig, redisConfig)
}

var ctx = context.Background()
...

func main() {
	trans, _ := in_red_cfs.NewTransaction(sop.ForWriting, -1, true)
	trans.Begin()

	// Create/instantiate a new B-Tree named "fooStore" w/ 200 slots, Key is unique & other parameters
	// including the "transaction" that it will participate in. SmallData enum was selected for the
	// ValueDataSize as the value part of the item will be small in data size, of string type.
	// If the value part will be small, it is good to choose SmallData so SOP will persist the
	// value part together with the key in the Node segment itself.
	//
	// In this case, when the Node segment is read from its partition, it will contain both the
	// Keys & the Values (of the Node) ready for consumption. Small value data fits well with this.
	so := sop.ConfigureStore("fooStore", false, 200, "", sop.SmallData, "/your/blobs/basefolder/here")
	// Key is of type "int" & Value is of type "string".
	b3, _ := in_red_cfs.NewBtree[int, string](ctx, so, trans)

	b3.Add(ctx, 1, "hello world")

	...

	// Once you are done with the management, call transaction commit to finalize changes, save to backend.
	trans.Commit(ctx)
}
```

And, yet another example showing user-defined structs both as Key & Value pair. Other bits were omitted for brevity.
```
// Sample Key struct.
type PersonKey struct {
	Firstname string
	Lastname  string
}

// Sample Value struct.
type Person struct {
	Gender string
	Email  string
	Phone  string
	SSN    string
}

// Helper function to create Key & Value pair.
func newPerson(fname string, lname string, gender string, email string, phone string, ssn string) (PersonKey, Person) {
	return PersonKey{fname, lname}, Person{gender, email, phone, ssn}
}

// The Comparer function that defines sort order.
func (x PersonKey) Compare(other interface{}) int {
	y := other.(PersonKey)

	// Sort by Lastname followed by Firstname.
	i := cmp.Compare[string](x.Lastname, y.Lastname)
	if i != 0 {
		return i
	}
	return cmp.Compare[string](x.Firstname, y.Firstname)
}

const nodeSlotLength = 500

func main() {

	// Create and start a transaction session.
	trans, err := in_red_cfs.NewTransaction(sop.ForWriting, -1, true)
	trans.Begin()

	// Create the B-Tree (store) instance. ValueDataSize can be SmallData or MediumData in this case.
	// Let's choose MediumData as the person record can get set with medium sized data, that storing it in
	// separate segment than the Btree node could be beneficial or more optimal per I/O than storing it
	// in the node itself(as in SmallData case).
	so := sop.ConfigureStore("persondb", false, nodeSlotLength, "", sop.MediumData, "/your/blobs/basefolder/here")
	b3, err := in_red_cfs.NewBtree[PersonKey, Person](ctx, so, trans)

	// Add a person record w/ details.
	pk, p := newPerson("joe", "krueger", "male", "email", "phone", "mySSN123")
	b3.Add(ctx, pk, p)

	...
	// To illustrate the Find & Get Value methods.
	if ok, _ := b3.FindOne(ctx, pk, false); ok {
		v, _ := b3.GetCurrentValue(ctx)
		// Do whatever with the fetched value, "v".
		...
	}

	// And lastly, to commit the changes done within the transaction.
	trans.Commit(ctx)
}
```
You can store or manage any data type in Golang. From native types like int, string, long, etc... to custom structs for either or both Key & Value pair. For custom structs as Key, all you need to do is to implement the "Compare" function. This is required by SOP so then you can specify how the items will be sorted. You can define however you like the sorting to happen. Compare has int return type which follows standard "comparable" interface. The return int value is as follows:
  * Returns ```0``` means both keys being compared are equal
  * ```> 1``` means that the current key(x) is greater than the other key(y) being compared
  * ```< 1``` means that the current key(x) is lesser than the other key(y) being compared

You can also create or open one or many B-Trees within a transaction. And you can have/or manage one or many transactions within your application.
Import path for SOP V2 is: "github.com/SharedCode/sop/in_red_cfs". "in_red_cfs" is an acronym that stands for:
SOP in Redis, Cassandra & File System(in_red_cfs).

V2 is in Beta 2 status and there is no known issue.

But yeah, V2 is showing very good results. ACID, two phase commit transaction, and impressive performance as Redis is baked in. SOP V2 actually succeeded in turning M-Way Trie a native "resident" of the cluster. Each of the host running SOP, be it an application or a micro-service, is turned into a high performance database & rich search server. Each, a master, or shall I say, master-less. And, of course, it is objects persistence, thus, you just author your golang struct and SOP takes care of fast storage & ultra fast searches and in the order you specified. No need to worry whether you are hitting an index, because each SOP "store"(or B-Tree) is the index itself! :)

Check out the "Sample Configuration" section below or the unit tests under "in_red_cfs" folder to get idea how to specify the configuration for Cassandra and Redis. The file system is easy, just give it a path to the folder you want to be base folder for each store, can be on a different drive/storage sub-system per store, for better throughput. Also, if you want to specify the Cassandra consistency level per API, you can take a look at the "ConsistencyBook" field of the Cassandra Config struct. Each of the Repository/Store API CRUD operation has Consistency level settable under the "ConsistencyBook", or you can just leave it and default for the session is, "local quorum".
See here for code details: https://github.com/SharedCode/sop/blob/d473b66f294582dceab6bdf146178b3f00e3dd8d/cassandra/connection.go#L35

## Streaming Data
As discussed above, the third usability scenario of SOP is support for very large data. Here is sample config code for creating a Btree that is fit for this use-case:
```
	btree, _ := in_red_cfs.NewBtree[StreamingDataKey[TK], []byte](ctx, sop.ConfigureStore("fooStore", true, 500, "Streaming data", sop.BigData, "/your/blobs/basefolder/here"), trans)
```
This sample code is from the ```StreamingDataStore struct``` in package ```sop/streaming_data```, it illustrates the ```sop.ConfigureStore``` helper function & ```sop.BigData``` value data size enum use. The streaming data store was implemented to store or manage very large value part of the item. It is a byte array and you can "encode" to it chunks of many MBs. This is the typical use-case for the ```sop.BigData``` enum. See the code here for more details: https://github.com/SharedCode/sop/blob/master/streaming_data/streaming_data_store.go

Sample code to use this ```StreamingDataStore```:
```
import (
	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_red_cfs"
)

// ...
	// To create and populate a "streaming data" store.
	trans, _ := in_red_cfs.NewTransaction(sop.ForWriting, -1, true)
	trans.Begin()
	sds := in_red_cfs.NewStreamingDataStore[string](ctx, "fooStore", trans, "/your/blobs/basefolder/here")
	// Add accepts a string parameter, for naming the item, e.g. - "fooVideo".
	// It returns an "encoder" object which your code can use to upload chunks
	// of the data.
	encoder, _ := sds.Add(ctx, "fooVideo")
	for i := 0; i < 10; i++ {
		encoder.Encode(fmt.Sprintf("%d. a huge chunk, about 15MB.", i))
	}
	trans.Commit(ctx)

	// Read back the data.
	trans, _ = in_red_cfs.NewTransaction(sop.ForReading, -1, true)
	trans.Begin()
	sds = in_red_cfs.OpenStreamingDataStore[string](ctx, "fooStore", trans)

	// Find the video we uploaded.
	sds.FindOne(ctx, "fooVideo")
	decoder, _ := sds.GetCurrentValue(ctx)
	var chunk string
	for {
		if err := decoder.Decode(&chunk); err == io.EOF {
			// Stop when we have consumed all data(reached EOF) of the uploaded video.
			break
		}
		// Do something with the downloaded data chunk.
		fmt.Println(chunk)
	}
	// End the reader transaction.
	trans.Commit(ctx)
```
### Upload
The Streaming Data Store's methods like ```Add```, ```AddIfNotExists``` and ```Update``` all return an ```Encoder``` object that allows your code to upload(via ```Encode``` method) chunks or segments of data belonging to the item, e.g. - a video if it is a video, or anything that is huge data. Divide your large data into decent chunk size, e.g. - 20MB chunk, 500 of them will allow you to store a 10GB data/content. Upon completion, calling transaction ```Commit``` will finalize the upload.

### Download
On downloading, code can call ```FindOne``` to find the item and position the built-in cursor to it, then call ```GetCurrentValue``` will return a ```Decoder``` object that allows your code to download the chunks or segments of the uploaded data(via ```Decode``` method). And like usual, calling the transaction ```Commit``` will finalize the reading transaction. If you pass a buffer to ```Decode``` that matches your uploaded chunk size(recommended) then the number of times you call ```Decoder.Decode``` will match the number of times you invoked ```Encoder.Encode``` during upload.

### Fragment(s) Download
Streaming Data store supports ability to skip chunk(s) and start downloading to a given desired chunk #. Btree store's navigation method ```Next``` is very appropriate for this. Sample code to show how to position to the fragment or chunk #:
```
	// FindChunk will find & position the "cursor" to the item with a given key and chunk index(1). Chunk index is 0 based, so, 1 is actually the 2nd chunk.
	sds.FindChunk(ctx, "fooVideo", 1)

	// Calling GetCurrentValue will return a decoder that will start downloading from chunk #2 and beyond, 'til EOF.
	decoder, _ := sds.GetCurrentValue(ctx)
	// decoder.Decode method will behave just the same, but starts with the current fragment or chunk #.
```
Alternately, instead of using ```FindOne``` & ```Next``` to skip and position to the chunk #, you can use the ```FindChunk``` method and specify the chunk # your code wants to start downloading from.

If you think about it, this is a very useful feature. For example, you can skip and start downloading (or streaming your movie!) from a given segment. Or if you use SOP to manage/store and to download your big data, e.g. - a software update, a data graph, etc... you can easily support inteligent download, e.g. - "resume and continue" without coding at all.

And since our backing store is Cassandra, benefit from its replication feature across data centers. All free softwares and code is in your hands, 'can enhance it or request for enhancement that you need.

## Default & Custom Store Cache Config
SOP comes with default cache config that are used by BTree constructor functions(NewBtree(...)) which is set to 12 hour absolute expiration for each data part of a given data store. If this is not acceptable as default, your code can change the default store cache config by using this API:
  * sop/SetDefaultCacheConfig(StoreCacheConfig) - specify new default store caching config. After the update, future calls to B-Tree constructor(NewBtree(..)) function that has no specification for store cache config(nil), will use this default value.

NOTE: This has no effect for those **NewBtree** calls where a store cache config is specified, see top section - quick guide for setting store cache config for more details.

## Sample Configuration
```
var cassConfig = cassandra.Config{
	ClusterHosts: []string{"localhost:9042"},
	Keyspace:     "btree",
}
var redisConfig = redis.Options{
	Address:                  "localhost:6379",
	Password:                 "", // no password set
	DB:                       0,  // use default DB
	DefaultDurationInSeconds: 24 * 60 * 60,
}

func init() {
	Initialize(cassConfig, redisConfig)
}
```
Above illustrates sample configuration for Cassandra & Redis bits, and how to initialize (via in_red_cfs.Initialize(..) function) the "system". You specify that and call Initialize one time(e.g. in init() like as shown) in your app or microservice and that is it.

## Transaction Batching
You read that right, in SOP, all your actions within a transaction becomes the batch that gets submitted to the backend. Thus, you can just focus on your data mining and/or application logic and let the SOP transaction to take care of submitting all your changes for commit. Even items you've fetched are checked for consistency during commit. And yes, there is a "reader" transaction where you just do fetches or item reads, then on commit, SOP will ensure the items you read did not change while in the middle or up to the time you submitted or committed the transaction.

Recommended size of a transaction is about 500 items(and should typically match the "slot length" of the node, without going over the Cassandra "logged transaction" ceiling), more or less, depending on your data structure sizes. That is, you can fetch(Read) and/or do management actions such as Create, Update, Delete for around 500 items more or less and do commit to finalize the transaction.

## Atomicity, Consistency, Isolation and Durability
SOP transaction achieves each of these ACID transaction attributes by moving the M-Way Trie(B-Tree for short) within the SOP code library. B-Tree is the heart of database systems. It enables fast storage and searches, a.k.a. - indexing engine. But more than that, by SOP's design, the B-Tree is used as part of the "controller logic" to provide two phase commit, ACID transactions.

It has nifty algorithms controlling/talking to Redis & Cassandra(in behalf of your CRUD operations) in order to ensure each ACID attribute is enforced by the transaction. If ACID attributes spells mission critical for your system, then look no further. SOP provides all that and a whole lot more, e.g. built-in data caching via Redis. So, your data are "cached" in Redis and since SOP transaction also caches your data within the host memory, then you get a L1/L2 caching for free, just by using SOP code library.

## Fine Tuning
There are four primary ingredients affecting performance and I/O via SOP. They are:
  * Slot Length - typical values are 100, 500, 1,000 and so on... up to 10,000, depends on your application data requirements & usage scenario.
  * Batch Size - typically aligns with Slot Length, i.e. - set the batch size to the same amount/value as the Slot Length. Fine tune it to an ideal value which does not go over your Cassandra setup's "batch size" ceiling.
  * Cache Duration - see respective section above for details about cache duration.
  * Cassandra Consistency level - specifying what consistency to use per API call can further optimize/fine tune your data mgmgt. SOP's ACID transaction feature totally gives you a new dimension to address consistency, and the other attributes of ACID transaction. See "ConsistencyBook" discussion above for details about configurable consistency levels on API calls. It defaults to the recommended "local quorum", thus, very sufficient, but perhaps you would want to explore a more relaxed or fine grained control on consistency levels (used by SOP) when "talking" to Cassandra.

Base on your data structure size and the amount you intend to store using SOP, there is an opportunity to optimize for I/O and performance. Small to medium size data, will typically fit well with a bigger node size. For typical structure size scenarios, slot length anywhere from 100 to 5,000 may be ideal. You can match the batch size with the slot length. In this case, it means that you are potentially filling in a node with your entire batch. This is faster for example, as compared to your batch requiring multiple nodes, which will require more "virtual Ids" (or handles) in the registry table, thus, will (potentially) require more reads from registry & the node blob table. And more importantly, during commit, the lesser the number of nodes(thus, lesser "virtual Ids") used, the leaner & faster the "logged transaction" performs, which is the deciding step in the commit process, the one that makes your changes available to other transactions/machines, or triggers rollback due to conflict. It is best to keep that (virtual Ids) volume as minimal as possible.

But of course, you have to consider memory requirements, i.e. - how many bytes of data per Key/Value pair(item) that you will store. (SmallData) If you configure for the Key & Value pair to be persisted together with the other data including meta data of the node then it is a straight up one node that will contain your entire batch's items. Not bad really, but of course, you may have to do fine tuning, try a combination of "slot length"(and batch size) and see how that affects the I/O throughput. Fetches will always be very very fast, and the bigger node size(bigger slot length!), the better for fetches(reads). BUT in trade off with memory. As one node will occupy bigger memory, thus, you will have to checkout the Redis caching and your application cluster, to see how the overall setup performs.
You can also consider storing the Value part to a dedicated partition(MediumData), this will keep your Nodes' memory footprint small in exchange of an extra read when fetching the Value data part. And lastly, you can also consider "data streaming"(BigData), which is similar to MediumData, but with global caching turned off, and such... fitted for the "very large data, data streaming" use-case.

Reduce or increase the "slot length" and see what is fit with your application data requirements scenario.
In the tests that comes with SOP(under "in_red_ck" folder), the node slot length is set to 500 with matching batch size. This proves decent enough. I tried using 1,000 and it even looks better in my laptop. :)
But 500 is decent, so, it was used as the test's slot length.

Batch size caveat: In case you get failure on commit with an error of (or due to) "batch size is too big", you can reduce the batch size so you won't reach your configured Cassandra's "logged transaction" batch size ceiling. In the SOP test's case, this error was seen after many re-runs and changes, thus, it was reduced down to 200, from 500(but no change in slot length). This is a good example of fine tuning to match with your Cassandra's (registry tables' logged transaction) limit.

You specify the slot length, one time, during B-Tree creation, see NewBtree(..) call in link below for example.
Here: https://github.com/SharedCode/sop/blob/800e7e23e9e2dce42f708db9fe9a90f3e9bbe988/in_red_ck/transaction_test.go#L57C13-L57C22

## Transaction Logging
SOP supports transaction logging, you can enable this by passing "true" to the third parameter of the ```in_red_cfs.NewTransaction(true, -1, **true**)``` method to create a new transaction. Logging can be important specially when your cluster is not stable yet, and it is somewhat prone to host reboot for maintenance, etc... When a transaction is in "commit" process and the host dies, then the transaction temp resources will be left hanging. If logging is on, then the next time SOP transaction commit occurs, like after reboot of a host, then SOP will cleanup these left hanging temp resources.

Can be a life saver specially if you are storing/managing very large data set, and thus, your temp partitions are occupying huge storage space. Turn logging on in your transactions, it is highly recommended.

## Item Serialization
By default, uses Golang's built-in JSON marshaller for serialization for simplicity and support for "streaming". But you can override this by assigning your own "Marshaler" interface implementation to ```../in_red_cfs/cassandra``` & ```../redis``` packages.
See here for details about the "Marshaler" interface: https://github.com/SharedCode/sop/blob/c6d8a1716b1ab7550df7e1d57503fdb7e041f00f/encoding.go#L8C1-L8C27

## Two Phase Commit
Two phase commit is required so SOP can offer "seamless" integration with your App's other DB backend(s)' transactions. On Phase 1 commit, SOP will commit all transaction session changes onto respective new (but geared for permanence) Btree transaction nodes. Your App will then be allowed to commit any other DB(s) transactions it use. Your app is allowed to Rollback any of these transactions and just relay the Rollback to SOP ongoing transaction if needed.

On successful commit on Phase 1, SOP will then commit Phase 2, which is, to tell all Btrees affected in the transaction to finalize the committed Nodes and make them available on succeeding Btree I/O.
Phase 2 commit is a very fast, quick action as changes and Nodes are already resident on the Btree storage, it is just a matter of finalizing the Virtual ID registry with the new Nodes' physical addresses to swap the old with the new ones.

See here for more details on two phase commit & how to access it for your application transaction integration: https://github.com/SharedCode/sop/blob/21f1a1b35ef71327882d3ab5bfee0b9d744345fa/transaction.go#L23a

## Optimistic Orchestration Algorithm (OOA)
SOP uses a new, proprietary & open sourced, thus MIT licensed, unique algorithm using Redis I/O for orchestration, which aids in decentralized, highly parallelized operations. It uses simple Redis I/O ```fetch-set-fetch``` (not the Redis lock API!) for conflict detection/resolution and data merging across transactions whether in same machine or across different machines.
Here is a brief description of the algorithm for illustration:
  * Create a globally unique ID(UUID) for the item
  * Issue a Redis ```get``` on target item key to check whether this item is locked

A. If Item exists in Redis...
  * Check whether the fetched item key has the item ID, if yes then it means the item is locked by this client and can do whatever operations on it
  * If fetched item has a different item ID then it means the item was locked by another transaction
    - if the fetched item lock is compatible for the request, e.g. - both are "read lock" then proceed or treat as if "read lock" was attained
    - otherwise, rollback and abort/fail the transaction

B. If Item does not exist in Redis...
  * Update the item key in Redis with the ID using ```set``` Redis API
  * Fetch again to check whether this session "won" in attempting to attain a lock
  * If fetched ID is not the same as the item ID then another session won and apply the same logic check for compatible "read lock" and roll back and abort/fail the transaction if incompatible lock is determined
  * If fetched ID is the same then we can proceed and treat as if "lock" was attained...

Now at this point, the "lock" attained only works for about 99% of the cases, thus, another Redis "fetch" for the in-flight item(s) version check is done right before the final step of commit.
Then, as a "final final" step(after doing the mentioned Redis ```fetch``` for in-flight item(s)' version check), SOP uses the backend storage's feature to ensure only one management action for the target item(s) in-flight is done.

The entire multi-step & multi-data locks, e.g. ```lock keys``` & in-flight item(s)' version checks, "lock attainment" process is called OOA and ensures highly scaleable data conflict resolution and merging. Definitely not the Redis "lock" API. :)
The estimated time complexity is: O(3r) + O(r) or simply: O(4r)
where:
  * r represents the number of items needing lock and doing a single Redis fetch or set operation, a very quick, global cache/in-memory I/O. I stayed away from using "n" and used "r" to denote that it is a very very quick Redis I/O, not a database I/O.

OOA algorithm was specially cooked by yours truly to make hot-spot free, "decentralized", distributed processing to be practical and easily "efficiently" done. This is the first use-case, but in time, I believe we can turn this into another "commodity". :)
If you are or you know of an investor, perhaps this is the time you dial that number and get them to know SOP project. Hehe.

## Concurrent or Parallel Commits
SOP is designed to be friendly to transaction commits occurring concurrently or in parallel. In most cases, it will be able to "merge" properly the records from successful transaction commit(s), record or row level "locking". If not then it means your transaction has conflicting change with another transaction commit elsewhere in the  cluster, and thus, it will be rolled back, or the other one, depends on who got to the final commit step first. SOP uses a combination of algorithmic ingredients like "optimistic locking", intelligent "merging", etc... doing its magic with the M-Way trie and Redis & Cassandra.

The magic will start to happen after you have created the Btree(s) (& transaction committed them) you will be using. Having such enables a lot of the "cool commits merging" features. Typically, you should have "initializer" code block or function somewhere in your app/microservice where you instantiate the B-Tree stores analogous to creating your tables in RDBMS. You run DDL scripts to create the tables before running your application logic that populates the tables, i.e. - DML scripts.

Sample code to illustrate this:
```
t1, _ := in_red_cfs.NewTransaction(sop.ForWriting, -1, true)
t1.Begin()
b3, _ := in_red_cfs.NewBtree[int, string](ctx, sop.ConfigureStore("twoPhase2", true, 50, "", sop.SmallData, "/your/blobs/basefolder/here"), t1)

t1.Commit(ctx)
// ***

eg, ctx2 := errgroup.WithContext(ctx)

f1 := func() error {
	t1, _ := in_red_cfs.NewTransaction(sop.ForWriting, -1, true)
	t1.Begin()
	b3, _ := in_red_cfs.OpenBtree[int, string](ctx2, "twophase2", t1)
	b3.Add(ctx2, 5000, "I am the value with 5000 key.")
	b3.Add(ctx2, 5001, "I am the value with 5001 key.")
	b3.Add(ctx2, 5002, "I am the value with 5002 key.")
	return t1.Commit(ctx2)
}

f2 := func() error {
	t2, _ := in_red_cfs.NewTransaction(sop.ForWriting, -1, true)
	t2.Begin()
	b32, _ := in_red_cfs.OpenBtree[int, string](ctx2, "twophase2", t2)
	b32.Add(ctx2, 5500, "I am the value with 5500 key.")
	b32.Add(ctx2, 5501, "I am the value with 5501 key.")
	b32.Add(ctx2, 5502, "I am the value with 5502 key.")
	return t2.Commit(ctx2)
}

eg.Go(f1)
eg.Go(f2)

if err := eg.Wait(); err != nil {
	t.Error(err)
	return
}
```

And yes, there is no resource locking in above code & it is able to merge just fine those records added across different transaction commits that ran concurrently. :)

Check out the integration test that demonstrate this, here: https://github.com/SharedCode/sop/blob/493fba2d6d1ed810bfb4edc9ce568a1c98e159ff/in_red_cfs/integration_tests/transaction_edge_cases_test.go#L315C6-L315C41
(the sample adds one record but it is not needed, empty Btree will work just fine)

## ACID Transactions vs. Big Data
It is well known to the database world that data engines are written to support being transactional or not. Transactions work best for non-big data management. And Big Data support typically has no support for transactions, specifically, ACID type of transactions. These perception change with SOP V2+. That is, SOP V2 supports ACID transactions and Big Data, together with "partial updates". Yes, full fidelity Big Data management protected by ACID transactions.

How?
Streaming Data Store was written for this. And if you are looking for Big Data partial updates, then you can use this Btree store. Remember, in Streaming Data Store, you are given the chance to upload/download big data in streaming fashion, like using the Encoder for upload & Decoder for download. If you have noticed, using this store you can do partial download. That is, go straight to a given chunk # then download that fragment or fragments 'til the end. Similarly, you can update a given set of chunk(s). Yes, you can also update one or more chunks part of a transaction and upon commit, these updated chunk(s) will replace the one stored in the backend.

So, essentially, we have partial update support even for the Big Data with ACID transaction protection. :)
Without exchanging anything or causing any weakness on any feature we have. So, all you have to do to take advantage of this feature is, to be able to design & organize your big data set into chunks that which, you can have option to update any part(s) of them.

Sample Project: Upload 1TB of big data
```
package big_data
import(
	github.com/SharedCode/sop/in_red_cfs
)

type BigKey struct {
	filename string
	chunkIndex int
}

// The Comparer function that defines sort order.
func (x BigKey) Compare(other interface{}) int {
	y := other.(BigKey)

	// Sort by filename followed by chunk index.
	i := cmp.Compare[string](x.filename, y.filename)
	if i != 0 {
		return i
	}
	return cmp.Compare[int](x.chunkIndex, y.chunkIndex)
}

func uploader() {
	t, _ := in_red_cfs.NewTransaction(sop.ForWriting, -1, true)
	t.Begin()
	b3, _ := in_red_cfs.NewBtree[bigKey, []byte](ctx, sop.StoreOptions{
		Name:                     "bigstore",
		SlotLength:               500,
		IsUnique:                 true,
		IsValueDataActivelyPersisted: true,
		BlobStoreBaseFolderPath:  dataPath,
		CacheConfig:              sop.NewStoreCacheConfig(time.Duration(5*time.Hour), true),
	}, t)

	// Add byte array of 50 MB chunk size.
	b3.Add(ctx, BigKey{filename: "bigfile", chunkIndex: 0}, []byte{..})

	// ...
	// Commit transaction every 500 inserts, then begin a new one...

	// Add upp to 20,000 will store 1TB of data. :)
	b3.Add(ctx, BigKey{filename: "bigfile", chunkIndex: 20000}, []byte{..})
	t.Commit(ctx)
}
```

Above is an example how to upload using a single thread of execution. Of course, since Golang supports highly concurrent programming, you can instead write a Micro Service that has endpoint for upload and allows client to submit data files in similar fashion above, but now, you can put this Micro Service in a load balancer, and wala, suddenly, you can support a cluster of services that can do parallel uploads of big data files. Secured and surpassing anything on the market!
Micro Service endpoint can be secured using OAuth and thus, the setup now can surpass whatever most scaleable "objects system" in the market, may compare or surpass(depends on your design/implementation) even the biggest AWS S3 (or Oracle RDMS, if it can do Big data!) one can afford.

And all "ACID transaction" guarded, "richly searchable", "partially updateable" with better readable code, great concurrency model/control under your fingertips, like using Go channels and Go routines.

## Big Data Partial Updates
Updating any part(s) of the Big Data file is of no special case, SOP Btree.Update(..) method will take care of updating the target part of the file needing modification. Sample code snippet is shown below for illustration.
```
package big_data
import(
	github.com/SharedCode/sop/in_red_cfs
)

//...

t, _ := in_red_cfs.NewTransaction(sop.ForWriting, -1, true)
t.Begin()
b3, _ := in_red_cfs.OpenBtree[bigKey, []byte](ctx, "bigstore", t)

// Update chunk index # 100, with your new byte array of a given size.
b3.Update(ctx, BigKey{filename: "bigfile", chunkIndex: 100}, []byte{..})

// Commit the change.
t.Commit(ctx)

```

## Tid Bits

SOP is an object persistence based, modern database engine within a code library. Portability & integration is one of SOP's primary strengths. Code uses the Store API to store & manage key/value pairs of data.

Internal Store implementation uses an enhanced, modernized M-Way Trie, implementation that virtualizes RAM & Disk storage. Few of key enhancements to this B-Tree as compared to traditional implementations are:

* node load optimization keeps it at around 62%-75+% full average load of inner & leaf nodes. Traditional B-Trees only achieve about half-full (50%) at most, average load. This translates to a more compressed or more dense data Stores saving IT shops from costly storage hardware.
* leaf nodes' height in a particular case is tolerated not to be perfectly balanced to favor speed of deletion at zero/minimal cost in exchange. Also, the height disparity due to deletion tends to get repaired during inserts due to the node load optimization feature discussed above.
* virtualization of RAM and Disk due to the seamless-ness & effectivity of handling Btree Nodes and their app data. There is no context switch, thus no unnecessary latency, between handling a Node in RAM and on disk.
* etc... a lot more enhancements waiting to be documented/cited as time permits.

Via usage of SOP API, your application will experience low latency, very high performance scalability.

## How to Build & Run
Nothing special here, just issue a "go build" in the folder where you have the go.mod file and it will build the code libraries. Issue a "go test" to run the unit test on test files, to see they pass. You can debug, step-through the test files to learn how to use the code library.
The Enterprise version V2 is in package "in_red_cfs"(& "in_red_ck" if wanting to use Cassandra for blobs storage, but file system is recommended), thus, you can peruse through the "integration" tests in this folder & run them selectively. It requires setting up Cassandra & Redis and providing configuration for the two. Which is also illustrated by the mentioned tests, and also briefly discussed above.

## Brief Background
SOP is written in Go and is a full re-implementation of the c# version. A lot of key technical features of SOP got carried over and a lot more added. V2 support ACID transactions and turn any application using it into a high performance database server itself. If deployed in a cluster, turns the entire cluster into a well oiled application & database server combo cluster that is masterless and thus, hot-spot free & horizontally scalable.

V1 written in c# dotnet was designed to be a data server. It can be used to create a server app where clients submit requests for data storage and mgmt. BUT realizing that this is not horizontally scaleable, I designed V2(this current version in Golang!) to address the horizontal scale, without sacrificing much of the vertical scaleability. I think I succeeded. :)

A design where I broke apart the "server" data mgmt and introduced horizontal scale design without losing much of the scaleability and acceleration inherent for a "server" piece. It is not an outcome of luck, it is as designed from the ground up, leaving the legacy or traditional form and out with a new one! :)

## SOP in Memory
SOP in-memory was created in order to model the structural bits of SOP and allowed us to author the same M-Way Trie algorithms that will work irrespective of backend, be it in-memory or others, such as the "in Cassandra & Redis" implementation, as discussed above.

SOP in-memory is a full implementation and you can use it if it fits the needs, i.e. - no persistence, map + sorted "range" queries/updates.

Sample Basic Usage:
  * Import the sop/in_memory, e.g. ```import sop "github.com/SharedCode/sop/in_memory"```
  * Instantiate the b-tree manager, e.g. - ```sop.NewBtree[int, string](false)```. The single parameter specifies whether you would want to manage unique keys.
  * Populate the b-tree, e.g. - ```b3.Add(<key>, <value>)```
  * Do a range query, e.g. ```b3.FindOne(<key>, true),... b3.Next(), b3.GetCurrentKey or b3.GetCurrentValue``` will return either the key or the value currently selected by the built-in "cursor".
  * Let the b-tree go out of scope.

Here is the complete example:

```
package hello_world

import (
	"fmt"
	"testing"

	sop "github.com/SharedCode/sop/in_memory"
)

func TestBtree_HelloWorld(t *testing.T) {
	fmt.Printf("Btree hello world.\n")
	b3 := sop.NewBtree[int, string](false)
	b3.Add(5000, "I am the value with 5000 key.")
	b3.Add(5001, "I am the value with 5001 key.")
	b3.Add(5000, "I am also a value with 5000 key.")

	if !b3.FindOne(5000, true) || b3.GetCurrentKey() != 5000 {
		t.Errorf("FindOne(5000, true) failed, got = %v, want = 5000", b3.GetCurrentKey())
	}
	fmt.Printf("Hello, %s.\n", b3.GetCurrentValue())

	if !b3.Next() || b3.GetCurrentKey() != 5000 {
		t.Errorf("Next() failed, got = %v, want = 5000", b3.GetCurrentKey())
	}
	fmt.Printf("Hello, %s.\n", b3.GetCurrentValue())

	if !b3.Next() || b3.GetCurrentKey() != 5001 {
		t.Errorf("Next() failed, got = %v, want = 5001", b3.GetCurrentKey())
	}
	fmt.Printf("Hello, %s.\n", b3.GetCurrentValue())
	fmt.Printf("Btree hello world ended.\n\n")
}

Here is the output of the sample code above:
Btree hello world.
Hello, I am also a value with 5000 key..
Hello, I am the value with 5000 key..
Hello, I am the value with 5001 key..
Btree hello world ended.
```

Requirements
  * Golang version that supports generics
  * Internet access to github
