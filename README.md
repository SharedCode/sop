# M-Way Trie algorithms for Scaleable Objects Persistence (SOP)

Scaleable Objects Persistence (SOP) Framework

## SOP In-Memory
SOP in-memory was created in order to model the structural bits of SOP and allowed us to author the same M-Way Trie algorithms that will work irrespective of backend, be it in-memory or others, such as the "in Cassandra & Redis" implementation(see discussion below for details).

SOP in-memory, is a full implementation. It has all the bits required to be used like a golang map but which, has the features of a b-tree, which is, manage & fetch data in your desired sort order (as driven by your item key type & its Comparer implementation), and do other nifty features such as "range query" & "range updates", turning "go" into a very powerful data management language, imagine the power of "go channels" & "go routines" mixed in to your (otherwise) DML scripts, but instead, write it in "go", the same language you write your app. No need to have impedance mismatch.

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

# SOP in Cassandra & Redis

Requirements:
  * Cassandra
  * Redis
  * Kafka if wanting to enable the Delete Service

Blob storage was implemented in Cassandra, thus, there is no need for AWS S3. Import path for SOP V2 is: "github.com/SharedCode/sop/in_red_ck".
SOP in Redis, Cassandra & Kafka(in_red_ck). Or fashionably, SOP in "red Calvin Klein", hehe.

V2 is in POC status but there is no known issue.

But yeah, V2 is showing very good results. ACID, two phase commit transaction, and impressive performance as Redis is baked in. SOP V2 actually succeeded in turning M-Way Trie a native "resident" of the cluster. Each of the host running SOP, be it an application or a micro-service, is turned into a high performance database server. Each, a master, or shall I say, master-less. And, of course, it is objects persistence, thus, you just author your golang struct and SOP takes care of fast storage & ultra fast searches and in the order you specified. No need to worry whether you are hitting an index, because each SOP "store"(or B-Tree) is the index itself! :)

Check out the "Sample Configuration" section below or the unit tests under "in_red_ck" folder to get idea how to specify the configuration for Cassandra and Redis. Also, if you want to specify the Cassandra consistency level per API, you can take a look at the "ConsistencyBook" field of the Cassandra Config struct. Each of the Repository/Store API CRUD operation has Consistency level settable under the "ConsistencyBook", or you can just leave it and default for the session is, "local quorum".
Here: https://github.com/SharedCode/sop/blob/d473b66f294582dceab6bdf146178b3f00e3dd8d/in_red_ck/cassandra/connection.go#L35

## Cache Duration
You can specify the Redis cache duration by using the following API:
  * in_red_ck/cassandra/SetRegistryCacheDuration(duration) - defaults to 12 hrs, but you can specify if needs to cache the registry "virtual Ids" differently.
  * in_red_ck/cassandra/SetStoreCacheDuration(duration) - defaults to 2 hrs.
  * in_red_ck/SetNodeCacheDuration(duration) - defaults to 1 hr. Definitely please do change if wanting different cache duration.

The Redis cache is minimally used because our primary is Cassandra DB, which is a very fast DB. BUT yeah, please do change if wanting to benefit with bigger Redis caching. :)

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
Above illustrates sample configuration for Cassandra & Redis bits, and how to initialize (via in_red_ck.Initialize(..) function) the "system". You specify that and call Initialize one time(e.g. in init() like as shown) in your app or microservice and that is it.

## Transaction Batching
You read that right, in SOP, all your actions within a transaction becomes the batch that gets submitted to the backend. Thus, you can just focus on your data mining and/or application logic and let the SOP transaction to take care of submitting all your changes for commit. Even items you've fetched are checked for consistency during commit. And yes, there is a "reader" transaction where you just do fetches or item reads, then on commit, SOP will ensure the items you read did not change while in the middle or up to the time you submitted or committed the transaction.

Recommended size of a transaction is about 500 items(and should typically match the "slot length" of the node), more or less, depending on your data structure sizes. That is, you can fetch(Read) and/or do management actions such as Create, Update, Delete for around 500 items more or less and do commit to finalize the transaction.

## Atomicity, Consistency, Isolation and Durability
SOP transaction achieves each of these ACID transaction attributes by moving the M-Way Trie(B-Tree for short) within the SOP code library. B-Tree is the heart of database systems. It enables fast storage and searches, a.k.a. - indexing engine. But more than that, by SOP's design, the B-Tree is used as part of the "controller logic" to provide two phase commit, ACID transactions.

It has nifty algorithms controlling/talking to Redis & Cassandra(in behalf of your CRUD operations) in order to ensure each ACID attribute is enforced by the transaction. If ACID attributes spells mission critical for your system, then look no further. SOP provides all that and a whole lot more, e.g. built-in data caching via Redis. So, your data are "cached" in Redis and since SOP transaction also caches your data within the host memory, then you get a L1/L2 caching for free, just by using SOP code library.

## Fine Tuning
There are two primary ingredients affecting performance and I/O via SOP. They are:
  * Slot Length - typical values are 100, 500, 1,000 and so on... depends on your application data requirements & usage scenario
  * Batch Size - typically aligns with Slot Length, i.e. - set the batch size to the same amount/value as the Slot Length

Base on your data structure size and the amount you intend to store using SOP, there is an opportunity to optimize for I/O and performance. Small to medium size data, will typically fit well with a bigger node size. For typical structure size scenarios, slot length anywhere from 100 to 1,000 may be ideal. You can match the batch size with the slot length. In this case, it means that you are potentially filling in a node with your entire batch. This is faster for example, as compared to your batch requiring multiple nodes, which will require more "virtual Ids" (or handles) in the registry table.

But of course, you have to consider memory requirements, i.e. - how many bytes of data per Key/Value pair(item) that you will store. In this version, the data is persisted together with the other data including meta data of the node. Thus, it is a straight up one node(one partition in Cassandra) that will contain your entire batch's items. Not bad really, but of course, you may have to do fine tuning, try a combination of "slot length"(and batch size) and see how that affects the I/O throughput. Fetches will always be very very fast, and the bigger node size(bigger slot length!), the better for fetches(reads). BUT in trade off with memory. As one node will occupy bigger memory, thus, you will have to checkout the Cassandra "size"(perf of VMs & hot spots), Redis caching and your application cluster, to see how the overall setup performs.

Reduce or increase the "slot length" and see what is fit with your application data requirementes scenario.
In the tests that comes with SOP(under in_red_ck folder), the node slot length is set to 500 with matching batch size. This proves decent enough. I tried using 1,000 and it even looks better in my laptop. :)
But 500 is decent, so, it was used as the test's slot length.

## Delete Service
The system leaves out unused Nodes from time to time after you do a management action(Create, Update or Delete), an aftermath of ACID transactions. I.e. - so transaction(s) that fetch current versions of Nodes will not be interrupted and continue to fetch them, not until the system (elsewhere) commits new versions in an "instantaneous" super quick action, thus making current ones "obsolete". These leftover "obsolete" Nodes need to be deleted and there are two options in place to do that:
  * Deletes on commit - SOP will, by default, delete these unused or leftover Nodes
  * Deletes via Kafka route - if DeleteService is enabled, SOP will enqueue to Kafka and let your application to take messages from Kafka and do the deletes, on your desired schedule or interval

The optimal choice is the latter. For example, you can setup a Kafka consumer which takes from Kafka queue, enable the "delete service" in SOP(see "EnableDeleteSevice") and uses SOP's BlobStore API to delete these Nodes, at your schedule or interval, like once every day or every hour. For sample code how to do this, pls. feel free to reuse/pattern it with the "in_red_ck/delete_service.go" code.
Here: https://github.com/SharedCode/sop/blob/3b3b574bb97905ca38d761dedd8af95a7fbce4e2/in_red_ck/delete_service.go#L24C11-L24C11

## General Discussion of SOP V2

Below features are mostly achieved in this SOP V2 POC, only three features did not make it, i.e. - the data driver for support of huge blobs & its companion feature, streaming to support extremely huge data. And the "long lived" transaction that will use "transaction sandbox" storage. This last feature may not make it, not until V4 or probably even later, as the market for such is not big enough.

SOP is a modern database engine within a code library. It is categorized as a NoSql engine, but which because of its scale-ability, is considered to be an enabler, coo-petition/player in the Big Data space.

Integration is one of SOP's primary goals, its ease of use, API, being part/closest! to the App & in-memory performance level were designed so it can get (optionally) utilized as a middle-ware for current RDBMS and other NoSql/Big Data engines/solution.

Code uses the Store API to store & manage key/value pairs of data. Internal Store implementation uses an enhanced, modernized M-Way Tree(which we will simply call B-Tree for brevity), implementation that virtualizes RAM & Disk storage. Few of key enhancements to this B-Tree as compared to traditional implementations are:

* node load optimization keeps it at around 62%-75+% full average load of inner & leaf nodes. Traditional B-Trees only achieve about half-full (50%) at most, average load. This translates to a more compressed or more dense data Stores saving IT shops from costly storage hardware.
* leaf nodes' height in a particular case is tolerated not to be perfectly balanced to favor speed of deletion at zero/minimal cost in exchange. Also, the height disparity due to deletion tends to get repaired during inserts due to the node load optimization feature discussed above.
* virtualization of RAM and Disk due to the seamless-ness & effectivity of handling Btree Nodes and their app data. There is no context switch, thus no unnecessary latency, between handling a Node in RAM and on disk.
* data block technology enables support for "very large blob" (vlblob) efficient storage and access without requiring "data streaming" concept. Backend stores that traditionally are not recommended for storage of vlblob can be enabled for such. E.g. - Cassandra will not feel the "vlblobs" as SOP will store manage-able data chunk size to Cassandra store.
* etc... a lot more enhancements waiting to be documented/cited as time permits.

SOP addresses data management scale-ability internally, at the data driver level, so when you use SOP code library, all you have to do is focus on authoring your application data solution. Nifty algorithms such as use of MRU data cache to keep frequently accessed data in memory, bulk I/O operations, B-Tree index usability optimizations, data bucketing for large data scenarios, etc... are already pre-baked, done at the driver level, so you don't have to.

Via usage of SOP API, your application will experience low latency, very high performance scalability.

# Build Instructions
## Prerequisite
Here are the prerequisites for doing a local run:
* Redis running locally using default Port
* Cassandra running locally using default Port
* Golang that supports generics, currently set to 1.21.5 and higher

## How to Build & Run
Nothing special here, just issue a "go build" in the folder where you have the go.mod file and it will build the code libraries. Issue a "go test" to run the unit test on test files, to see they pass. You can debug, step-through the test files to learn how to use the code library.
The Enterprise version V2 is in package "in_red_ck", thus, you can peruse through the "integration" tests in this folder & run them selectively. It requires setting up Cassandra & Redis and providing configuration for the two. Which is also illustrated by the mentioned tests, and also briefly discussed above.

# Technical Details
SOP is written in Go and is a full re-implementation of the c# version. A lot of key technical features of SOP got carried over and few more added that supported a master-less implementation. That is, backend Stores such as Cassandra, is utilized and SOP library will be master-less in order to offer a complete, 100% horizontal scaling with no hot-spotting or any application instance bottlenecks.

## Component Layout
* SOP code library for managing key/value pair of any data type using Go's generics.
* Redis for clustered, out of process data caching.
* Cassandra, etc... as backend Stores.
Support for additional backends other than Cassandra will be done on per request basis, or as time permits.

Cassandra will be used as data Registry & as the data blob store. Redis will provide the necessary out of process "caching" needs to accelerate I/O.

## Very Large Blob Layout
Blobs is stored in Cassandra, thus, benefitting from its built-in features like "replication" across regions, etc...

## Item Serialization
Uses Golang's built-in marshaller for serialization for simplicity and support for "streaming"(future feature, perhaps in V3).

## Transaction
SOP sports ACID, two phase commit transactions with two modes:
* in-memory transaction sandbox - short lived and changes are persisted only during transaction commit. Initial implementation will support (out of process, e.g. in redis) in-memory, short lived transactions as will be more optimal I/O wise.
* on-disk transaction sandbox - long lived and changes persisted to a Transaction Sandbox table and committed to their final Btree store destinations during commit. Future next will support long lived transactions which are geared for special types of use-cases(future feature, perhaps in V4?).

### Two Phase Commit
Two phase commit is required so SOP can offer "seamless" integration with your App's other DB backend(s)' transactions. On Phase 1 commit, SOP will commit all transaction session changes onto respective new (but geared for permanence) Btree transaction nodes. Your App will then be allowed to commit any other DB(s) transactions it use. Your app is allowed to Rollback any of these transactions and just relay the Rollback to SOP ongoing transaction if needed.
On successful commit on Phase 1, SOP will then commit Phase 2, which is, to tell all Btrees affected in the transaction to finalize the committed Nodes and make them available on succeeding Btree I/O.
Phase 2 commit will be a very fast, quick action as changes and Nodes are already resident on the Btree storage, it is just a matter of finalizing the Virtual ID registry with the new Nodes' physicall addresses to swap the old with the new ones.
