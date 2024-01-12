# M-Way Trie algorithms for Scaleable Object Persistence (SOP)

Scaleable Object Persistence (SOP) Framework

SOP Version 1(beta) is an in-memory implementation. It was created in order to model the structural bits of SOP and allowed us to author the same M-Way Trie algorithms that will work irrespective of backend, be it in-memory or others, such as that geared for V2.

SOP in-memory, is a full implementation. It has all the bits required to be used like a golang map but which, has the features of a b-tree, which is, manage & fetch data in your desired sort order (as driven by your item key type & its Comparer implementation), and do other nifty features such as "range query" & "range updates", turning "go" into a very powerful data management language, imagine the power of "go channels" & "go routines" mixed in to your (otherwise) DML scripts, but instead, write it in "go", the same language you write your app. No need to have impedance mismatch.

Sample Basic Usage:
  * Import the sop/in_memory, e.g. ```import sop "github.com/SharedCode/sop/in_memory"```
  * Instantiate the b-tree manager, e.g. - ```sop.NewBtree[int, string](false)```. The single parameter specifies whether you would want to manage unique keys.
  * Populate the b-tree, e.g. - ```b3.Add(<key>, <value>)```
  * Do a range query, e.g. ```b3.FindOne(<key>, true),... b3.Next(), b3.GetCurrentKey or b3.GetCurrentValue``` will return either the key or the value currently selected by the built-in "cursor".
  * Let the b-tree go out of scope or assign nil to it.

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

SOP V2 Requirements
  * Cassandra
  * Redis
  * Kafka if wanting to enable the Delete Service

Blob storage was implemented in Cassandra, thus, there is no need for AWS S3. Import path for SOP V2 is: "github.com/SharedCode/sop/in_red_ck".
SOP in Redis, Cassandra & Kafka(in_red_ck). Or fashionably, SOP in "red Calvin Klein", hehe.

V2 is in POC status but there is no known issue. Unit tests is very important at this point and it is being worked on to increase coverage.

But yeah, V2 is showing very good results. ACID, two phase commit transaction, and impressive performance as Redis is baked in. SOP V2 actually succeeded in turning M-Way Trie a native "resident" of the cluster. Each of the host running SOP, be it an application or a micro-service, is turned into a high performance database server. Each, a master, or shall I say, master-less. And, of course, it is object persistence, thus, you just author your golang struct and SOP takes care of fast storage & ultra fast searches and in the order you specified. No need to worry whether you are hitting an index, because each SOP "store"(or B-Tree) is the index itself! :)

Check out the unit tests under "in_red_ck" folder to get idea how to specify the configuration for Cassandra and Redis. Also, if you want to specify the Cassandra consistency level per API, you can take a look at the "ConsistencyBook" field of the Cassandra Config struct. Each of the Repository/Store API CRUD operation has Consistency level settable under the "ConsistencyBook", or you can just leave it and default for the session is, "local quorum".

Cache Duration
You can specify the Redis cache duration by using the following API:
  * in_red_ck/cassandra/SetRegistryCacheDuration(duration) - defaults to 12 hrs, but you can specify if needs to cache the registry "virtual Ids" differently.
  * in_red_ck/cassandra/SetStoreCacheDuration(duration) - defaults to 2 hrs.
  * in_red_ck/SetNodeCacheDuration(duration) - defaults to 1 hr. Definitely please do change if wanting different cache duration.
The Redis cache is minimally used because our primary is Cassandra DB, which is a very fast DB. BUT yeah, please do change if wanting to benefit with bigger Redis caching. :)

Below discussions are mostly achieved in this SOP V2 POC, I will update and move what ever details did not make it, e.g. the data driver for support of huge blobs to a future, V3 release section.

SOP is a modern database engine within a code library. It is categorized as a NoSql engine, but which because of its scale-ability, is considered to be an enabler, coo-petition/player in the Big Data space.

Integration is one of SOP's primary goals, its ease of use, API, being part/closest! to the App & in-memory performance level were designed so it can get (optionally) utilized as a middle-ware for current RDBMS and other NoSql/Big Data engines/solution.

Code uses the Store API to store & manage key/value pairs of data. Internal Store implementation uses an enhanced, modernized M-Way Tree(which we will simply call B-Tree for brevity), implementation that virtualizes RAM & Disk storage. Few of key enhancements to this B-Tree as compared to traditional implementations are:

* node load optimization keeps it at around 62%-75% full average load of inner & leaf nodes. Traditional B-Trees only achieve about half-full (50%) average load. This translates to a more compressed or more dense data Stores saving IT shops from costly storage hardware.
* leaf nodes' height in a particular case is tolerated not to be perfectly balanced to favor speed of deletion at zero/minimal cost in exchange. Also, the height disparity due to deletion tends to get repaired during inserts due to the node load optimization feature discussed above.
* virtualization of RAM and Disk due to the seamless-ness & effectivity of handling Btree Nodes and their app data. There is  no context switch, thus no unnecessary latency, between handling a Node in RAM and on disk.
* data block technology enables support for "very large blob" (vlblob) efficient storage and access without requiring "data streaming" concept. Backend stores that traditionally are not recommended for storage of vlblob can be enabled for such. E.g. - Cassandra will not feel the "vlblobs" as SOP will store manage-able data chunk size to Cassandra store.
* etc... a lot more enhancements waiting to be documented/cited as time permits.

SOP addresses data management scale-ability internally, at the data driver level, so when you use SOP code library, all you have to do is focus on authoring your application data solution. Nifty algorithms such as use of MRU data cache to keep frequently accessed data in memory, bulk I/O operations, B-Tree index usability optimizations, data bucketing for large data scenarios, etc... are already pre-baked, done at the driver level, so you don't have to.

Via usage of SOP API, your application will experience low latency, very high performance scalability.

# Build Instructions
## Prerequisite
Here are the prerequisites for doing a local run:
* Redis running locally using default Port
* Cassandra running locally using default Port
* Access & permission to an AWS S3 bucket

## How to Build & Run
Nothing special here, just issue a "go build" in the folder where you have the go.mod file and it will build the code libraries. Issue a "go test" to run the unit test on test files, to see they pass. You can debug, step-through the test files to learn how to use the code library.

# Technical Details
SOP written in Go will be a full re-implementation. A lot of key technical features of SOP will be carried over and few more will be added in order to support a master-less implementation. That is, backend Stores such as Cassandra, AWS S3 bucket will be utilized and SOP library will be master-less in order to offer a complete, 100% horizontal scaling with no hot-spotting or any application instance bottlenecks.

## Component Layout
* SOP code library for managing key/value pair of any data type using Go's generics.
* redis for clustered, out of process data caching.
* Cassandra, AWS S3, etc... as backend Stores.
Support for additional backends other than Cassandra & AWS S3 will be done on per request basis, or as time permits.

Cassandra will be used as data Registry & AWS S3 as the data blob store. Redis will provide the necessary out of process "caching" needs to accelerate I/O.

## Very Large Blob Layout
Blobs will be stored in AWS S3, thus, benefitting from its built-in features like "replication" across regions, etc...

## Item Serialization
Will use Golang's built-in marshaller for serialization for simplicity and support for "streaming".

## Transaction
SOP will sport ACID, two phase commit transactions with two modes:
* in-memory transaction sandbox - short lived and changes are persisted only during transaction commit. Initial implementation will support (out of process, e.g. in redis) in-memory, short lived transactions as will be more optimal I/O wise.
* on-disk transaction sandbox - long lived and changes persisted to a Transaction Sandbox table and committed to their final Btree store destinations during commit. Future next will support long lived transactions which are geared for special types of use-cases.

### Two Phase Commit
Two phase commit is required so SOP can offer "seamless" integration with your App's other DB backend(s)' transactions. On Phase 1 commit, SOP will commit all transaction session changes onto respective new (but geared for permanence) Btree transaction nodes. Your App will then be allowed to commit any other DB(s) transactions it use. Your app is allowed to Rollback any of these transactions and just relay the Rollback to SOP ongoing transaction if needed.
On successful commit on Phase 1, SOP will then commit Phase 2, which is, to tell all Btrees affected in the transaction to finalize the committed Nodes and make them available on succeeding Btree I/O.
Phase 2 commit will be a very fast, quick action as changes and Nodes are already resident on the Btree storage, it is just a matter of finalizing the Virtual ID registry with the new Nodes' physicall addresses to swap the old with the new ones.
