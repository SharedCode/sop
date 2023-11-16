# Sop

Scalable Object Persistence (SOP) Framework

SOP Version 1(beta) is an in-memory implementation. It was created in order to model the structural bits of SOP and allow creation of mocks that can be used for V2. Usage of common unit tests for in-memory and the real thing is ideal as it allows us to have two versions that behave in the same manner when it comes to storage and management of items, thus, allowing baseline comparison between the two versions.

SOP in-memory however, is a full implementation. It has all the bits required to be used like a golang map but which, has the features of a b-tree, which is, manage data in your desired sort order (as driven by your item key type & its Comparer implementation).

Usage:
  * Import the sop/in_memory, e.g. import "github.com/SharedCode/sop/in_memory"
  * Instantiate the b-tree manager, e.g. - in_memory.NewBtree[int, string](false). The single parameter specifies whether you would want to manage unique keys.
  * Populate the b-tree, e.g. - b3.Add(<key>, <value>)
  * Do a range query, e.g. b3.FindOne(<key>, true),... b3.MoveToNext(), b3.GetCurrentKey or b3.GetCurrentValue will return either the key or the value currently selected by the built-in "cursor".
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
	b3, _ := sop.NewBtree[int, string](false)
	b3.Add(5000, "I am the value with 5000 key.")
	b3.Add(5001, "I am the value with 5001 key.")
	b3.Add(5000, "I am also a value with 5000 key.")

	if ok,_ := b3.FindOne(5000, true); !ok || b3.GetCurrentKey() != 5000 {
		t.Errorf("FindOne(5000, true) failed, got = %v, want = 5000", b3.GetCurrentKey())
	}
	fmt.Printf("Hello, %s.\n", b3.GetCurrentValue())

	if ok,_ := b3.MoveToNext(); !ok || b3.GetCurrentKey() != 5000 {
		t.Errorf("MoveToNext() failed, got = %v, want = 5000", b3.GetCurrentKey())
	}
	fmt.Printf("Hello, %s.\n", b3.GetCurrentValue())

	if ok,_ :=b3.MoveToNext(); !ok || b3.GetCurrentKey() != 5001 {
		t.Errorf("MoveToNext() failed, got = %v, want = 5001", b3.GetCurrentKey())
	}
	fmt.Printf("Hello, %s.\n", b3.GetCurrentValue())

	fmt.Printf("Btree hello world ended.\n\n")
	b3 = nil
}

Here is the output of the sample code above:
Btree hello world.
Hello, I am also a value with 5000 key..
Hello, I am the value with 5000 key..
Hello, I am the value with 5001 key..
Btree hello world ended.
```

Requirements
  * Golang toolset version that supports generics
  * Internet access to github


Below discussions are for the next version, V2.

SOP is a modern database engine within a code library. It is categorized as a NoSql engine, but which because of its scale-ability, is considered to be an enabler, coo-petition/player in the Big Data space.

Integration is one of SOP's primary goals, its ease of use, API, being part/closest! to the App & in-memory performance level were designed so it can get (optionally) utilized as a middle-ware for current RDBMS and other NoSql/Big Data engines/solution.

Code uses the Store API to store & manage key/value pairs of data. Internal Store implementation uses an enhanced, modernized M-Way Tree(which we will simply call B-Tree for brevity), implementation that virtualizes RAM & Disk storage. Few of key enhancements to this B-Tree as compared to traditional implementations are:

* node load optimization keeps it at around 75%-98% full average load of inner & leaf nodes. Traditional B-Trees only achieve about half-full (50%) average load. This translates to a more compressed or more dense data Stores saving IT shops from costly storage hardware.
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

## How to Build & Run
Nothing special here, just issue a "go build" in the folder where you have the go.mod file and it will build the code libraries. Issue a "go test" to run the unit test on test files, to see they pass. You can debug, step-through the test files to learn how to use the code library.

# Technical Details
SOP written in Go will be a full re-implementation. A lot of key technical features of SOP will be carried over and few more will be added in order to support a master-less implementation. That is, backend Stores such as Cassandra, AWS S3 bucket will be utilized and SOP library will be master-less in order to offer a complete, 100% horizontal scaling with no hot-spotting or any application instance bottlenecks.

## Component Layout
* SOP code library for managing key/value pair of any data type (interface{}/interface{}).
* redis for clustered, out of process data caching.
* Cassandra, AWS S3 (future next), etc... as backend Stores.
Support for additional backends other than Cassandra & AWS S3 will be done on per request basis.

Cassandra integration will sport recommended "time series" solution to scale storage and access on Cassandra. Tomb Stones will also be minimally used. SOP has deleted data (block) recycling technology, thus, making usage of Storage engines like Cassandra where deletes are expensive & large blobs, can be made optimal or efficiency unaffected.

## Very Large Blob Layout
Large data including vlblobs are optionally storable as a set of data blocks. Being able to use many small to medium sized data blocks to store a huge data is considered optimal. Example, a 2GB data can be stored using four 512MB data blocks. Each block having its own partition and thus, all four can be read "served" up from the Cluster by four different cluster node. Similarly, during write, Cassandra cluster can perform optimally storing these four blocks on four different partitions.

This storage structure together with SOP's "data block" recycling feature, solves the issue of Cassandra (and any backend store for this matter) not being suited for storing large data sets. Operating without requirement to use "streaming" feature also simplifies the API and the Application trying to access/use this kind of large data.
This solution is so much better than streaming because, other than it doesn't require special "streaming" feature in Cassandra engine, it utilizes the backend's optimal IO method. i.e. - parallel access using multiple cluster nodes on multi-partitioned data sets.

Data blocks' uniform size also removes any Cassandra "hot spots" when the data is being served. Even after data is recycled multiple times, its IO performance when it comes to being stored/served by Cassandra doesn't degrade at all.

## Item Serialization
Application can specify Item (key & value pair) serialization and if not, SOP will default to treating Key and Value pairs as "string" types. This means each Item will be persisted/read as string and keys will be compared & thus, Items sorted like a string type based on key.

## Transaction
SOP will sport ACID, two phase commit transactions with two modes:
* in-memory transaction sandbox - short lived and changes are persisted only during transaction commit. Initial implementation will support (out of process, e.g. in redis) in-memory, short lived transactions as will be more optimal I/O wise.
* on-disk transaction sandbox - long lived and changes persisted to a Transaction Sandbox table and committed to their final Btree store destinations during commit. Future next will support long lived transactions which are geared for special types of use-cases.

### Two Phase Commit
Two phase commit is required so SOP can offer "seamless" integration with your App's other DB backend(s)' transactions. On Phase 1 commit, SOP will commit all transaction session changes onto respective new (but geared for permanence) Btree transaction nodes. Your App will then be allowed to commit any other DB(s) transactions it use. Your app is allowed to Rollback any of these transactions and just relay the Rollback to SOP ongoing transaction if needed.
On successful commit on Phase 1, SOP will then commit Phase 2, which is, to tell all Btrees affected in the transaction to finalize the committed Nodes and make them available on succeeding Btree I/O.
Phase 2 commit will be a very fast, quick action as changes and Nodes are already resident on the Btree storage, it is just a matter of finalizing the Virtual ID registry with the new Nodes' physicall addresses to swap the old with the new ones.

