# M-Way Trie algorithms for Scaleable Objects Persistence (SOP)

Scaleable Objects Persistence (SOP) Framework - Golang V2

Code Coverage: https://app.codecov.io/github/SharedCode/sop

# SOP in Cassandra & Redis
M-Way Trie data structures & algorithms based Objects persistence, using Cassandra as backend storage & Redis for caching, orchestration & node/data merging. Sporting ACID transactions and two phase commit for seamless 3rd party database integration.

SOP has all the bits required to be used like a golang map but which, has the features of a b-tree, which is, manage & fetch data in your desired sort order (as driven by your item key type & its Comparer implementation), and do other nifty features such as "range query" & "range updates", turning "go" into a very powerful data management language, imagine the power of "go channels" & "go routines" mixed in to your (otherwise) DML scripts, but instead, write it in "go", the same language you write your app. No need to have impedance mismatch.

Requirements:
  * Cassandra
  * Redis
  * Kafka if wanting to enable the Delete Service
  * Golang that supports generics, currently set to 1.21.5 and higher

## Sample Code
Below is a sample code, edited for brevity and to show the important parts.

```
import (
	"github.com/SharedCode/sop/in_red_ck"
	"github.com/SharedCode/sop/in_red_ck/cassandra"
	"github.com/SharedCode/sop/in_red_ck/redis"
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
	in_red_ck.Initialize(cassConfig, redisConfig)
}

var ctx = context.Background()
...

func main() {
	trans, _ := in_red_ck.NewTransaction(true, -1)
	trans.Begin()

	// Create/instantiate a new B-Tree named "fooStore" w/ 500 slots & other parameters
	// including the "transaction" that it will participate in.
	//
	// Key is of type "int" & Value is of type "string".
	b3, _ := in_red_ck.NewBtree[int, string](ctx, "fooStore", 500, false, false, true, "", trans)

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
	trans, err := in_red_ck.NewTransaction(true, -1)
	trans.Begin()

	// Create the B-Tree (store) instance.
	b3, err := in_red_ck.NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", trans)

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

You can also create or open one or many B-Trees within a transaction. And you can have/or manage one or many transactions within your application. Of course, it is a full-fledged ACID transaction implementation.

Blob storage was implemented in Cassandra, thus, there is no need for AWS S3. Import path for SOP V2 is: "github.com/SharedCode/sop/in_red_ck".
SOP in Redis, Cassandra & Kafka(in_red_ck). Or fashionably, SOP in "red Calvin Klein", hehe.

V2 is in Alpha status but there is no known issue.

But yeah, V2 is showing very good results. ACID, two phase commit transaction, and impressive performance as Redis is baked in. SOP V2 actually succeeded in turning M-Way Trie a native "resident" of the cluster. Each of the host running SOP, be it an application or a micro-service, is turned into a high performance database server. Each, a master, or shall I say, master-less. And, of course, it is objects persistence, thus, you just author your golang struct and SOP takes care of fast storage & ultra fast searches and in the order you specified. No need to worry whether you are hitting an index, because each SOP "store"(or B-Tree) is the index itself! :)

Check out the "Sample Configuration" section below or the unit tests under "in_red_ck" folder to get idea how to specify the configuration for Cassandra and Redis. Also, if you want to specify the Cassandra consistency level per API, you can take a look at the "ConsistencyBook" field of the Cassandra Config struct. Each of the Repository/Store API CRUD operation has Consistency level settable under the "ConsistencyBook", or you can just leave it and default for the session is, "local quorum".
See here for code details: https://github.com/SharedCode/sop/blob/d473b66f294582dceab6bdf146178b3f00e3dd8d/in_red_ck/cassandra/connection.go#L35

## Cache Duration
You can specify the Redis cache duration by using the following API:
  * in_red_ck/cassandra/SetRegistryCacheDuration(duration) - defaults to 12 hrs, but you can specify if needs to cache the registry "virtual Ids" differently.
  * in_red_ck/cassandra/SetStoreCacheDuration(duration) - defaults to 2 hrs caching of the "store" metadata record.
  * in_red_ck/SetNodeCacheDuration(duration) - defaults to 1 hr caching of the B-Tree Nodes that contains the Key/Value pairs application data.

The Redis cache is minimally used because our primary is Cassandra DB, which is a very fast DB. BUT yeah, please do change if wanting to benefit with bigger Redis caching. Virtual Ids were set to 12 hrs by default but may need a shorter duration and instead, the Nodes where your application data resides needs a longer duration, for example.

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

But of course, you have to consider memory requirements, i.e. - how many bytes of data per Key/Value pair(item) that you will store. In this version, the data is persisted together with the other data including meta data of the node. Thus, it is a straight up one node(one partition in Cassandra) that will contain your entire batch's items. Not bad really, but of course, you may have to do fine tuning, try a combination of "slot length"(and batch size) and see how that affects the I/O throughput. Fetches will always be very very fast, and the bigger node size(bigger slot length!), the better for fetches(reads). BUT in trade off with memory. As one node will occupy bigger memory, thus, you will have to checkout the Cassandra "size"(perf of VMs & hot spots), Redis caching and your application cluster, to see how the overall setup performs.

Reduce or increase the "slot length" and see what is fit with your application data requirementes scenario.
In the tests that comes with SOP(under "in_red_ck" folder), the node slot length is set to 500 with matching batch size. This proves decent enough. I tried using 1,000 and it even looks better in my laptop. :)
But 500 is decent, so, it was used as the test's slot length.

Batch size caveat: In case you get failure on commit with an error of (or due to) "batch size is too big", you can reduce the batch size so you won't reach your configured Cassandra's "logged transaction" batch size ceiling. In the SOP test's case, this error was seen after many re-runs and changes, thus, it was reduced down to 200, from 500(but no change in slot length). This is a good example of fine tuning to match with Cassandra's limit.

You specify the slot length, one time, during B-Tree creation, see NewBtree(..) call in link below for example.
Here: https://github.com/SharedCode/sop/blob/800e7e23e9e2dce42f708db9fe9a90f3e9bbe988/in_red_ck/transaction_test.go#L57C13-L57C22

## Delete Service
The system leaves out unused Nodes from time to time after you do a management action(Create, Update or Delete), an aftermath of ACID transactions. I.e. - so transaction(s) that fetch current versions of Nodes will not be interrupted and continue to fetch them, not until the system (elsewhere) commits new versions in an "instantaneous" super quick action, thus making current ones "obsolete". These leftover "obsolete" Nodes need to be deleted and there are two options in place to do that:
  * Deletes on commit - SOP will, by default, delete these unused or leftover Nodes
  * Deletes via Kafka route - if DeleteService is enabled, SOP will enqueue to Kafka and let your application to take messages from Kafka and do the deletes, on your desired schedule or interval

The optimal choice is the latter. For example, you can setup a Kafka consumer which takes from Kafka queue, enable the "delete service" in SOP(see "EnableDeleteSevice") and uses SOP's BlobStore API to delete these Nodes, at your schedule or interval, like once every day or every hour. For sample code how to do this, pls. feel free to reuse/pattern it with the "in_red_ck/delete_service.go" code.
See here for code details: https://github.com/SharedCode/sop/blob/3b3b574bb97905ca38d761dedd8af95a7fbce4e2/in_red_ck/delete_service.go#L24C11-L24C11

## Item Serialization
By default, uses Golang's built-in JSON marshaller for serialization for simplicity and support for "streaming"(future feature, perhaps in V3). But you can override this by assigning your own "Marshaler" interface implementation to ```../in_red_ck/cassandra``` & ```../in_red_ck/redis``` packages.
See here for details about the "Marshaler" interface: https://github.com/SharedCode/sop/blob/c6d8a1716b1ab7550df7e1d57503fdb7e041f00f/encoding.go#L8C1-L8C27

## Two Phase Commit
Two phase commit is required so SOP can offer "seamless" integration with your App's other DB backend(s)' transactions. On Phase 1 commit, SOP will commit all transaction session changes onto respective new (but geared for permanence) Btree transaction nodes. Your App will then be allowed to commit any other DB(s) transactions it use. Your app is allowed to Rollback any of these transactions and just relay the Rollback to SOP ongoing transaction if needed.

On successful commit on Phase 1, SOP will then commit Phase 2, which is, to tell all Btrees affected in the transaction to finalize the committed Nodes and make them available on succeeding Btree I/O.
Phase 2 commit is a very fast, quick action as changes and Nodes are already resident on the Btree storage, it is just a matter of finalizing the Virtual ID registry with the new Nodes' physicall addresses to swap the old with the new ones.

See here for more details on two phase commit & how to access it for your application transaction integration: https://github.com/SharedCode/sop/blob/21f1a1b35ef71327882d3ab5bfee0b9d744345fa/in_red_ck/transaction.go#L23a

## Concurrent or Parallel Commits
SOP is designed to be friendly to transaction commits occurring concurrently or in parallel. In most cases, it will be able to "merge" properly the records from successful transaction commit(s), record or row level "locking". If not then it means your transaction has conflicting change with another transaction commit elsewhere in the  cluster, and thus, it will be rolled back, or the other one, depends on who got to the final commit step first. SOP uses a combination of algorithmic ingredients like "optimistic locking", intelligent "merger", etc... doing its magic with the M-Way trie and Redis & Cassandra.

The magic will start to happen after adding(and committing) your 1st record/batch. One record will do, 'this will allow for the B-Tree to have a "root node". Having such enables a lot of the "cool commits merging" features. Typically, you should have "initializer" code block or function somewhere in your app/microservice where you instantiate the B-Tree stores and "seed" them with record(s).

Sample code to illustrate this:
```
t1, _ := in_red_ck.NewTransaction(true, -1)
t1.Begin()
b3, _ := in_red_ck.NewBtree[int, string](ctx, "twophase2", 8, false, true, true, "", t1)

// *** Add a single item then commit so we persist "root node".
b3.Add(ctx, 500, "I am the value with 500 key.")
t1.Commit(ctx)
// ***

eg, ctx2 := errgroup.WithContext(ctx)

f1 := func() error {
	t1, _ := in_red_ck.NewTransaction(true, -1)
	t1.Begin()
	b3, _ := in_red_ck.OpenBtree[int, string](ctx2, "twophase2", t1)
	b3.Add(ctx2, 5000, "I am the value with 5000 key.")
	b3.Add(ctx2, 5001, "I am the value with 5001 key.")
	b3.Add(ctx2, 5002, "I am the value with 5002 key.")
	return t1.Commit(ctx2)
}

f2 := func() error {
	t2, _ := in_red_ck.NewTransaction(true, -1)
	t2.Begin()
	b32, _ := in_red_ck.OpenBtree[int, string](ctx2, "twophase2", t2)
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

Check out the integration test that demonstrate this, here: https://github.com/SharedCode/sop/blob/493fba2d6d1ed810bfb4edc9ce568a1c98e159ff/in_red_ck/integration_tests/transaction_edge_cases_test.go#L315C6-L315C41

## Tid Bits

SOP is an object persistence based, modern database engine within a code library. Portability & integration is one of SOP's primary strengths. Code uses the Store API to store & manage key/value pairs of data.

Internal Store implementation uses an enhanced, modernized M-Way Tree, implementation that virtualizes RAM & Disk storage. Few of key enhancements to this B-Tree as compared to traditional implementations are:

* node load optimization keeps it at around 62%-75+% full average load of inner & leaf nodes. Traditional B-Trees only achieve about half-full (50%) at most, average load. This translates to a more compressed or more dense data Stores saving IT shops from costly storage hardware.
* leaf nodes' height in a particular case is tolerated not to be perfectly balanced to favor speed of deletion at zero/minimal cost in exchange. Also, the height disparity due to deletion tends to get repaired during inserts due to the node load optimization feature discussed above.
* virtualization of RAM and Disk due to the seamless-ness & effectivity of handling Btree Nodes and their app data. There is no context switch, thus no unnecessary latency, between handling a Node in RAM and on disk.
* etc... a lot more enhancements waiting to be documented/cited as time permits.

Via usage of SOP API, your application will experience low latency, very high performance scalability.

## How to Build & Run
Nothing special here, just issue a "go build" in the folder where you have the go.mod file and it will build the code libraries. Issue a "go test" to run the unit test on test files, to see they pass. You can debug, step-through the test files to learn how to use the code library.
The Enterprise version V2 is in package "in_red_ck", thus, you can peruse through the "integration" tests in this folder & run them selectively. It requires setting up Cassandra & Redis and providing configuration for the two. Which is also illustrated by the mentioned tests, and also briefly discussed above.

## Brief Background
SOP is written in Go and is a full re-implementation of the c# version. A lot of key technical features of SOP got carried over and a lot more added. V2 support ACID transactions and turn any application using it into a high performance database server itself. If deployed in a cluster, turns the entire cluster into a well oiled application & database server combo cluster that is masterless and thus, hot-spot free & horizontally scalable.

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
