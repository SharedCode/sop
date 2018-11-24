# Sop

Scalable Object Persistence (SOP) Framework

SOP is a modern database engine within a code library. It is categorized as a NoSql engine, but which because of its scale-ability, is considered to be an enabler, coo-petition/player (wannabee) in the Big Data space.

Integration is one of SOP's primary goals, its ease of use, API, being part/closest! to the App & in-memory performance level were designed so it can get (optionally) utilized as a middle-ware for current RDBMS and other NoSql/Big Data engines/solution.

Code uses the Store API to store & manage key/value pairs of data. Internal Store implementation uses an enhanced, modernized B-Tree implementation that virtualizes RAM & Disk storage. Couple of key enhancements to this B-Tree as compared to traditional implementations are:

node load optimization keeps it at around 75%-98% full average load of inner & leaf nodes. Traditional B-Trees only achieve about half-full (50%) average load. This translates to a more compressed or more dense data Stores saving IT shops from costly storage hardware.
leaf nodes' height in a particular case is tolerated not to be perfectly balanced to favor speed of deletion at zero/minimal cost in exchange. Also, the height disparity due to deletion tends to get repaired during inserts due to the node load optimization feature discussed above.
etc... a lot more enhancements waiting to be documented/cited as time permits.
The above listed enhancements together with the data structure and algorithms employed to take advantage of computer's memory (RAM) to keep more frequently used objects readily available to the application in their native form primarily distinguishes this implementation with the rest. Thus, the overall solution was branded virtualization of RAM and Disk due to the seamless-ness & effectivity offered.

SOP addresses data management scale-ability internally, at the data driver level, so when you use SOP code library, all you have to do is focus on authoring your application data solution. Nifty algorithms such as use of MRU data cache to keep frequently accessed data in memory, bulk I/O operations, B-Tree index usability optimizations, data bucketing for large data scenarios, etc... are already pre-baked, done at the driver level, so you don't have to.

Via usage of SOP API, your application will experience low latency, very high performance scalability.
