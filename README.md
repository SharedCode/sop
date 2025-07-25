Scalable Objects Persistence (SOP) Framework - Golang V2

Code Coverage: https://app.codecov.io/github/sharedcode/sop

# Introduction
What is SOP?

Scalable Objects Persistence(SOP) is a raw storage engine that bakes together a set of storage related features & algorithms in order to provide the most efficient & reliable (ACID attributes of transactions) technique (known) of storage management and rich search, as it brings to the application, the raw muscle of "raw storage", direct IO communications w/ disk drives. In a code library form factor today.

SOP V2 ships as a Golang code library. Thus, it can be used for storage management by applications of many types across different hardware architectures & Operating Systems (OS), that are supported by the Golang compiler.

See more details here that describe further, the different qualities & attributes/features of SOP, and why it is a good choice as a storage engine for your applications today: [Summary](README2.md)

Before I go, I would like to say, SOP is a green field, totally new. What is being shipped in V2 is just the start of this new product. We are barely scratching the surface of what can be done that will help storage management at super scale. SOP is a super computing enabler. The way its architecture was laid out, independent features and together, they are meant to give us the best/most efficient performance & IO of a group of computers (cluster), network & their storage, that can possibly give us.

## High level features/usability articles about SOP
See the entire list & details here: https://github.com/sharedcode/sop/blob/master/README2.md#high-level-features-articles-about-sop

# How to Use SOP?
You will be surprised how easy to use SOP. Because we have shipped in SOP everything you need to manage your data & at SUPER scale! Its API is like NoSQL (Key/Value pair based), but it does NOT need anything else other than Redis for caching. That is it. Think of it this way, your Cassandra/MongoDB/Oracle/ElasticSearch (SOP provides unlimited B-tree! limited only by your hardware), etc.. & their client libraries IS IN SOP code library. Boom, simple, nothing else needed in this option to do storage management using SOP.

First, you need to decide & pick a hardware/software setup for your production and your development environments (& anything in between). SOP supports all or most of them, so, you will have freedom/flexibility which one to choose. Ensure you have plenty of disk drives storage space, enough to store your planned amount of data to manage.

For software dependency, SOP only depends on Redis & the Golang compiler/runtime (1.24.3 & above). Here are instructions to setup the environments including your development machine:
1. Setup/install Redis in your target environment(s), e.g. in Production cluster & in your development machine/cluster. Make sure to give Redis in each of the environment plenty of resources, e.g. - memory/RAM so it can serve/scale to the needs of your cluster.

2. Ensure you have provisioned the disk drives where you will tell SOP to store/manage the data, in each of the environment.
NOTE: please use sector size of 4096 when formatting the drives. This is the default sector size in most Linux & MacOS servers, thus, SOP uses that size in its direct IO memory aligned allocations. Windows supports this sector size as well.

3. In development machine, import SOP code library to your application and start coding using SOP API to manage the data. We recommend the "inredfs" package as it is very lean & requiring only Redis as dependency. SOP github location: https://github.com/sharedcode/sop/inredfs Since SOP was written in Golang, then you have a few ways to use it in your application, depending on which language you are writing your application (or microservice or API, any app type...). If your application is written in Golang then you can directly import the SOP package and use the package in the Golang fashion. If your application is written in other languages, you have a choice whether to use SOP via its compiled binary. So, you can download SOP source code, build binary to your target hardware architecture & OS. Example, build it for Linux x86.
THEN you can integrate with this binary in your application. For example, if in Python, you can use GoPy to integrate. If in Java, you will need JNI, etc... Each of this technique has its own challenges, the best & easiest is to write your application in Golang then SOP is imported/used as a normal Go package.
Second best is, to make it available as a microservice to your application(s), write a microservice in Go using SOP. Then you have solved communications and reuse via RESTful API interactions. Or gRPC, etc... which ever you want to support in your microservice. You can then freely author your application in any language, even in DotNet c# if you want to.

4. See SOP's API discussions (link is in bottom below) for more details on how to use different features/functionalities of SOP. Like how to initialize Redis passing the Redis cluster config details.
How to create/begin & commit/rollback SOP transactions, use its B-tree API to store/manage key/value pairs of data sets (CRUD: Add, GetXx, Update, Remove), and how to do searches (FindOne, navigation methods like First, Last, Next, Previous, etc...).

5. Once done and you are satisfied with your application development, you can then release your application, SOP library & other dependencies to your next target environment. This will be nothing special than your typical applications development and release process. SOP is just a code library/package. And your microservice (if you made one) that manages your data (using SOP perhaps!) should be released following your team's standard method of releasing a microservice to your target environment, manually and/or CICD.

6. Ensure you have setup a proper application user with proper permission to your target disk drives. Follow the standard way how to do it in your environment. Nothing is special here, SOP uses files/disk drives like ordinary packages, but via DirectIO & OS File System API. SOP enables support for different OS/hardware architectures without requiring anything else, other than what was discussed above. It uses the same locking mechanism via Redis to lock the virtual IDs and the low-level file sectors. And what is called as "unified locks", because the same can serve both purposes, high level & low level locks.

7. SOP Python bindings is in PyPi: https://pypi.org/project/sop4py. When ready & your team is into Python applications development, you can download sop4py from PyPi. The SOP Python bindings package source code is in https://github.com/sharedcode/sop/tree/master/jsondb/python. The API was designed to be easy to use and fit for Python "idiomatic" usage for database management & rich search. Please checkout the unit tests under this folder (test_btree.py & test_btree_idx.py) to get idea how to use the SOP Python bindings API for managing data & rich search within transactions.

# Usability
See details here: https://github.com/sharedcode/sop/blob/master/README2.md#usability

# SOP API Discussions
See details here: https://github.com/sharedcode/sop/blob/master/README2.md#simple-usage

# SOP for Python (sop4py)
See details here: https://github.com/sharedcode/sop/tree/master/jsondb/python#readme