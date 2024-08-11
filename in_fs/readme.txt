SOP in File System

This directory will contain the SOP store implementation that persists data to a file system. It will not require
anything else other than directory path where SOP library will store/manage the data. No Cassandra, no Redis required.

For caching purposes, it is thought that using the cluster of machines hosting the apps linked to SOP library should
be sufficient. Use of MPI API should address optimized inter-process communication, thus, perhaps achieving or surpassing
Redis caching solution.


Directory As A Storage(DAAS)
A special file naming convention will be used to solve scalability on managing data on files of particular directoy(ies).
First three hexadecimal digits/letters of the name will convey the three levels of directories where files will be stored.
The fourth hex will be used to distribute the data files across the mentioned (prefix) three levels. By evenly distributing
the data files across the three level folders, will create a sustained throughput, similar to achieving a b-tree w/
time complexity search time w/ file I/O "delay" but is horizontally scaleable.

GUID Format: abcdxxxx-xxxx-xxxx-xxxx-xxxxxxx
Prefixes discussion:
a - First level folder
b - second level folder
c - third level folder
d - used to distribute the files across the three levels(prefix), as shown above. Modulo(%) will be used as the distribution
logic. That is, for example, given 0-15 hexadecimal "slots", applying modulo 3 to select which level(0 to 2), will naturally
distribute the files on the three level folders.

Example (GUID) filenames:
aaaa1234-1234-1234-1234-12345678.txt
aaab1234-1234-1234-1234-12345678.txt
aaa11234-1234-1234-1234-12345678.txt
aaa71234-1234-1234-1234-12345678.txt

All of the above data files will be store in folder hierarchy:
* "a" (1st hex) of level 1 in folder a
* "a" (2nd hex) of level 2 in folder a/a
* "a" (3rd hex) of level 3 in folder a/a/a
This is because all of them have prefix or starts with the hex "aaa".

Now, for distribution to which "level" folder, let us take the 1st data file, aaaa1234-1234-1234-1234-12345678.txt for
illustration. This data file will be stored in the folder "aa" because its 4th hex digit("a") when modulo 3, will give a
value of 1.

File aaab1234-1234-1234-1234-12345678.txt will be stored in folder "aaa"(because 11%3=2).


Solid State Drive(SSD)
Usage of SSD or better performing hard drive(s)/backend data storage system should further optimize the overall
cluster I/O throughput.


B-Tree Nodes in DAAS
Each of the B-Tree Node will be stored in the DAAS file.

Object Registry in DAAS
Each of entry(handle!) in the Object Registry will be stored in the DAAS file & eventually, cached using MPI API implementation. We will
use a map(in-memory) managed by each host in the cluster as data cache, so, the Object Registry I/O will be optimal(as is cached in-memory).
Initial implementation will have no caching for early delivery, but implementing the Store interface, thus, injecting caching later on
will be smooth.
