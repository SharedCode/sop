This is a great time to join and contribute to SOP's development. The new version being rewritten from the ground up (in golang) has just started. All modules have not been rewritten yet. We are still in prototyping mode. :)
There is no perfect time than it is today, right now. So come and join. :)

The new features together with the ones from legacy SOP (in c#) combined is very exciting. Example, true ACID transactions, usage
of Cassandra as backend data store and support for BLOBs with performance degradation.

Also, SOP authors are well adept on Agile style of dev't. Thus, we can all work on different modules & functionalities without
much hazzle and find easy time to integrate our code.

But indeed, there is a process that needs to be established and followed in order to make our dev'ts easy. Here we go:
* Best way to understand SOP is to read the "ReadMe" file so you get to familiarize with the high level features.
* Look into unit tests to understand the different interfaces, starting with the high level ones.
* Browse through the code. Yes, SOP is being written with simplicity. Choice of golang is purposely done, so we can maintain
simplicity of code, without sacrificing code agility. Once you start coding in golang, you will understand why "go". :)
* Don't be shy to ask the SOP Authors for questions, clarifications.
* Before implementing a specific module or functionality, please ask the Authors first. So we can account who works on which
module, functionality and then avoid duplication of effort.
* Ensure to test your code, provide unit test(s) as deemed necessary.
* Follow git dev't best practice, such as Branch forking, submitting a "pull request" for merging code with the main(master) branch.

Following are the main modules being worked on:
* Cassandra adaptor
* Btree algorithm rewrite in go
* Btree Transaction behavior
* Two Phase Commit ACID Transaction for Cassandra

