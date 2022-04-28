# Lab 4: Sharded Key/Value Service
*Adapted from the [MIT 6.824
Labs](http://nil.csail.mit.edu/6.824/2015/labs/lab-4.html)*


## Lab4 Results
* Passed part1 8/8 test cases
* Passed part2 5/14 test cases
* Did not finish part3


## Overview
We followed the guidelines to implement this lab but did not conquer it.

**The key elements in implement design are:**
* re-balance in shardMaster
* refactor paxosServer to accommodate the needs of shardStoreServer
* the communication process when re-configuration

**Our design:**
* shards re-balance was implemented on a most-even and least-move base
* shardMasters wrapped in Lab3 paxosServer
* shardStoreSever with a refactored paxosServer as its subNode
* shardStoreServer and shardStoreClient periodically query for latest shardConfig
* shardStoreClient send requests to groups according to the shardConfig and keep re-trying till getting results
* shardStoreServer pass client requests to sub-paxosServer for consensus
* sub-paxosServers reach consensus and redirect the command to their shardStoreServers for execution
* re-configuration infos from shardMaster are also wrapped in a command and passed to sub-paxosServer
* while a shardStoreServer is in re-configuration, it won't accept other requests or re-config
* upon receiving a re-config command from its sub-paxosServer, a shardStoreServer build a shardExchanger
* a shardExchanger includes all shards to send out and the addresses to send to, as well as all shards to receive and trusted addresses to receive from
* shards are sent wrapped in a shardPack Message with configNumber and sender verification
* upon receiving shards from a trusted sender, a shardReceipt is sent to all servers in the sender's group
* when all shards are sent and are received, a shardServer considers itself finished the re-config and start to accept requests and reconfig command again



## Optimizations we did
* At the very beginning, shardExchanger were built and passed to sub-paxosServers for consensus, 
* but later we found the size of it was big and affected the latency, so we only sent a necessary shardConfig wrapped in a command to sub-paxos
* Timer on shard exchange and query to shardMaster was carefully set and adjusted to pass part of the tests

## Review the whole project
We've put tremendous time into both lab3 and lab4, and this is the best we could do,
given the fact that zhensheng took 5 courses in this semester and xueyang was struggling to secure a job after graduation.
The debug process was time-consuming and frustrating. We spent much time reading papers and design the protocol, 
but understanding the concepts well did not help to pass some of the tests.
Hope our effort can be taken into consideration.